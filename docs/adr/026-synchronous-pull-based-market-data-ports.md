# ADR-026: Synchronous, pull-based market-data ports

- Status: proposed
- Date: 2026-10-05

## Context

The live Binance feed is network I/O and naturally asynchronous; historical
replay is a sequential read. ADR-019 demands one domain path for both, and
ADR-025 keeps async runtimes out of the core.

## Decision

`MarketDataProvider` is a synchronous pull interface: the core asks for the
next event and blocks until one exists. Adapters that do async I/O run their
own runtime internally (for example on a dedicated thread) and hand
normalized domain events to the port through a bounded channel. The core,
the ports and the application services contain no async code.

The Binance live adapter has two such channels: producers → capture
thread and capture thread → core (`live.rs`; ADR-032 D2). Each holds **65 536
messages** (`inbound_channel_capacity`, `core_channel_capacity`; defaults
in `LiveConfig::new`). A full channel blocks its sender, and nothing is
dropped. Every `stats` journal line records each channel's high-water
mark and blocked time (ADR-032 D11).

### Why 65 536

The evidence is the measurement comment on #77
(<https://github.com/Replikanti/mie/issues/77#issuecomment-6101927637>,
"M0b" below).

- **History.** The #9 plan (comment 6006788589) set 65 536 for both
  channels. Its only reason was a risk note: "large bounded channels plus
  high-water and blocked-time metrics … ADR-026 gets its numbers from the
  soak". No derivation was written until #77.
- **Lower side: what the capture needed.** The largest high-water of any
  soak run is core 2 604 and inbound 603. Both are from #9 run 2, the core
  one at 2026-10-08 17:00:57 UTC, inside the US cash session. Blocked time
  was 0 ms on both channels in every `stats` line of every run (#9, #10,
  #13). 65 536 is therefore about 25 times the measured maximum.
- **Lower side: a projection.** Divide a run's high-water by the send rate
  of the minute in which it was reached. The result is an effective stall
  S: how many seconds of arrivals the queue held. The largest S is
  24.16 s (#9 run 1, core: 1 121 at 46.4 messages per second). The busiest
  25 s of the 12-month archive held 60 015 aggregate trades (2026-08-19
  15:27:35–15:28:00 UTC). The other six streams add 19 messages per second
  at their p99 rates. A stall of that length during that burst projects a
  queue of **58 458** messages, below 65 536. Across all runs and both
  channels, no other pairing projects more (M0b, section 3). The rule was
  fixed before the measurement: keep 65 536 if and only if the projection
  stays below it. This is a projection, not a measurement. It assumes the
  queue grows with the arrival rate at a fixed stall, and live arrival can
  be burstier than exchange time when an upstream lag releases a backlog.
  With the largest S of *any* high-water rise (26.98 s), the projection is
  62 746, which is still below.
- **Upper side: what the capacity costs.** `std::sync::mpsc::sync_channel`
  allocates every slot up front and writes each slot's stamp, so every
  page is resident before the first message. A slot is the message plus 8
  bytes, aligned. The inbound slot is 248 bytes, so 65 536 slots take
  15.5 MiB. The core slot is 88 bytes, so 5.5 MiB. Together that is
  22.0 MB, about 48 % of the 45.8 MB peak RSS of `mie ingest` in #9.
- **The value.** 2^16 is a round power of two between the two sides. It
  is above the projected worst burst, at a cost of tens of megabytes. It
  is not forced. Any value above the projection (about 59 000) meets the
  lower side, and the upper side is a cost that grows linearly, not a
  limit. Choosing among those values is a judgment.

## Consequences

- Replay, backtests and tests drive the core with plain loops: fully
  deterministic, no executor scheduling in the result path.
- The live adapter owns buffering, backpressure and reconnects, and must
  surface feed gaps as data instead of swallowing them.
- One blocked core thread per consumer of a live stream — fine for one
  instrument and one primary consumer (ADR-002).
- **A smaller channel capacity** blocks earlier. At or below a run's
  high-water, that run would have blocked, and below the projected 58 458
  messages the worst stall seen during the year's busiest burst would
  block. A blocked inbound sender stalls its WebSocket reader. It neither
  reads nor pings, and its frames wait in the socket and get their receive
  time only when read. Two readers blocked at once enter the channel in
  thread-scheduling order, not arrival order. If one stream's frames fall
  more than the 2000 ms hold-back behind the merge watermark, they become
  `LateEvent` gaps (ADR-032 D6). A reader blocked for 90 s or more drops
  its own connection at the next liveness check, because liveness counts
  from the last frame before the block (ADR-032 D8). That gives a
  `Disconnected` gap. A blocked core sender stalls the capture thread: no
  appends, no seals and no `stats` lines until it clears, while the inbound
  channel fills behind it.
- **A larger channel capacity** costs memory linearly: 16 times the
  22.0 MB at 1 048 576 slots, about 352 MB. A deeper queue adds delay,
  not loss. Full at the year's peak 25 s rate (about 2 400 messages per
  second), 65 536 messages are about 27 s of events. mie has no live
  latency budget, because there is no execution path (ADR-010; ADR-032
  justification addendum, section c).

## Alternatives considered

- **Async ports (`async fn` in traits)** — couples every core crate to an
  executor and lets scheduling influence processing order.
- **Push-based callbacks** — inverts control into the adapter and makes
  backpressure and ordering harder to reason about.
- **An unbounded channel (`mpsc::channel`)** — no backpressure. While the
  core stalls, memory grows without bound, so a slow core ends in an
  out-of-memory kill instead of a measured blocked time.
- **Drop on full (`try_send`, discard)** — loses events. That is against
  raw-first capture (ADR-030, ADR-032 D2) and against surfacing feed gaps
  as data.
- **A rendezvous channel (`sync_channel(0)`) or a capacity of a few
  dozen** — every consumer hiccup blocks the readers. Every run, even the
  8-minute #9 rotation run, reached a high-water of at least 46 on both
  channels, so a capacity below that would have blocked in every run.
- **8 192 per channel** (about 3 times the measured maximum) — saves
  about 19 MB (22.0 → 2.75 MB of slots), but it sits below the projected
  58 458 messages.

## Accept when

The Binance live adapter feeds the core through this port at full BTCUSDT
trade and depth rates during the US cash session, without blocking,
dropped events or unbounded buffering. This is settled by one run, judged
from its capture journal (`journal.rs`):

- **The run.** A weekday, all seven streams, the default configuration
  (`ingest.example.toml`). It runs from 30 min before the US cash session
  opens to 30 min after it closes: 13:00–20:30 UTC while US daylight time
  is in effect (session 13:30–20:00), 14:00–21:30 UTC otherwise (session
  14:30–21:00).
- **Pass** requires all three checks:
  1. `blocked_ms` is 0 for `inbound` and `core` in every `stats` line up
     to the session close. The counter is cumulative per run, so a block
     in the lead-in fails the run too.
  2. `run_end.events` equals `core.sent` in the run's last `stats` line,
     and `run_end.exit_code` is 0.
  3. aggTrade and depth are each connected for at least 99 % of the
     6 h 30 min session. That means at most 3 min 54 s in total between a
     `disconnected` line of the stream and its next `connected` line,
     counted inside the session.
- **Outcome.** If check 3 fails, the run is void and is repeated: it
  recorded an outage, not a capacity result. If check 1 or 2 fails, this
  ADR is revised before acceptance, because the channel capacity or the
  core's throughput does not hold at US-session rates. The record also
  gives each channel's high-water at the session close, and whether it
  rose inside the session.

Why each threshold (M0b, sections 1 and 4):

- **Weekday, US session.** Over the 12-month archive, a weekend day
  carries about half a weekday's trades. The four busiest weekday hours lie
  inside the session in both clock regimes: 13–16 UTC under US daylight
  time and 14–17 UTC under standard time. Session hours average 1.9 times
  (daylight) and 2.2 times (standard) the other weekday hours, and the
  year's busiest 25 s fell inside a session. The year's short bursts do
  not keep to the session, though (the 7–14 s peak was at 02:18 UTC, the
  1–3 s peak at 10:05 UTC). One run observes one session's bursts; the
  projection above covers the year's peak.
- **All seven streams, defaults.** Depth is the part of this criterion
  that no soak has measured inside the session (*Progress*). The defaults
  are what `mie ingest` runs with.
- **30 min on each side.** Start-up fills the queues while the
  connections open and the order book syncs. #9 run 2 reached core 395 in
  its first minute, and the short #9 rotation run and #10 run 1 reached
  97–100 % of their whole-run high-water within two minutes. The lead-in
  keeps those rises out of the session, so a rise inside the session is a
  session effect. The tail keeps the shutdown out. The length is a
  judgment and costs one extra hour of capture.
- **Blocked time 0.** Any blocked time means the bounded channel reached
  its limit, with the effects listed under *Consequences*. It was 0 in
  every line of about 47 h of soak runs, so a blocked millisecond at 25
  times headroom would contradict the derivation above.
- **`events` = `core.sent`, exit 0.** Every event the capture released
  reached the core, and the capture did not stop on a failed append or
  seal (ADR-032 D10).
- **99 % connected.** A run without trade or depth rates proves nothing
  about capacity. On this host, the #10 soak lost 4.2 s of order-book sync
  in 13.7 h, two real depth disconnects included (ADR-038), and a planned
  rotation takes under 2 s per stream (ADR-032). A healthy run is
  therefore far above 99 %, and only an outage longer than 3.9 min voids
  it.

Progress 2026-10-10: the #9 soak (about 30 h, trades without depth) fed the
core with channel blocked time 0 ms and no dropped events. Its high-water
peaked at core 2 604 and inbound 603 of 65 536 (run 2). The core peak came
at 2026-10-08 17:00:57 UTC, inside the US cash session, so trade rates are
covered for the session. An earlier version of this paragraph said "at most
753". That is the run 1 maximum as of 16:33 UTC on 2026-10-07, shortly
after its 16:04–16:18 burst, not the soak maximum (#77). Depth rates are
covered by the #10 soak (13 h 41 min, all seven streams, ADR-038): the
depth diff stream ran at 10 messages per second (p50) and 11 (p99) with a
maximum of 40, at about 640 MB of raw depth per day, and fed the core with
channel blocked time 0 ms, high-water 714 (core) and 488 (inbound) of
65 536 and no dropped events. That window excluded the US session, and so
did #13 (22:30–01:30 UTC). Depth at US-session rates is the remaining gap,
and the run above closes it.
