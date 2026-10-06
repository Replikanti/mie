# ADR-032: Binance live capture — per-stream connections, raw first, bounded hold-back

- Status: proposed
- Date: 2026-10-06

## Context

#9 captures the BTCUSDT perpetual (Binance USDⓈ-M) live feed into the raw
store and feeds the core through `MarketDataProvider`. The earlier decisions
fix most of the frame:

- Market-data ports are synchronous and pull-based. An adapter may run its
  own runtime and hands events over through a bounded channel (ADR-026).
- The canonical order uses exchange time only. Late live events become
  `LateEvent` gaps, and ADR-028 D6 leaves the size of the hold-back to #9.
- Raw data is stored verbatim and persisted before it is parsed. A kill
  loses the records written since the last seal, and #9 bounds that loss
  (ADR-030).
- Live and replay share one normalization (ADR-019), so replay (#11) and
  the equivalence harness (#13) must be able to recompute what live
  delivered from persisted inputs.

Binance closes every WebSocket connection after 24 h, throttles
liquidation snapshots to one per second per symbol, and publishes open
interest over REST only.

## Decision

1. **One WebSocket connection per stream**: `aggTrade`, `markPrice@1s`,
   `forceOrder`, `kline_1m`. The open interest comes from a REST poller.
   Every message is stored exactly as it was received, under its raw stream
   name.
2. **Plain blocking threads, no async runtime.** There is one thread per
   connection, one open-interest poller and one capture thread. The capture
   thread is the only writer. For each frame it assigns `receive_seq` (a
   per-run counter over all streams, in processing order), appends the
   record, runs the pipeline, and sends the released events into a bounded
   channel. The provider blocks on that channel. So an event reaches the
   core only after its record was appended, and a frame that fails to parse
   is still persisted.
3. **Raw `event_time`** is the ADR-028 ordering field of the payload:
   aggTrade `T`, markPrice `E`, forceOrder `o.T`, kline `k.T` (the close
   time, also for an open bar), open interest `time`. If the field is
   missing, the push time `E` is used. If that is missing too, the receive
   time is used and counted as a time fallback.
4. **Capture metadata contract.** `session_id` is
   `<run_id>/<raw stream>/<connection ordinal>`. The `run_id` is the run's
   UTC start second, written `YYYYMMDDTHHMMSSZ`. For open interest, the
   ordinal grows after every failed poll. `receive_seq` orders the records
   of one run. The deterministic `Pipeline` (normalize → per-stream
   sequencer → hold-back merge) is a pure function of two inputs:
   - the run's records in `receive_seq` order: stream, payload, raw
     `event_time` and `session_id`, all in the raw store;
   - the run's parameters: `symbol`, `hold_back_ms`, `oi_retime_ms` (D12)
     and the per-stream seeds. They are **not** in the raw store. The journal's `run_start`
     line records them, and the seeds also depend on what was sealed when
     the run started.

   A recompute of live output (replay #11, the equivalence harness #13)
   therefore reads the raw store **plus** the run's `run_start` parameters
   (`mie_cli::journal::RunParameters`). It never uses configuration
   defaults: the same records with another hold-back or other seeds give
   different output. A test recomputes the pipeline from the persisted
   records and the run parameters and checks it against what the provider
   delivered.
5. **Gap rules**, per stream:
   - a session change always yields `Disconnected` [last delivered, first
     resumed], even when the trade ids happen to be contiguous;
   - an aggregate-trade id other than `last + 1` within one session yields
     `SequenceBreak` [previous `T`, next `T`];
   - each run is seeded with the largest event time sealed per stream in
     the last 7 days, so its first event yields `Disconnected` from there.
     This makes every restart, and the crash loss of ADR-030 D5, a gap;
   - a trade whose id is at or below the last one delivered is dropped. An
     id-less event is dropped only when it exactly repeats the last one;
   - a record of an id-less stream (mark price, liquidation, kline, open
     interest) that fails normalization yields `MissingData` [last
     delivered, the record's raw `event_time`]. The lost sample is then
     visible in the event stream, not only in the journal. On trades, the
     next id break already reports a lost trade.
6. **Hold-back of 2000 ms** of exchange time. The watermark is the highest
   ordering time pushed so far, gaps excluded. It never uses the wall clock.
   An event is released once it is more than the hold-back older than the
   watermark. Anything at or below the last released event becomes a
   `LateEvent` gap (ADR-028 D6). Per-stream maximum lateness is journaled.
7. **Planned reconnect at 23 h**, staggered by 5 min per stream position:
   close, then reconnect at once. Each stream accepts a sub-second gap once
   a day, and that gap is announced like any other disconnect. Connections
   do not overlap.
8. **Liveness.** The client sends a ping every 30 s. A connection that
   delivers no frame of any kind (data, ping or pong) for 90 s is dropped.
   Reconnects back off from 250 ms, doubling up to 30 s, without jitter. The
   backoff resets after 60 s of healthy connection.
9. **Open interest** is polled every 10 s, aligned to wall-clock multiples
   of 10 s. That cadence is also the `resolution_ms` of live `OpenInterest`,
   so normalization stays a pure function of the payload. The request and
   response times go to the capture journal; the raw envelope (ADR-030
   schema v1) is unchanged. Non-2xx responses and transport failures are
   journaled but never persisted. A 418 or 429 defers the next poll by an
   exponential backoff.
10. **Seal every 300 s** and at shutdown. That bounds the ADR-030 D5 loss
    to 5 minutes, and the restart seed turns that loss into a gap. A failed
    append or seal stops the capture, and the provider and `mie ingest`
    fail loudly (exit 1).
11. **The journal.** Lifecycle, gaps, seals, normalize errors, stats
    (channel high-water marks and blocked time), domain rejections and a
    `run_end` summary are written as JSON Lines. `mie capture-report`
    verifies a window against it. Among its checks, it reports `LateEvent`
    count, late fraction (`late / (events + late)`) and maximum lateness
    per stream. It fails when a stream's late fraction exceeds
    `--max-late-fraction`, which defaults to **0**. The hold-back exists
    so that nothing is late, so any late event means the hold-back is too
    small for that stream. A tolerance is a conscious choice made on the
    command line, not a default. Open interest re-timed under D12 counts as
    delivered, not late.
12. **Open interest that arrives late is re-timed, not dropped.** This
    **supersedes one clause of ADR-028**: the live OpenInterest ordering
    time in its table ("live: the response timestamp"). ADR-028 is
    accepted, so per the ADR process only its status line changes; it now
    points here. All other parts of ADR-028 stand, including the archive
    rule for open interest and "never a validity time earlier than
    publication".

    The REST `time` trails the poll by 4 to 8 s (see the documentation
    check), so under D6 every live sample is late. A late `OpenInterest`
    is delivered at `last released + 1`, the first millisecond still open,
    when both of these hold:
    - its slot passed by at most `oi_retime_ms` (`last released + 1 −
      time`; default 10 000 ms, one poll interval);
    - its exchange `time` is strictly newer than every open-interest
      sample already admitted, buffered or re-timed.

    Otherwise it becomes a `LateEvent` gap. A stale or repeated snapshot is
    therefore never delivered as the current value.
    - **Why it is sound.** The state engine requires one strictly
      increasing canonical order over all streams (`MarketStateEngine::
      apply`: `OutOfOrder` and `Duplicate`). The re-timed sample sorts
      after everything released, so that order holds by construction.
      Open interest is a sampled state observation without an exchange
      id. Moving it later makes the value valid later than the exchange
      stamped it, never earlier, which keeps ADR-028's "never a validity
      time earlier than publication" and ADR-028 D6's "never delivered
      into the past". The value itself and `resolution_ms` are unchanged.
    - **The allowance is one poll interval.** A sample that is staler than
      that would overlap its successor's slot, so it stays a gap.
    - **Determinism.** The rule depends only on the order of the records
      and the hold-back state, so `oi_retime_ms` is a run parameter. It is
      journaled in `run_start` and read back by `RunParameters` (D4). Tests
      recompute it from the persisted records and check that a different
      allowance gives different output.
    - **Scope.** The raw format is unchanged: the raw record keeps the
      exchange `time` as its `event_time`, and replay re-times it the same
      way. Every other stream keeps the 2000 ms hold-back and the plain
      `LateEvent` rule.
    - **Known limitation: the exchange time is not on the event.**
      `OpenInterest` has a single `time` field. For a re-timed sample it
      carries the delivery time, so the exchange's sampling time does not
      reach the core. That time can be recovered from the raw record: its
      `event_time` is the exchange `time`, and the record's `receive_seq`
      identifies it. Carrying both times on the event would change the
      `mie-domain` event model, and with it the payload tie-break of
      ADR-028. That is left to a later decision, for when a feature
      consumes open interest; today the engine passes it through.
    - **Consequence for comparisons.** A re-timed time depends on arrival
      within the run. Replaying the same raw records with the same run
      parameters reproduces it exactly. Two independent captures of the
      same market, however, can deliver the same sample at different
      times.

### Documentation check (2026-10-06)

The Binance documentation site answered with a bot challenge. The checks
below therefore rest on two sources: the official connector, and live
probes made on 2026-10-06. The connector is `binance-connector-js`, with
`common` v2.4.7 (2026-08-31) and its USDⓈ-M `websocket-streams` modules.

- **Base URLs.** WebSocket `wss://fstream.binance.com` and REST
  `https://fapi.binance.com`. The market-data streams (`aggTrade`,
  `markPrice`, `forceOrder`, `kline`) are served on the **`/market`**
  route, as `/market/ws/<stream>`. Depth, book ticker and the individual
  trade stream are on `/public`. On the legacy route, `/ws/<stream>`
  connected but delivered no frame within 3 s.
- **Individual trades.** USDⓈ-M has an individual-trade stream,
  `<symbol>@trade` on `/public`. It stays out of scope: `aggTrade` remains
  the trade source, because its `a` is the ADR-028 sequence id.
- **Payloads.** The live frames carry fields that the documentation
  examples lack: aggTrade `nq` and `st`, markPrice `ap` and `st`. Kline
  frames have spaces after the commas. Verbatim storage and
  ignore-unknown-fields parsing absorb all of this.
- **Open interest.** `GET /fapi/v1/openInterest?symbol=BTCUSDT` returns
  `{"symbol","openInterest","time"}` with a decimal string.
- **Liveness.** In a 115 s capture, the `forceOrder` connection received no
  liquidation and still was never dropped: the server answered the 30 s
  client pings. The 24 h connection limit and the 3 min server pings follow
  the Binance documentation and were not re-verified against the site.
- **Open-interest lateness.** In the same capture, the response `time`
  trailed the poll by 3.8 to 7.9 s (maximum lateness 7943 ms). Under the
  plain D6 rule, all 12 live samples became `LateEvent` gaps, and the
  live core saw no open interest. D12 fixes this. The live smoke run after
  that change is reported on #9.

## Consequences

- Live and replay normalize the same bytes with the same code. Replay can
  recompute the live output, gaps included, from the raw store in
  `receive_seq` order plus the run's `run_start` parameters (D4). The raw
  store alone is not enough. One outage costs one stream, not all of them.
- No async runtime, no executor scheduling, no `rand`. Tests drive every
  loop with scripted transports and a fake clock.
- Five connections and six threads per capture. That is fine for one
  instrument (ADR-002), and depth (#10) adds connections the same way.
- Live delivery waits 2 s, and a later event is lost to a gap rather than
  delivered into the past. The exception is open interest: it reaches the
  core re-timed by a few seconds (D12), and its exchange `time` stays in
  the raw record.
- Each stream has a sub-second gap once a day at its planned reconnect.
  Every gap is visible.
- The TLS stack (rustls with the ring provider and the operating system's
  root store) adds four per-crate license exceptions in `deny.toml`: ISC for
  `ring`, `untrusted` and `rustls-webpki`, and BSD-3-Clause for `subtle`.
  Every rustls build depends on `subtle`.
- Restarts within the same UTC second share a `run_id`.

## Alternatives considered

- **tokio and tokio-tungstenite.** ADR-026 allows an internal runtime but
  does not need one at five low-rate connections. It would add an async
  dependency tree, and scheduling would leak into tests.
- **One combined-stream connection.** Its `{"stream","data"}` wrapper would
  have to be stripped before persisting, so the stored bytes would no longer
  be verbatim (ADR-030). One outage would also open a gap on every stream at
  once.
- **Overlapping (make-before-break) reconnects.** They avoid the daily
  sub-second gap, but need cross-connection dedupe for the id-less streams.
  Deferred until the soak shows that the gap matters.
- **Jittered backoff.** There is a single client per stream, so there is no
  herd to spread out. It would also need randomness.
- **Ordering open interest by receive time.** That would make a local
  clock order domain events, against ADR-028 D1.
- **A per-stream hold-back for open interest.** Holding only open interest
  back longer does not work. The engine needs one strictly increasing order
  across all streams, so no event after an outstanding open-interest slot
  could be released until that slot closes. In effect, every stream would
  get the longer hold-back.
- **A global 10 s hold-back.** It would make every live event 10 s late
  just to accommodate the slowest sampled stream.

## Accept when

A ≥ 24 h live soak of the merged `mie ingest` finishes. It includes the
staggered 23 h reconnects and one deliberate restart. `mie capture-report`
must print `PASS` over the window with the default
`--max-late-fraction 0`. The soak's numbers, posted on #9, settle the
remaining values:

- messages per second per stream (p50 and p99);
- channel high-water marks and blocked time;
- `LateEvent` count, re-timed count and maximum lateness per stream. These
  settle the hold-back and the open-interest allowance;
- raw bytes per day.
