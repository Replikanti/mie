# ADR-046: Open-interest poller honours Retry-After; rate-limit pauses from exchange limits

- Status: accepted
- Date: 2026-10-10

## Context

ADR-032 D9 polls open interest every 10 s, aligned to wall-clock multiples
of 10 s, and states: "A 418 or 429 defers the next poll by an exponential
backoff." The 2026-10-10 justification addendum of ADR-032 (section f, row
"418/429 backoff") records what PR #48 built. The poller reuses the
reconnect `backoff_*` values (250 ms doubling to 30 s, ADR-032 D8), counts
them from the response, and rounds the result up to the next 10 s slot.
`Retry-After` is not read.

#84 names the defect that row exposes. As long as the answer comes within
2 s, the first six consecutive 418/429 answers (250 ms to 8 s) do not delay
the poll past its normal slot. So the poller keeps polling every 10 s into
an exhausted rate limit, and ignores a pause the exchange asks for. On
Binance that is the path to an IP ban, which stops live capture for every
stream on the IP, not only open interest.

The addendum mechanism (`docs/adr/README.md`) cannot fix this: an addendum
changes no value or normative statement, and a number shown to be wrong
goes through a superseding ADR. Hence this ADR.

### Sources (checked 2026-10-10)

- Binance "LIMITS → IP Limits", `binance/binance-spot-api-docs`
  `rest-api.md` at commit `263ac1a`: "Repeatedly violating rate limits
  and/or failing to back off after receiving 429s will result in an
  automated IP ban (HTTP status 418)." Bans "scale in duration for repeat
  offenders, from 2 minutes to 3 days." "A `Retry-After` header is sent
  with a 418 or 429 responses and will give the number of seconds required
  to wait, in the case of a 429, to prevent a ban, or, in the case of a
  418, until the ban is over."
- The USDⓈ-M futures documentation site answered with a bot challenge
  (HTTP 202, empty body), so the futures wording is not re-verified. The
  spot text above is the same exchange's documented policy; the futures
  form of the header is inferred from it and from the connector below.
- Binance's official connector, `binance/binance-connector-js`
  `common/src/utils.ts` at commit `492cf95`, shared by its futures
  clients: `parseInt(headers['retry-after'], 10)`, so integer seconds.
- `GET /fapi/v1/exchangeInfo`, `rateLimits` (probed 2026-10-10, ADR-032
  addendum row "Open-interest poll 10 s"): `REQUEST_WEIGHT`, interval
  `MINUTE` × 1, limit 2400, with the `x-mbx-used-weight-1m` response header.
  An open-interest request weighs 1.
- RFC 9110 §10.2.3: `Retry-After = HTTP-date / delay-seconds`,
  `delay-seconds = 1*DIGIT`, the number of seconds to delay after the
  response is received.

## Decision

1. **Read `Retry-After` on 418 and 429 only, in the delay-seconds form.**
   After trimming surrounding whitespace, a non-empty run of ASCII digits
   is a delay in seconds. Anything else (an HTTP-date, a sign, a decimal,
   an empty value) is unusable. Binance documents seconds, and its
   connector parses an integer.
2. **A usable value replaces the fallback**, also when it is shorter than
   the fallback, and also 0. It is what the exchange asks for: for a 429,
   the time until the weight window lets requests through again.
3. **Cap the value at 259 200 s** (3 days). Larger values, and digit runs
   that overflow, count as 259 200 s.
4. **Without a usable value, pause 60 s after a 429 and 120 s after a
   418.** No escalation state is kept across consecutive answers: each
   rate-limited answer pauses by its own rule.
5. **The pause runs from the response.** The next poll is the first 10 s
   grid slot at or after response time + pause, so every poll, successful
   or not, stays on the ADR-032 D9 grid.
6. **Scope.** Only the open-interest poller changes. `backoff_initial` and
   `backoff_max` keep serving the WebSocket reconnect (ADR-032 D8) and the
   order-book snapshot fetcher (ADR-038 D3), which still reads no headers.
   `OiPoll` journal events, session ordinals, `session_id` and the ADR-030
   raw schema are unchanged. Failures other than 418/429 do not pause, as
   before.
7. **Transport seam.** `HttpGet::get_reply` returns the status, the body
   and the first `Retry-After` value as received (`None` when absent or
   not visible ASCII). It is a provided method: the default wraps `get`
   and sees no headers. `UreqHttp`, the poller's production client,
   overrides it. The poller calls only `get_reply`.

This supersedes the 418/429 clause of ADR-032 D9 and the "418/429 backoff"
row of ADR-032's 2026-10-10 justification addendum (section f).

### Numbers

| Number | Source | Breaks below | Breaks above |
|---|---|---|---|
| `Retry-After` cap 259 200 s (`RETRY_AFTER_MAX_S`) | The longest documented IP ban, 3 days. On a 418 the header counts down to the ban's end, so no honest value exceeds it | A real ban longer than the cap is cut short, and the poller polls into an active ban. Repeat offences lengthen bans (documented escalation, not re-verified), so polling into one risks a longer one | A corrupt or hostile value keeps open interest dark longer than any documented ban. Without a cap, a 20-digit value would stop the poller for the life of the process |
| Pause 60 s after a 429 without a usable header (`PAUSE_AFTER_429_MS`) | The 1-minute `REQUEST_WEIGHT` window of `exchangeInfo`. Two requests at least 60 s apart never share a 1-minute window, fixed or rolling. Counting from response receipt keeps their arrivals at the server at least 60 s apart, because the next request is sent after the response arrived | A second poll can land in the same exhausted window. That is the "failing to back off after receiving 429s" the documentation names as the path to a 418 | No fewer requests per window (already at most one), and every extra 10 s is one more missing open-interest sample |
| Pause 120 s after a 418 without a usable header (`PAUSE_AFTER_418_MS`) | The shortest documented IP ban, 2 minutes | The next poll falls inside every possible ban, which the documentation treats as continuing to send requests | Open interest stays dark past the end of a 2-minute ban, one sample per 10 s. Longer bans are not covered by any fixed value; the header is documented on every 418 (D1) |
| 10 s grid | ADR-032 D9, unchanged (#84 Out of scope) | Not decided here: ADR-032 addendum row "Open-interest poll 10 s" | Same row. Here it only adds less than 10 s to every pause (below) |

Rounding up to the grid adds less than one interval to every pause. At
sub-second response times, the fallbacks give 70 s between polls after a
429 and 130 s after a 418; a header of N s gives the first slot at or
after response + N s.

## Consequences

- No poll is sent before the time the exchange asked for, and none twice
  in the weight window that answered 429. The defect of #84 is gone.
- Successful polls stay on the 10 s wall-clock grid; their samples, raw
  records and normalization are unchanged. Only live scheduling changes;
  replay never polls.
- A legitimate long ban keeps open interest dark for its whole length, up
  to 3 days. It shows as 418 `oi_poll` lines in the journal with Binance's
  body detail, and as one `Disconnected` gap when polling resumes.
  Shutdown stays responsive: the wait sleeps in 100 ms slices.
- A 429 without the header now costs about 7 samples instead of 0–1. A
  garbled header that parses as a huge number costs up to 3 days of open
  interest.
- ADR-038 D3 says the snapshot fetcher backs off "as for open interest".
  That is no longer literally true; the fetcher itself is unchanged (#84
  Out of scope), and its module doc no longer makes the comparison.
- The HTTP-date form of `Retry-After` falls back to D4. If Binance's
  futures API ever sends it, the poller pauses 60 or 120 s instead of the
  requested time.

## Alternatives considered

- **A justification addendum to ADR-032** (as #84 first asked). The
  `docs/adr/README.md` rule routes a changed value or normative statement
  through a superseding ADR.
- **max(header, own pause).** With a fixed fallback it ignores every header
  below 60 s. An ordinary 429 counts down to the end of its window, so this
  would wait longer than the exchange asks, and drop samples for nothing.
- **min(header, own pause).** It would poll earlier than the exchange asks,
  which is the behaviour the documentation punishes with a ban.
- **Keep the ADR-032 D8 exponential values and count them from the next
  slot** (20, 40, 60 s gaps). Its first steps still put a second request
  into the exhausted weight window. The values were set for WebSocket
  connection attempts against a 300-per-5-min connection limit
  (documented, not re-verified), not for
  REST weight.
- **Retry off the grid, exactly at response + header.** A success off the
  grid breaks D9's alignment, and it sits less than 10 s from its
  successor's slot, so two samples land within one `resolution_ms`.
- **No cap, a cap at `backoff_max` (30 s), or a cap at 2 min.** No cap
  lets one corrupt value stop the poller indefinitely. A 30 s or 2 min cap
  cuts every ban longer than the cap short and polls into it (cap row
  above).
- **HTTP-date support through the `httpdate` crate or a hand parser.**
  Binance documents seconds and its connector parses integers. A
  dependency, or parser code, for an undocumented form does not meet the
  `Cargo.toml` justification bar; the fallback covers that form.
- **A required trait method instead of a provided one.** Twelve
  implementations would change, including the archive and depth test
  doubles that are out of scope. The risk of a production transport
  silently keeping the header-blind default is covered by the loopback
  test on `UreqHttp`, the only production `HttpGet` the poller receives
  (wired in `mie-cli`).
- **Honour `Retry-After` on 503 too.** Binance does not document it for
  this API.
- **An escalating fallback for repeated header-less 418s.** No documented
  number exists to derive the steps from, and the header is documented on
  every 418.

## Why accepted without an `Accept when`

A live 418 or 429 cannot be provoked safely: it needs more than 2400
weight per minute from the capture IP, and the resulting ban would stop
every stream on that IP. The fake-clock tests in
`crates/mie-adapter-binance/tests/live_fake_transport.rs` pin the schedule
(header honoured, both fallbacks, at most one poll per 60 s window while
rate-limited, polls on the grid), and
`crates/mie-adapter-binance/tests/http_reply_loopback.rs` pins that the
production client returns the header.
