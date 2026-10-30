# #28 Order-flow interactions and counter-hypotheses — report

- Issue: #28. Pre-registered by [ADR-058](../adr/058-order-flow-conditional-studies.md)
  (accepted with this report).
- Run: 2026-10-30, once, with the inputs of
  [`docs/direction.md`](../direction.md) *Personal inputs*.
- Queries: [`028-order-flow-interactions.sql`](028-order-flow-interactions.sql),
  SHA-256 `dd32d605081d6c80992655e16f453d143ee4e3ba5b17f4bbe399fcee0e7144a1`.

This report records every result the pre-registered output holds. Null
results and `insufficient` cells are included. No cell is promoted: the
cells that read `hypothesis` or `counter` are handed to the Research Lab as
hypotheses (last section).

## Summary

- **Readings beyond chance.** Of 100 primary cells, 62 hold enough
  independent units to be read (N). In 17 of them the interval of E excludes
  0 (K). If no effect existed, (1 − c)·N = 3.1 would be expected. Of 995
  split cells, 334 are read and 77 exclude 0, against 16.7 expected. By
  family, K is well above the chance expectation for P2 (primary 8 of 12
  against 0.6; split 58 of 125 against 6.3), for P1-sell (primary 3 of 12,
  all at 4h) and for P1-trig (primary 4 of 12). It is at about chance for
  P1-buy (1 of 12; split 4 of 62 against 3.1). P4 reads 0 of 11 primary
  cells (split 1 of 27 against 1.4). P3 has 3 primary cells large enough to
  read, and 1 of them excludes 0; none of its split cells can be read.
- **Direction.** Seven of P2's eight primary cells read `counter` at 5m–1h:
  after high effort with a large price response, the tilt toward the
  aggressor's barrier is below the time-standardized baseline's (E between
  −0.0072 and −0.0295). The eighth, w=1h at 4h, reads `hypothesis`
  (E 0.0449). P1-sell's three cells read `hypothesis` at 4h (E 0.0185,
  0.0505, 0.1055 for w = 5m, 15m, 1h): after absorbed selling, the tilt
  toward the barrier away from the aggressor is above the baseline's.
  P1-trig reads `hypothesis` for `trigger.effort_result.15m@1` at 1h and
  4h and `counter` for `trigger.effort_result.1h@1` at 5m and 15m.
- **No net taker edge at μ = 0.10 R.** Every effect read is far below the
  taker round trip in R at its horizon. The largest primary |E| is 0.1055
  (P1-sell w=1h at 4h), where the fee is 0.25 R. At 5m and 15m the fee
  (2.14 R and 1.12 R) exceeds the largest gross race outcome possible
  (+1 R). No 1d cell, the only horizon where the fee (0.09 R) is below μ,
  can be read on one year. So no cell, primary or split, implies a net
  taker edge of μ = 0.10 R. See *Effects against costs*.
- **Block sensitivity.** 3 primary cells and 12 split cells read
  differently with the 28-day block (listed below).
- **No FDR here.** K against (1 − c)·N is a count, not a test. The
  permutation null and the false-discovery-rate correction of
  `docs/direction.md` T3 belong to the Research Lab (#32).

## Dataset

The #12 development year, records = replay = 2025-10-01 … 2026-09-30
(`1759276800000..1790812800000` ms), dataset format 2 (ADR-058 D2), built
once with the ADR-057 D9 flags:

| Manifest line | Value |
|---|---|
| `content` | `5e8e17b071487d358cb163d1dffae15efeda2fc8aa0250a9b9c4096c2a07f94c` |
| `data` | `5d9e27f862ac54dd93cff904458e9ee92767d5105a9d13a144351cba069f224d` |
| `engine` | `4c844593779da2e3-1` |
| `sampling` | `conditional.sampling@1 2c71fff23e8b0415` |
| `labeling` | `conditional.labels@2 6f90bff079e746f2` |
| `code` | `git:b6485454e7609372203c17b0b27bd4513448f0e5` (clean) |

Excluded ranges: none. The calibration slice (2024-10-01 … 2024-12-31,
version `c20761ef7e96acd21e3f6dae04cb9f2f20efb34c058dda1e775264e608f892ac`)
lies in the hold-out year, outside the year read, so `MIE_STUDY_EXCLUDE`
was empty. The study read neither the hold-out store nor any live store
(ADR-058 D1); the two R0 live builds of the acceptance were compared byte
for byte and not queried. The tool asserted D1 (verify, provenance, the ADR-057
reconciliation counts, 3 × 561 695 effort rows, 0 twin mismatches) before
it printed anything.

## Commands and inputs

From main `b648545`, clean. Binaries: `cargo build --release -p mie-cli`
and `cargo test --release -p mie-cli --test outcome_views --test
order_flow_study --no-run`. Each step ran under an 8 GB memory cap, one
after another:

```sh
mie conditional build --config <archive config> --source archive \
    --streams aggTrades,metrics --trade-correction fold \
    --from 2025-10-01 --to 2026-09-30 --out <year>
mie conditional build --config <R0 live config> --source live \
    --from 1792191366185 --to 1792315774245 --out <live 1>   # and <live 2>
mie conditional verify <year>    # and <live 1>, <live 2>
MIE_OUTCOME_DATASET=<year> \
    cargo test --release -p mie-cli --test outcome_views \
    measure_outcome_views -- --ignored --exact --nocapture
MIE_OUTCOME_DATASET=<year> MIE_STUDY_CONFIDENCE=0.95 MIE_STUDY_MU=0.10 \
    MIE_STUDY_FEE_BPS=10 MIE_STUDY_EXCLUDE= \
    cargo test --release -p mie-cli --test order_flow_study \
    measure_order_flow_study -- --ignored --exact --nocapture
```

| Input | Value | Source |
|---|---|---|
| Confidence level c | 0.95 | `docs/direction.md` *Personal inputs* |
| μ | 0.10 R | `docs/direction.md` *Personal inputs* |
| z = Φ⁻¹((1 + c)/2) | 1.959964 | ADR-058 D6 |
| n_min = ⌈(z/μ)²⌉ | 385 | ADR-058 D6 (T1 with σ ≤ 1) |
| Fee | 10 bps round trip (Binance USDⓈ-M VIP 0, taker on both sides) | `docs/direction.md` *Personal inputs* |
| Resamples, blocks, seed | 10 000; 56 days (28-day sensitivity); 28 | ADR-058 D5 |

Deviations from ADR-058: none. The build, verification and determinism
facts, and why the run's commit is `b648545`, are in ADR-058
*Acceptance*. `measure_order_flow_study` ran 1:18.61 (m:ss), peak RSS
1.7 GB.

## Reading note: units against n_min

A cell is read only when its independent units reach n_min = 385
(ADR-058 D6). Units are episodes thinned so that the counted races at
horizon H do not overlap, so one year caps them at its count of disjoint
windows: 105 120 / 35 040 / 8 760 / 2 190 / 365 at 5m / 15m / 1h / 4h /
1d. With 365 < 385, every 1d cell reads `insufficient` by construction.
At 4h a cell needs units in 385 of the 2 190 windows (17.6 %), so most
4h split cells read `insufficient` too. Episode counts bound P3 and P4
further: P3 with w = 15m and 1h has 294 and 211 episodes, and
`trigger.sfp.4h@1` and `trigger.sfp.1d@1` have 203 and 35, so none of
these cells can be read at any horizon. This follows from n_min and one
year of data, as ADR-058 D6 states; it is not a failure of the study. The
shares, E and the interval of an `insufficient` cell are printed but not
read.

Cells counted: 38 of 100 primary cells and 661 of 995 split cells read
`insufficient`. The split tables include the zero-member rows (`not_ready`
location and `flat` CVD, every one with n = 0 and reading `insufficient
(undefined 10000)`); they are kept as printed. Exclusions before any race:
1 445 / 1 455 / 1 500 baseline records whose 5m / 15m / 1h effort window
was not ready (the warm-up at the start of the year); no record had
incomplete coverage. A few P4 split and primary cells drop 1–2 members
whose stratum held no baseline race (column `dropped`).

## Effects against costs

The fee column is the taker round trip in R at the baseline's median
barrier half-width (ADR-058 D9; barrier widths only, no race read). On
the symmetric barriers the gross outcome per race in R is T, and the
effect against the baseline is E = T − T_b. Over the cells that were read
(not `insufficient`):

| H | fee (R) | read primary | largest primary \|E\| | largest primary \|T\| | read split | largest split \|E\| | largest split \|T\| |
|---|--:|--:|--:|--:|--:|--:|--:|
| 5m | 2.14 | 16 | 0.0357 | 0.0356 | 90 | 0.0732 | 0.0796 |
| 15m | 1.12 | 16 | 0.0351 | 0.0288 | 90 | 0.0681 | 0.0709 |
| 1h | 0.52 | 16 | 0.0718 | 0.0872 | 88 | 0.0878 | 0.1069 |
| 4h | 0.25 | 14 | 0.1055 | 0.0955 | 66 | 0.1161 | 0.1061 |
| 1d | 0.09 | 0 | – | – | 0 | – | – |

A net taker edge of μ needs a gross outcome of at least fee + μ per race:
0.35 R at 4h and 0.62 R at 1h, more than 1 R at 5m and 15m, which no race
outcome can reach. The largest gross |T| read is 0.1069. So no cell read
here implies a net taker edge at μ = 0.10 R, at any horizon. Net outcomes
(fees, slippage, funding) are the Research Lab's (#30, #32, #33); this
table only bounds them.

## Multiplicity

Transcribed from the tool's output below (*Multiplicity*). N counts only
the cells that were read; `insufficient` cells are not in N.

| family | cells | read (N) | intervals excluding 0 (K) | expected by chance (1 − c)·N |
|---|---|--:|--:|--:|
| P1-buy | primary | 12 | 1 | 0.6 |
| P1-buy | secondary | 62 | 4 | 3.1 |
| P1-sell | primary | 12 | 3 | 0.6 |
| P1-sell | secondary | 65 | 7 | 3.3 |
| P2 | primary | 12 | 8 | 0.6 |
| P2 | secondary | 125 | 58 | 6.3 |
| P3 | primary | 3 | 1 | 0.2 |
| P3 | secondary | 0 | 0 | 0.0 |
| P4 | primary | 11 | 0 | 0.6 |
| P4 | secondary | 27 | 1 | 1.4 |
| P1-trig | primary | 12 | 4 | 0.6 |
| P1-trig | secondary | 55 | 7 | 2.8 |

Split cells are subsets of their population and overlap each other (a
record in two location bins is in both cells), so the split lines are not
independent counts. No permutation null or FDR is applied here (ADR-058
D7).

## Block-sensitive cells

Cells whose reading changes with the 28-day block (ADR-058 D7), as
printed:

| population | table | split | H | units | E | interval (56 d) | reading |
|---|---|---|---|--:|--:|---|---|
| P1-buy w=5m | by regime | low | 5m | 1561 | 0.0266 | [0.0018, 0.0559] | hypothesis (block-sensitive (28d: inconclusive)) |
| P1-buy w=1h | by location | neither | 1h | 692 | 0.0613 | [0.0001, 0.1145] | hypothesis (block-sensitive (28d: inconclusive)) |
| P1-sell w=15m | by cvd | with | 4h | 785 | 0.0428 | [0.0002, 0.0854] | hypothesis (block-sensitive (28d: inconclusive)) |
| P2 w=5m | primary | all | 1h | 5362 | -0.0072 | [-0.0207, -0.0001] | counter (block-sensitive (28d: inconclusive)) |
| P2 w=5m | by regime | low | 5m | 4103 | -0.0238 | [-0.0569, 0.0035] | inconclusive (block-sensitive (28d: counter)) |
| P2 w=5m | by location | value_edge | 5m | 2397 | -0.0253 | [-0.0493, -0.0031] | counter (block-sensitive (28d: inconclusive)) |
| P2 w=5m | by location | structural_extreme | 1h | 1024 | 0.0844 | [0.0132, 0.1499] | hypothesis (block-sensitive (28d: inconclusive)) |
| P2 w=5m | by side | sell | 15m | 6989 | -0.0106 | [-0.0234, -0.0013] | counter (block-sensitive (28d: inconclusive)) |
| P2 w=15m | by location | structural_extreme | 1h | 722 | 0.0878 | [0.0085, 0.1511] | hypothesis (block-sensitive (28d: inconclusive)) |
| P2 w=15m | by cvd | with | 5m | 3893 | -0.0150 | [-0.0355, 0.0001] | inconclusive (block-sensitive (28d: counter)) |
| P2 w=15m | by cvd | with | 1h | 2910 | -0.0091 | [-0.0287, 0.0006] | inconclusive (block-sensitive (28d: counter)) |
| P2 w=1h | by regime | medium | 5m | 598 | -0.0120 | [-0.0413, 0.0005] | inconclusive (block-sensitive (28d: counter)) |
| P2 w=1h | by location | value_edge | 1h | 561 | -0.0436 | [-0.0957, -0.0003] | counter (block-sensitive (28d: inconclusive)) |
| P3 w=5m | primary | all | 15m | 440 | -0.0314 | [-0.0602, -0.0049] | counter (block-sensitive (28d: inconclusive)) |
| P1-trig trigger.effort_result.1h@1 | primary | all | 15m | 1305 | -0.0264 | [-0.0560, -0.0003] | counter (block-sensitive (28d: inconclusive)) |

Two kinds appear: 11 cells whose 56-day interval excludes 0 read
`inconclusive` with 28-day blocks (3 primary, 8 split), and 4 P2 split
cells that read `inconclusive` with 56-day blocks read `counter` with
28-day blocks.

## Hypotheses handed to the Research Lab

For #30 (walk-forward) and #32 (T3: permutation null and FDR). These are
the cells whose 56-day interval excludes 0. Each is a hypothesis with its
gross effect E against the time-standardized baseline; none is promoted,
none is net of costs, and none has passed T3. The Research Lab tests them
on data this study did not read (the hold-out from 2025-01-01 and live
data). Cells marked block-sensitive carry that flag.

### Primary cells (17)

| population | H | n | episodes | units | E | interval | reading |
|---|---|--:|--:|--:|--:|---|---|
| P1-buy w=1h | 1h | 9702 | 720 | 720 | 0.0718 | [0.0105, 0.1203] | hypothesis |
| P1-sell w=5m | 4h | 13147 | 6242 | 1490 | 0.0185 | [0.0017, 0.0345] | hypothesis |
| P1-sell w=15m | 4h | 11931 | 2604 | 1119 | 0.0505 | [0.0067, 0.0910] | hypothesis |
| P1-sell w=1h | 4h | 11003 | 798 | 599 | 0.1055 | [0.0562, 0.1552] | hypothesis |
| P2 w=5m | 5m | 56318 | 15033 | 14866 | -0.0295 | [-0.0473, -0.0144] | counter |
| P2 w=5m | 15m | 56318 | 15033 | 11119 | -0.0165 | [-0.0284, -0.0070] | counter |
| P2 w=5m | 1h | 56317 | 15032 | 5362 | -0.0072 | [-0.0207, -0.0001] | counter (block-sensitive (28d: inconclusive)) |
| P2 w=15m | 5m | 58785 | 5569 | 5569 | -0.0176 | [-0.0388, -0.0021] | counter |
| P2 w=15m | 15m | 58785 | 5569 | 5551 | -0.0132 | [-0.0313, -0.0008] | counter |
| P2 w=1h | 5m | 61163 | 1610 | 1610 | -0.0163 | [-0.0299, -0.0060] | counter |
| P2 w=1h | 15m | 61163 | 1610 | 1610 | -0.0131 | [-0.0334, -0.0003] | counter |
| P2 w=1h | 4h | 61033 | 1606 | 1078 | 0.0449 | [0.0130, 0.0676] | hypothesis |
| P3 w=5m | 15m | 2273 | 447 | 440 | -0.0314 | [-0.0602, -0.0049] | counter (block-sensitive (28d: inconclusive)) |
| P1-trig trigger.effort_result.15m@1 | 1h | 7496 | 4388 | 2930 | 0.0231 | [0.0071, 0.0435] | hypothesis |
| P1-trig trigger.effort_result.15m@1 | 4h | 7478 | 4378 | 1382 | 0.0307 | [0.0112, 0.0527] | hypothesis |
| P1-trig trigger.effort_result.1h@1 | 5m | 3738 | 1305 | 1305 | -0.0357 | [-0.0674, -0.0058] | counter |
| P1-trig trigger.effort_result.1h@1 | 15m | 3738 | 1305 | 1305 | -0.0264 | [-0.0560, -0.0003] | counter (block-sensitive (28d: inconclusive)) |

### Split cells (77)

| population | split | value | H | n | episodes | units | E | interval | reading |
|---|---|---|---|--:|--:|--:|--:|---|---|
| P1-buy w=5m | regime | low | 5m | 3218 | 1566 | 1561 | 0.0266 | [0.0018, 0.0559] | hypothesis (block-sensitive (28d: inconclusive)) |
| P1-buy w=5m | regime | low | 4h | 3218 | 1566 | 462 | 0.0664 | [0.0302, 0.0949] | hypothesis |
| P1-buy w=15m | regime | high | 15m | 2447 | 520 | 520 | 0.0646 | [0.0013, 0.1417] | hypothesis |
| P1-buy w=1h | location | neither | 1h | 8764 | 692 | 692 | 0.0613 | [0.0001, 0.1145] | hypothesis (block-sensitive (28d: inconclusive)) |
| P1-sell w=5m | location | neither | 4h | 11578 | 5644 | 1436 | 0.0252 | [0.0050, 0.0450] | hypothesis |
| P1-sell w=15m | regime | low | 15m | 3399 | 732 | 731 | 0.0681 | [0.0055, 0.1414] | hypothesis |
| P1-sell w=15m | regime | low | 1h | 3399 | 732 | 579 | 0.0739 | [0.0066, 0.1458] | hypothesis |
| P1-sell w=15m | location | neither | 4h | 10578 | 2421 | 1075 | 0.0540 | [0.0074, 0.0977] | hypothesis |
| P1-sell w=15m | cvd | with | 4h | 8038 | 1724 | 785 | 0.0428 | [0.0002, 0.0854] | hypothesis (block-sensitive (28d: inconclusive)) |
| P1-sell w=1h | location | neither | 4h | 9759 | 765 | 586 | 0.1023 | [0.0448, 0.1624] | hypothesis |
| P1-sell w=1h | cvd | with | 4h | 8464 | 599 | 458 | 0.1161 | [0.0541, 0.1863] | hypothesis |
| P2 w=5m | regime | low | 15m | 15648 | 4153 | 3107 | -0.0152 | [-0.0392, -0.0028] | counter |
| P2 w=5m | regime | medium | 5m | 16186 | 4316 | 4261 | -0.0242 | [-0.0438, -0.0122] | counter |
| P2 w=5m | regime | medium | 1h | 16186 | 4316 | 1571 | -0.0162 | [-0.0443, -0.0072] | counter |
| P2 w=5m | regime | high | 5m | 10680 | 2963 | 2938 | -0.0437 | [-0.0662, -0.0264] | counter |
| P2 w=5m | regime | high | 15m | 10680 | 2963 | 2222 | -0.0263 | [-0.0550, -0.0105] | counter |
| P2 w=5m | regime | high | 1h | 10679 | 2962 | 1136 | -0.0186 | [-0.0508, -0.0077] | counter |
| P2 w=5m | regime | high | 4h | 10677 | 2961 | 506 | -0.0445 | [-0.0805, -0.0340] | counter |
| P2 w=5m | regime | extreme | 5m | 12534 | 3450 | 3416 | -0.0606 | [-0.0825, -0.0460] | counter |
| P2 w=5m | regime | extreme | 15m | 12534 | 3450 | 2548 | -0.0509 | [-0.0759, -0.0428] | counter |
| P2 w=5m | regime | extreme | 1h | 12534 | 3450 | 1274 | -0.0469 | [-0.0863, -0.0356] | counter |
| P2 w=5m | regime | extreme | 4h | 12534 | 3450 | 489 | -0.0517 | [-0.0938, -0.0337] | counter |
| P2 w=5m | location | value_edge | 5m | 5176 | 2403 | 2397 | -0.0253 | [-0.0493, -0.0031] | counter (block-sensitive (28d: inconclusive)) |
| P2 w=5m | location | structural_extreme | 1h | 1860 | 1214 | 1024 | 0.0844 | [0.0132, 0.1499] | hypothesis (block-sensitive (28d: inconclusive)) |
| P2 w=5m | location | neither | 5m | 49625 | 13846 | 13717 | -0.0314 | [-0.0486, -0.0165] | counter |
| P2 w=5m | location | neither | 15m | 49625 | 13846 | 10425 | -0.0190 | [-0.0307, -0.0098] | counter |
| P2 w=5m | location | neither | 1h | 49624 | 13845 | 5135 | -0.0102 | [-0.0246, -0.0028] | counter |
| P2 w=5m | side | buy | 5m | 27934 | 8462 | 8448 | -0.0354 | [-0.0556, -0.0166] | counter |
| P2 w=5m | side | buy | 15m | 27934 | 8462 | 7075 | -0.0225 | [-0.0389, -0.0094] | counter |
| P2 w=5m | side | sell | 5m | 28384 | 8377 | 8359 | -0.0237 | [-0.0413, -0.0092] | counter |
| P2 w=5m | side | sell | 15m | 28384 | 8377 | 6989 | -0.0106 | [-0.0234, -0.0013] | counter (block-sensitive (28d: inconclusive)) |
| P2 w=5m | cvd | with | 5m | 32231 | 9227 | 9199 | -0.0284 | [-0.0478, -0.0134] | counter |
| P2 w=5m | cvd | with | 15m | 32231 | 9227 | 7597 | -0.0147 | [-0.0312, -0.0022] | counter |
| P2 w=5m | cvd | against | 5m | 24087 | 7850 | 7841 | -0.0310 | [-0.0486, -0.0144] | counter |
| P2 w=5m | cvd | against | 15m | 24087 | 7850 | 6643 | -0.0190 | [-0.0327, -0.0069] | counter |
| P2 w=15m | regime | low | 5m | 16798 | 1606 | 1606 | -0.0243 | [-0.0550, -0.0062] | counter |
| P2 w=15m | regime | low | 15m | 16798 | 1606 | 1598 | -0.0225 | [-0.0527, -0.0083] | counter |
| P2 w=15m | regime | high | 5m | 10931 | 1171 | 1171 | -0.0256 | [-0.0486, -0.0172] | counter |
| P2 w=15m | regime | high | 15m | 10931 | 1171 | 1168 | -0.0292 | [-0.0599, -0.0204] | counter |
| P2 w=15m | regime | high | 1h | 10931 | 1171 | 800 | -0.0403 | [-0.0865, -0.0228] | counter |
| P2 w=15m | regime | high | 4h | 10931 | 1171 | 438 | -0.0729 | [-0.1253, -0.0524] | counter |
| P2 w=15m | regime | extreme | 5m | 12945 | 1286 | 1286 | -0.0459 | [-0.0841, -0.0226] | counter |
| P2 w=15m | regime | extreme | 15m | 12945 | 1286 | 1284 | -0.0413 | [-0.0853, -0.0278] | counter |
| P2 w=15m | regime | extreme | 1h | 12945 | 1286 | 890 | -0.0469 | [-0.1038, -0.0358] | counter |
| P2 w=15m | regime | extreme | 4h | 12945 | 1286 | 421 | -0.0495 | [-0.1064, -0.0292] | counter |
| P2 w=15m | location | structural_extreme | 1h | 1468 | 772 | 722 | 0.0878 | [0.0085, 0.1511] | hypothesis (block-sensitive (28d: inconclusive)) |
| P2 w=15m | location | neither | 5m | 52375 | 5284 | 5284 | -0.0215 | [-0.0413, -0.0075] | counter |
| P2 w=15m | location | neither | 15m | 52375 | 5284 | 5271 | -0.0168 | [-0.0348, -0.0050] | counter |
| P2 w=15m | location | neither | 1h | 52375 | 5284 | 3577 | -0.0111 | [-0.0293, -0.0013] | counter |
| P2 w=15m | side | buy | 5m | 28882 | 3377 | 3377 | -0.0274 | [-0.0482, -0.0125] | counter |
| P2 w=15m | side | buy | 15m | 28882 | 3377 | 3375 | -0.0235 | [-0.0394, -0.0137] | counter |
| P2 w=15m | cvd | with | 15m | 37781 | 3893 | 3886 | -0.0138 | [-0.0323, -0.0017] | counter |
| P2 w=15m | cvd | against | 5m | 21004 | 2979 | 2979 | -0.0221 | [-0.0466, -0.0038] | counter |
| P2 w=1h | regime | low | 5m | 17461 | 509 | 509 | -0.0225 | [-0.0535, -0.0049] | counter |
| P2 w=1h | regime | low | 15m | 17461 | 509 | 509 | -0.0354 | [-0.0765, -0.0126] | counter |
| P2 w=1h | regime | low | 1h | 17461 | 509 | 509 | -0.0333 | [-0.0824, -0.0119] | counter |
| P2 w=1h | regime | medium | 15m | 16910 | 598 | 598 | -0.0139 | [-0.0453, -0.0020] | counter |
| P2 w=1h | regime | high | 5m | 11649 | 426 | 426 | -0.0192 | [-0.0460, -0.0065] | counter |
| P2 w=1h | regime | high | 15m | 11649 | 426 | 426 | -0.0216 | [-0.0618, -0.0032] | counter |
| P2 w=1h | regime | high | 1h | 11649 | 426 | 426 | -0.0244 | [-0.0761, -0.0034] | counter |
| P2 w=1h | regime | extreme | 5m | 13829 | 428 | 428 | -0.0309 | [-0.0585, -0.0148] | counter |
| P2 w=1h | location | value_edge | 1h | 5050 | 561 | 561 | -0.0436 | [-0.0957, -0.0003] | counter (block-sensitive (28d: inconclusive)) |
| P2 w=1h | location | neither | 5m | 55363 | 1567 | 1567 | -0.0160 | [-0.0298, -0.0050] | counter |
| P2 w=1h | location | neither | 4h | 55234 | 1563 | 1055 | 0.0456 | [0.0135, 0.0696] | hypothesis |
| P2 w=1h | side | buy | 5m | 29130 | 1020 | 1020 | -0.0240 | [-0.0420, -0.0081] | counter |
| P2 w=1h | side | buy | 15m | 29130 | 1020 | 1020 | -0.0236 | [-0.0519, -0.0020] | counter |
| P2 w=1h | side | buy | 4h | 29031 | 1017 | 775 | 0.0514 | [0.0037, 0.0887] | hypothesis |
| P2 w=1h | cvd | with | 5m | 47669 | 1331 | 1331 | -0.0143 | [-0.0269, -0.0052] | counter |
| P2 w=1h | cvd | with | 4h | 47565 | 1327 | 959 | 0.0533 | [0.0210, 0.0774] | hypothesis |
| P4 trigger.sfp.15m@1 | regime | medium | 4h | 1264 | 973 | 465 | -0.0697 | [-0.1094, -0.0245] | counter |
| P1-trig trigger.effort_result.5m@1 | regime | medium | 15m | 4024 | 3308 | 2487 | 0.0279 | [0.0090, 0.0521] | hypothesis |
| P1-trig trigger.effort_result.5m@1 | regime | high | 4h | 2599 | 2140 | 465 | 0.0488 | [0.0097, 0.0898] | hypothesis |
| P1-trig trigger.effort_result.15m@1 | regime | low | 1h | 2174 | 1286 | 885 | 0.0753 | [0.0376, 0.1273] | hypothesis |
| P1-trig trigger.effort_result.15m@1 | location | neither | 1h | 6649 | 4000 | 2733 | 0.0269 | [0.0090, 0.0484] | hypothesis |
| P1-trig trigger.effort_result.15m@1 | location | neither | 4h | 6633 | 3990 | 1332 | 0.0306 | [0.0107, 0.0515] | hypothesis |
| P1-trig trigger.effort_result.1h@1 | location | neither | 5m | 3326 | 1220 | 1220 | -0.0472 | [-0.0786, -0.0158] | counter |
| P1-trig trigger.effort_result.1h@1 | location | neither | 15m | 3326 | 1220 | 1220 | -0.0349 | [-0.0668, -0.0085] | counter |

Not handed on: the long horizons. Every 1d cell and most 4h split cells
are `insufficient` on one year, and P3 (w = 15m, 1h), `trigger.sfp.4h@1`
and `trigger.sfp.1d@1` cannot be read at all. They need more data than
one year, not another reading of this one.

## Results: the tool's output

The output of `measure_order_flow_study`, transcribed from its first line
to its multiplicity table, with every heading two levels lower and runs of
blank lines collapsed: no row is dropped and no value is rounded or
re-derived. Columns (ADR-058 D9): `n` the members whose race at H ran (hypothesis, counter or
neither barrier first); `censored` and `unavailable` the members whose
race did not run; `dropped` the members with no baseline race in their
stratum (none of these three is in `n`); `episodes`, `units` (D6); `P_h`, `P_c`, `P_n` the hypothesis,
counter and neither shares; `T = P_h − P_c`; `T_b` the standardized
baseline tilt; `E = T − T_b` with its 56-day interval at c; the reading
(D7) and its flags. The hypothesis and counter-hypothesis of each pair are
those of ADR-058 D3.

### Order-flow interaction study (#28, ADR-058)

dataset content 5e8e17b071487d358cb163d1dffae15efeda2fc8aa0250a9b9c4096c2a07f94c, data 5d9e27f862ac54dd93cff904458e9ee92767d5105a9d13a144351cba069f224d, engine 4c844593779da2e3-1, conditional.sampling@1 2c71fff23e8b0415, conditional.labels@2 6f90bff079e746f2, code git:b6485454e7609372203c17b0b27bd4513448f0e5
queries docs/research/028-order-flow-interactions.sql sha256 dd32d605081d6c80992655e16f453d143ee4e3ba5b17f4bbe399fcee0e7144a1
excluded none; twin mismatches 0

#### Inputs

c = 0.95, μ = 0.1 R, z = 1.959964, n_min = 385; fee 10 bps round trip; 10000 resamples, blocks 56 and 28 days, seed 28; 365 baseline days.

#### Horizons

| H | baselines with barriers | median half-width (bps) | fee (R) | disjoint windows in the days read |
|---|--:|--:|--:|--:|
| 5m | 525525 | 4.68 | 2.14 | 105120 |
| 15m | 525375 | 8.93 | 1.12 | 35040 |
| 1h | 524700 | 19.13 | 0.52 | 8760 |
| 4h | 522000 | 39.36 | 0.25 | 2190 |
| 1d | 504000 | 109.55 | 0.09 | 365 |

#### Exclusions before any race is read

| universe | window | trigger | records | effort not ready | incomplete coverage | no failed side |
|---|---|---|--:|--:|--:|--:|
| P1-trig | 5m | trigger.effort_result.5m@1 | 12952 | 0 | 0 | 0 |
| P1-trig | 15m | trigger.effort_result.15m@1 | 7496 | 0 | 0 | 0 |
| P1-trig | 1h | trigger.effort_result.1h@1 | 3738 | 0 | 0 | 0 |
| P4 | – | trigger.failed_auction@1 | 697 | 0 | 0 | 0 |
| P4 | – | trigger.sfp.15m@1 | 4267 | 0 | 0 | 0 |
| P4 | – | trigger.sfp.1d@1 | 44 | 0 | 0 | 0 |
| P4 | – | trigger.sfp.1h@1 | 1063 | 0 | 0 | 0 |
| P4 | – | trigger.sfp.4h@1 | 257 | 0 | 0 | 0 |
| baseline | 5m | – | 525599 | 1445 | 0 | 0 |
| baseline | 15m | – | 525599 | 1455 | 0 | 0 |
| baseline | 1h | – | 525599 | 1500 | 0 | 0 |

#### P1-buy w=5m

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 12302 | 0 | 0 | 0 | 5830 | 5809 | 0.4741 | 0.4668 | 0.0592 | 0.0073 | -0.0035 | 0.0108 | [-0.0115, 0.0325] | inconclusive |
| all | 15m | 12302 | 0 | 0 | 0 | 5830 | 4936 | 0.4763 | 0.4670 | 0.0567 | 0.0093 | 0.0018 | 0.0075 | [-0.0187, 0.0341] | inconclusive |
| all | 1h | 12302 | 0 | 0 | 0 | 5830 | 3239 | 0.4724 | 0.4689 | 0.0587 | 0.0036 | 0.0124 | -0.0088 | [-0.0383, 0.0201] | inconclusive |
| all | 4h | 12265 | 2 | 35 | 0 | 5813 | 1472 | 0.4665 | 0.4544 | 0.0791 | 0.0121 | 0.0140 | -0.0018 | [-0.0252, 0.0248] | inconclusive |
| all | 1d | 11949 | 5 | 348 | 0 | 5648 | 323 | 0.4634 | 0.4491 | 0.0875 | 0.0143 | 0.0123 | 0.0020 | [-0.0260, 0.0327] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 3218 | 0 | 0 | 0 | 1566 | 1561 | 0.4770 | 0.4680 | 0.0550 | 0.0090 | -0.0176 | 0.0266 | [0.0018, 0.0559] | hypothesis (block-sensitive (28d: inconclusive)) |
| low | 15m | 3218 | 0 | 0 | 0 | 1566 | 1326 | 0.4745 | 0.4814 | 0.0441 | -0.0068 | -0.0114 | 0.0046 | [-0.0270, 0.0443] | inconclusive |
| low | 1h | 3218 | 0 | 0 | 0 | 1566 | 915 | 0.4612 | 0.4646 | 0.0743 | -0.0034 | -0.0146 | 0.0112 | [-0.0511, 0.0785] | inconclusive |
| low | 4h | 3218 | 0 | 0 | 0 | 1566 | 462 | 0.4403 | 0.4018 | 0.1579 | 0.0385 | -0.0278 | 0.0664 | [0.0302, 0.0949] | hypothesis |
| low | 1d | 3217 | 0 | 1 | 0 | 1565 | 146 | 0.4187 | 0.4293 | 0.1520 | -0.0106 | -0.0273 | 0.0167 | [-0.0323, 0.0585] | insufficient |
| medium | 5m | 3496 | 0 | 0 | 0 | 1668 | 1662 | 0.4771 | 0.4748 | 0.0481 | 0.0023 | 0.0018 | 0.0005 | [-0.0283, 0.0344] | inconclusive |
| medium | 15m | 3496 | 0 | 0 | 0 | 1668 | 1424 | 0.4840 | 0.4651 | 0.0509 | 0.0189 | 0.0082 | 0.0106 | [-0.0217, 0.0515] | inconclusive |
| medium | 1h | 3496 | 0 | 0 | 0 | 1668 | 968 | 0.4763 | 0.4700 | 0.0538 | 0.0063 | 0.0212 | -0.0149 | [-0.0465, 0.0258] | inconclusive |
| medium | 4h | 3496 | 0 | 0 | 0 | 1668 | 514 | 0.4723 | 0.4751 | 0.0526 | -0.0029 | 0.0290 | -0.0319 | [-0.0680, 0.0159] | inconclusive |
| medium | 1d | 3468 | 0 | 28 | 0 | 1655 | 192 | 0.4622 | 0.4680 | 0.0698 | -0.0058 | 0.0017 | -0.0074 | [-0.0471, 0.0322] | insufficient |
| high | 5m | 2601 | 0 | 0 | 0 | 1214 | 1208 | 0.4733 | 0.4637 | 0.0631 | 0.0096 | 0.0004 | 0.0092 | [-0.0259, 0.0473] | inconclusive |
| high | 15m | 2601 | 0 | 0 | 0 | 1214 | 1027 | 0.4814 | 0.4660 | 0.0527 | 0.0154 | 0.0029 | 0.0125 | [-0.0340, 0.0722] | inconclusive |
| high | 1h | 2601 | 0 | 0 | 0 | 1214 | 702 | 0.4790 | 0.4814 | 0.0396 | -0.0023 | 0.0121 | -0.0144 | [-0.0627, 0.0458] | inconclusive |
| high | 4h | 2601 | 0 | 0 | 0 | 1214 | 391 | 0.5033 | 0.4406 | 0.0561 | 0.0627 | 0.0074 | 0.0553 | [-0.0074, 0.1234] | inconclusive |
| high | 1d | 2536 | 0 | 65 | 0 | 1185 | 172 | 0.5162 | 0.4097 | 0.0741 | 0.1065 | -0.0168 | 0.1233 | [0.0457, 0.1690] | insufficient |
| extreme | 5m | 2772 | 0 | 0 | 0 | 1303 | 1300 | 0.4686 | 0.4589 | 0.0725 | 0.0097 | 0.0019 | 0.0078 | [-0.0380, 0.0651] | inconclusive |
| extreme | 15m | 2772 | 0 | 0 | 0 | 1303 | 1109 | 0.4697 | 0.4506 | 0.0797 | 0.0191 | 0.0049 | 0.0142 | [-0.0243, 0.0680] | inconclusive |
| extreme | 1h | 2772 | 0 | 0 | 0 | 1303 | 725 | 0.4769 | 0.4589 | 0.0642 | 0.0180 | 0.0135 | 0.0046 | [-0.0305, 0.0715] | inconclusive |
| extreme | 4h | 2770 | 2 | 0 | 0 | 1303 | 369 | 0.4567 | 0.5000 | 0.0433 | -0.0433 | 0.0179 | -0.0613 | [-0.1083, 0.0322] | insufficient |
| extreme | 1d | 2728 | 5 | 39 | 0 | 1274 | 139 | 0.4685 | 0.4850 | 0.0466 | -0.0165 | 0.0574 | -0.0739 | [-0.1292, 0.0153] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 1035 | 0 | 0 | 0 | 621 | 619 | 0.4628 | 0.4744 | 0.0628 | -0.0116 | -0.0111 | -0.0005 | [-0.1014, 0.0725] | inconclusive |
| value_edge | 15m | 1035 | 0 | 0 | 0 | 621 | 592 | 0.4976 | 0.4570 | 0.0454 | 0.0406 | -0.0176 | 0.0581 | [-0.0284, 0.1234] | inconclusive |
| value_edge | 1h | 1035 | 0 | 0 | 0 | 621 | 515 | 0.4667 | 0.4667 | 0.0667 | 0.0000 | -0.0115 | 0.0115 | [-0.0738, 0.0973] | inconclusive |
| value_edge | 4h | 1034 | 0 | 1 | 0 | 620 | 394 | 0.4149 | 0.4681 | 0.1170 | -0.0532 | -0.0421 | -0.0111 | [-0.1079, 0.0725] | inconclusive |
| value_edge | 1d | 1012 | 0 | 23 | 0 | 607 | 194 | 0.3943 | 0.4891 | 0.1166 | -0.0949 | -0.0357 | -0.0592 | [-0.1658, 0.0223] | insufficient |
| structural_extreme | 5m | 222 | 0 | 0 | 0 | 135 | 135 | 0.4640 | 0.4414 | 0.0946 | 0.0225 | -0.0131 | 0.0357 | [-0.0850, 0.1587] | insufficient |
| structural_extreme | 15m | 222 | 0 | 0 | 0 | 135 | 132 | 0.4369 | 0.4324 | 0.1306 | 0.0045 | 0.0080 | -0.0035 | [-0.1286, 0.1138] | insufficient |
| structural_extreme | 1h | 222 | 0 | 0 | 0 | 135 | 125 | 0.4685 | 0.4324 | 0.0991 | 0.0360 | -0.0214 | 0.0574 | [-0.0635, 0.1505] | insufficient |
| structural_extreme | 4h | 222 | 0 | 0 | 0 | 135 | 113 | 0.3333 | 0.4279 | 0.2387 | -0.0946 | -0.0661 | -0.0285 | [-0.1239, 0.1097] | insufficient |
| structural_extreme | 1d | 217 | 0 | 5 | 0 | 133 | 82 | 0.3364 | 0.4562 | 0.2074 | -0.1198 | -0.0540 | -0.0659 | [-0.2235, 0.0910] | insufficient |
| neither | 5m | 11083 | 0 | 0 | 0 | 5335 | 5316 | 0.4746 | 0.4672 | 0.0582 | 0.0074 | -0.0030 | 0.0104 | [-0.0084, 0.0305] | inconclusive |
| neither | 15m | 11083 | 0 | 0 | 0 | 5335 | 4532 | 0.4747 | 0.4690 | 0.0563 | 0.0057 | 0.0032 | 0.0025 | [-0.0234, 0.0304] | inconclusive |
| neither | 1h | 11083 | 0 | 0 | 0 | 5335 | 3038 | 0.4733 | 0.4697 | 0.0569 | 0.0036 | 0.0150 | -0.0113 | [-0.0418, 0.0193] | inconclusive |
| neither | 4h | 11047 | 2 | 34 | 0 | 5318 | 1420 | 0.4732 | 0.4540 | 0.0729 | 0.0192 | 0.0200 | -0.0008 | [-0.0248, 0.0251] | inconclusive |
| neither | 1d | 10758 | 5 | 320 | 0 | 5165 | 321 | 0.4716 | 0.4457 | 0.0826 | 0.0259 | 0.0170 | 0.0089 | [-0.0183, 0.0385] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

##### by cvd

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| with | 5m | 6301 | 0 | 0 | 0 | 2950 | 2936 | 0.4794 | 0.4641 | 0.0565 | 0.0154 | -0.0041 | 0.0195 | [-0.0037, 0.0444] | inconclusive |
| with | 15m | 6301 | 0 | 0 | 0 | 2950 | 2507 | 0.4780 | 0.4620 | 0.0600 | 0.0160 | 0.0013 | 0.0148 | [-0.0282, 0.0627] | inconclusive |
| with | 1h | 6301 | 0 | 0 | 0 | 2950 | 1695 | 0.4725 | 0.4639 | 0.0636 | 0.0086 | 0.0128 | -0.0043 | [-0.0607, 0.0450] | inconclusive |
| with | 4h | 6276 | 0 | 25 | 0 | 2936 | 844 | 0.4603 | 0.4576 | 0.0821 | 0.0027 | 0.0150 | -0.0122 | [-0.0531, 0.0306] | inconclusive |
| with | 1d | 6160 | 2 | 139 | 0 | 2872 | 250 | 0.4734 | 0.4356 | 0.0911 | 0.0378 | 0.0192 | 0.0186 | [-0.0273, 0.0673] | insufficient |
| against | 5m | 6001 | 0 | 0 | 0 | 2925 | 2920 | 0.4684 | 0.4696 | 0.0620 | -0.0012 | -0.0028 | 0.0017 | [-0.0311, 0.0353] | inconclusive |
| against | 15m | 6001 | 0 | 0 | 0 | 2925 | 2512 | 0.4746 | 0.4723 | 0.0532 | 0.0023 | 0.0024 | -0.0000 | [-0.0352, 0.0370] | inconclusive |
| against | 1h | 6001 | 0 | 0 | 0 | 2925 | 1682 | 0.4724 | 0.4741 | 0.0535 | -0.0017 | 0.0119 | -0.0136 | [-0.0485, 0.0258] | inconclusive |
| against | 4h | 5989 | 2 | 10 | 0 | 2922 | 863 | 0.4730 | 0.4510 | 0.0760 | 0.0220 | 0.0129 | 0.0091 | [-0.0250, 0.0455] | inconclusive |
| against | 1d | 5789 | 3 | 209 | 0 | 2820 | 261 | 0.4528 | 0.4635 | 0.0838 | -0.0107 | 0.0051 | -0.0158 | [-0.0622, 0.0373] | insufficient |
| flat | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P1-buy w=15m

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 11179 | 0 | 0 | 0 | 2441 | 2441 | 0.4631 | 0.4630 | 0.0739 | 0.0001 | -0.0031 | 0.0032 | [-0.0213, 0.0310] | inconclusive |
| all | 15m | 11179 | 0 | 0 | 0 | 2441 | 2439 | 0.4839 | 0.4614 | 0.0547 | 0.0225 | 0.0033 | 0.0192 | [-0.0140, 0.0558] | inconclusive |
| all | 1h | 11179 | 0 | 0 | 0 | 2441 | 1908 | 0.4803 | 0.4534 | 0.0664 | 0.0269 | 0.0142 | 0.0127 | [-0.0398, 0.0610] | inconclusive |
| all | 4h | 11150 | 2 | 27 | 0 | 2433 | 1084 | 0.4771 | 0.4416 | 0.0813 | 0.0355 | 0.0165 | 0.0190 | [-0.0296, 0.0655] | inconclusive |
| all | 1d | 10904 | 2 | 273 | 0 | 2367 | 303 | 0.4651 | 0.4430 | 0.0920 | 0.0221 | 0.0097 | 0.0124 | [-0.0419, 0.0708] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 2797 | 0 | 0 | 0 | 679 | 679 | 0.4415 | 0.4802 | 0.0783 | -0.0386 | -0.0157 | -0.0229 | [-0.0715, 0.0216] | inconclusive |
| low | 15m | 2797 | 0 | 0 | 0 | 679 | 679 | 0.4730 | 0.4787 | 0.0483 | -0.0057 | -0.0073 | 0.0016 | [-0.0971, 0.0996] | inconclusive |
| low | 1h | 2797 | 0 | 0 | 0 | 679 | 548 | 0.4694 | 0.4287 | 0.1019 | 0.0408 | 0.0031 | 0.0377 | [-0.0712, 0.1580] | inconclusive |
| low | 4h | 2797 | 0 | 0 | 0 | 679 | 340 | 0.4151 | 0.3901 | 0.1949 | 0.0250 | -0.0115 | 0.0366 | [-0.0363, 0.1041] | insufficient |
| low | 1d | 2796 | 0 | 1 | 0 | 678 | 129 | 0.3902 | 0.4460 | 0.1638 | -0.0558 | -0.0299 | -0.0259 | [-0.0779, 0.0247] | insufficient |
| medium | 5m | 3135 | 0 | 0 | 0 | 683 | 683 | 0.4807 | 0.4648 | 0.0545 | 0.0159 | 0.0050 | 0.0109 | [-0.0250, 0.0506] | inconclusive |
| medium | 15m | 3135 | 0 | 0 | 0 | 683 | 683 | 0.4903 | 0.4759 | 0.0338 | 0.0144 | 0.0132 | 0.0011 | [-0.0309, 0.0383] | inconclusive |
| medium | 1h | 3135 | 0 | 0 | 0 | 683 | 554 | 0.4868 | 0.4715 | 0.0418 | 0.0153 | 0.0193 | -0.0040 | [-0.0575, 0.0558] | inconclusive |
| medium | 4h | 3135 | 0 | 0 | 0 | 683 | 354 | 0.5081 | 0.4478 | 0.0440 | 0.0603 | 0.0392 | 0.0211 | [-0.0529, 0.0894] | insufficient |
| medium | 1d | 3103 | 0 | 32 | 0 | 679 | 171 | 0.4631 | 0.4683 | 0.0686 | -0.0052 | -0.0038 | -0.0014 | [-0.1027, 0.0796] | insufficient |
| high | 5m | 2447 | 0 | 0 | 0 | 520 | 520 | 0.4708 | 0.4553 | 0.0740 | 0.0155 | 0.0007 | 0.0148 | [-0.0369, 0.0739] | inconclusive |
| high | 15m | 2447 | 0 | 0 | 0 | 520 | 520 | 0.5031 | 0.4401 | 0.0568 | 0.0629 | -0.0017 | 0.0646 | [0.0013, 0.1417] | hypothesis |
| high | 1h | 2447 | 0 | 0 | 0 | 520 | 410 | 0.5014 | 0.4540 | 0.0445 | 0.0474 | 0.0031 | 0.0443 | [-0.0271, 0.1351] | inconclusive |
| high | 4h | 2447 | 0 | 0 | 0 | 520 | 290 | 0.5525 | 0.4074 | 0.0400 | 0.1451 | 0.0189 | 0.1262 | [0.0480, 0.1898] | insufficient |
| high | 1d | 2412 | 0 | 35 | 0 | 507 | 145 | 0.5498 | 0.3694 | 0.0808 | 0.1803 | -0.0087 | 0.1890 | [0.0776, 0.2602] | insufficient |
| extreme | 5m | 2633 | 0 | 0 | 0 | 561 | 561 | 0.4592 | 0.4501 | 0.0908 | 0.0091 | 0.0071 | 0.0020 | [-0.0338, 0.0518] | inconclusive |
| extreme | 15m | 2633 | 0 | 0 | 0 | 561 | 559 | 0.4706 | 0.4520 | 0.0775 | 0.0186 | 0.0094 | 0.0092 | [-0.0291, 0.0748] | inconclusive |
| extreme | 1h | 2633 | 0 | 0 | 0 | 561 | 439 | 0.4596 | 0.4649 | 0.0756 | -0.0053 | 0.0210 | -0.0263 | [-0.1054, 0.0904] | inconclusive |
| extreme | 4h | 2631 | 2 | 0 | 0 | 560 | 267 | 0.4379 | 0.5211 | 0.0410 | -0.0832 | 0.0297 | -0.1129 | [-0.1860, 0.0226] | insufficient |
| extreme | 1d | 2593 | 2 | 38 | 0 | 546 | 122 | 0.4693 | 0.4778 | 0.0528 | -0.0085 | 0.0759 | -0.0844 | [-0.1563, 0.0260] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 918 | 0 | 0 | 0 | 314 | 314 | 0.4575 | 0.4553 | 0.0871 | 0.0022 | -0.0112 | 0.0134 | [-0.0826, 0.1075] | insufficient |
| value_edge | 15m | 918 | 0 | 0 | 0 | 314 | 314 | 0.4597 | 0.4706 | 0.0697 | -0.0109 | -0.0153 | 0.0044 | [-0.0971, 0.1239] | insufficient |
| value_edge | 1h | 918 | 0 | 0 | 0 | 314 | 298 | 0.4913 | 0.4227 | 0.0861 | 0.0686 | -0.0080 | 0.0767 | [-0.0544, 0.2033] | insufficient |
| value_edge | 4h | 916 | 0 | 2 | 0 | 313 | 254 | 0.4651 | 0.3897 | 0.1452 | 0.0753 | -0.0210 | 0.0963 | [-0.0170, 0.2040] | insufficient |
| value_edge | 1d | 901 | 0 | 17 | 0 | 309 | 155 | 0.4373 | 0.4273 | 0.1354 | 0.0100 | -0.0460 | 0.0559 | [-0.0663, 0.1562] | insufficient |
| structural_extreme | 5m | 94 | 0 | 0 | 0 | 47 | 47 | 0.4149 | 0.4468 | 0.1383 | -0.0319 | -0.0063 | -0.0256 | [-0.3177, 0.2272] | insufficient |
| structural_extreme | 15m | 94 | 0 | 0 | 0 | 47 | 47 | 0.3936 | 0.4362 | 0.1702 | -0.0426 | 0.0637 | -0.1063 | [-0.2758, 0.1120] | insufficient |
| structural_extreme | 1h | 94 | 0 | 0 | 0 | 47 | 47 | 0.5638 | 0.3830 | 0.0532 | 0.1809 | 0.1485 | 0.0323 | [-0.1535, 0.1577] | insufficient |
| structural_extreme | 4h | 94 | 0 | 0 | 0 | 47 | 46 | 0.2872 | 0.4574 | 0.2553 | -0.1702 | -0.0601 | -0.1101 | [-0.3512, 0.1562] | insufficient |
| structural_extreme | 1d | 81 | 0 | 13 | 0 | 46 | 38 | 0.3457 | 0.5432 | 0.1111 | -0.1975 | -0.1533 | -0.0442 | [-0.2810, 0.2074] | insufficient |
| neither | 5m | 10191 | 0 | 0 | 0 | 2311 | 2311 | 0.4633 | 0.4644 | 0.0723 | -0.0012 | -0.0027 | 0.0016 | [-0.0239, 0.0278] | inconclusive |
| neither | 15m | 10191 | 0 | 0 | 0 | 2311 | 2309 | 0.4868 | 0.4609 | 0.0523 | 0.0259 | 0.0045 | 0.0214 | [-0.0107, 0.0571] | inconclusive |
| neither | 1h | 10191 | 0 | 0 | 0 | 2311 | 1813 | 0.4787 | 0.4565 | 0.0649 | 0.0222 | 0.0152 | 0.0069 | [-0.0402, 0.0525] | inconclusive |
| neither | 4h | 10164 | 2 | 25 | 0 | 2303 | 1051 | 0.4794 | 0.4457 | 0.0749 | 0.0337 | 0.0205 | 0.0133 | [-0.0356, 0.0624] | inconclusive |
| neither | 1d | 9946 | 2 | 243 | 0 | 2240 | 298 | 0.4677 | 0.4442 | 0.0881 | 0.0235 | 0.0145 | 0.0091 | [-0.0461, 0.0698] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

##### by cvd

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| with | 5m | 6390 | 0 | 0 | 0 | 1372 | 1372 | 0.4692 | 0.4543 | 0.0765 | 0.0149 | -0.0037 | 0.0185 | [-0.0155, 0.0585] | inconclusive |
| with | 15m | 6390 | 0 | 0 | 0 | 1372 | 1372 | 0.4883 | 0.4588 | 0.0529 | 0.0294 | 0.0035 | 0.0259 | [-0.0198, 0.0746] | inconclusive |
| with | 1h | 6390 | 0 | 0 | 0 | 1372 | 1090 | 0.4809 | 0.4490 | 0.0701 | 0.0319 | 0.0147 | 0.0173 | [-0.0560, 0.0798] | inconclusive |
| with | 4h | 6363 | 1 | 26 | 0 | 1365 | 646 | 0.4754 | 0.4432 | 0.0814 | 0.0322 | 0.0173 | 0.0149 | [-0.0655, 0.0832] | inconclusive |
| with | 1d | 6260 | 1 | 129 | 0 | 1339 | 228 | 0.4749 | 0.4350 | 0.0901 | 0.0399 | 0.0163 | 0.0236 | [-0.0542, 0.1041] | insufficient |
| against | 5m | 4789 | 0 | 0 | 0 | 1118 | 1118 | 0.4550 | 0.4746 | 0.0704 | -0.0196 | -0.0023 | -0.0174 | [-0.0497, 0.0105] | inconclusive |
| against | 15m | 4789 | 0 | 0 | 0 | 1118 | 1116 | 0.4780 | 0.4648 | 0.0572 | 0.0132 | 0.0030 | 0.0102 | [-0.0247, 0.0449] | inconclusive |
| against | 1h | 4789 | 0 | 0 | 0 | 1118 | 893 | 0.4794 | 0.4592 | 0.0614 | 0.0203 | 0.0136 | 0.0066 | [-0.0558, 0.0658] | inconclusive |
| against | 4h | 4787 | 1 | 1 | 0 | 1116 | 563 | 0.4794 | 0.4395 | 0.0811 | 0.0399 | 0.0155 | 0.0244 | [-0.0377, 0.0929] | inconclusive |
| against | 1d | 4644 | 1 | 144 | 0 | 1075 | 220 | 0.4518 | 0.4537 | 0.0945 | -0.0019 | 0.0007 | -0.0027 | [-0.0664, 0.0714] | insufficient |
| flat | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P1-buy w=1h

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 9702 | 0 | 0 | 0 | 720 | 720 | 0.4599 | 0.4538 | 0.0863 | 0.0061 | -0.0029 | 0.0090 | [-0.0270, 0.0379] | inconclusive |
| all | 15m | 9702 | 0 | 0 | 0 | 720 | 720 | 0.4821 | 0.4533 | 0.0646 | 0.0288 | 0.0015 | 0.0273 | [-0.0156, 0.0623] | inconclusive |
| all | 1h | 9702 | 0 | 0 | 0 | 720 | 720 | 0.5058 | 0.4186 | 0.0757 | 0.0872 | 0.0154 | 0.0718 | [0.0105, 0.1203] | hypothesis |
| all | 4h | 9653 | 0 | 49 | 0 | 716 | 547 | 0.5014 | 0.4211 | 0.0775 | 0.0803 | 0.0164 | 0.0638 | [-0.0110, 0.1247] | inconclusive |
| all | 1d | 9437 | 0 | 265 | 0 | 696 | 238 | 0.4575 | 0.4617 | 0.0809 | -0.0042 | 0.0068 | -0.0111 | [-0.0700, 0.0540] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 2213 | 0 | 0 | 0 | 194 | 194 | 0.4478 | 0.4591 | 0.0931 | -0.0113 | -0.0115 | 0.0002 | [-0.0358, 0.0444] | insufficient |
| low | 15m | 2213 | 0 | 0 | 0 | 194 | 194 | 0.4776 | 0.4754 | 0.0470 | 0.0023 | -0.0027 | 0.0050 | [-0.0550, 0.0616] | insufficient |
| low | 1h | 2213 | 0 | 0 | 0 | 194 | 194 | 0.4790 | 0.4171 | 0.1039 | 0.0619 | 0.0121 | 0.0498 | [-0.0617, 0.1616] | insufficient |
| low | 4h | 2213 | 0 | 0 | 0 | 194 | 157 | 0.4302 | 0.4067 | 0.1631 | 0.0235 | 0.0009 | 0.0226 | [-0.1056, 0.1373] | insufficient |
| low | 1d | 2213 | 0 | 0 | 0 | 194 | 89 | 0.3904 | 0.4528 | 0.1568 | -0.0624 | -0.0108 | -0.0515 | [-0.2186, 0.0796] | insufficient |
| medium | 5m | 2373 | 0 | 0 | 0 | 191 | 191 | 0.4775 | 0.4501 | 0.0725 | 0.0274 | 0.0050 | 0.0224 | [-0.0498, 0.0806] | insufficient |
| medium | 15m | 2373 | 0 | 0 | 0 | 191 | 191 | 0.5057 | 0.4526 | 0.0417 | 0.0531 | 0.0167 | 0.0364 | [-0.0574, 0.1145] | insufficient |
| medium | 1h | 2373 | 0 | 0 | 0 | 191 | 191 | 0.5065 | 0.4488 | 0.0447 | 0.0577 | 0.0329 | 0.0248 | [-0.0919, 0.1240] | insufficient |
| medium | 4h | 2373 | 0 | 0 | 0 | 191 | 160 | 0.5082 | 0.4475 | 0.0442 | 0.0607 | 0.0355 | 0.0252 | [-0.1274, 0.1411] | insufficient |
| medium | 1d | 2348 | 0 | 25 | 0 | 189 | 102 | 0.3901 | 0.5400 | 0.0698 | -0.1499 | -0.0227 | -0.1272 | [-0.3261, 0.0493] | insufficient |
| high | 5m | 2497 | 0 | 0 | 0 | 185 | 185 | 0.4561 | 0.4573 | 0.0865 | -0.0012 | 0.0033 | -0.0045 | [-0.0646, 0.0507] | insufficient |
| high | 15m | 2497 | 0 | 0 | 0 | 185 | 185 | 0.4746 | 0.4489 | 0.0765 | 0.0256 | -0.0018 | 0.0274 | [-0.0352, 0.0870] | insufficient |
| high | 1h | 2497 | 0 | 0 | 0 | 185 | 185 | 0.5439 | 0.3917 | 0.0645 | 0.1522 | 0.0259 | 0.1263 | [0.0342, 0.1889] | insufficient |
| high | 4h | 2497 | 0 | 0 | 0 | 185 | 156 | 0.5971 | 0.3444 | 0.0585 | 0.2527 | 0.0079 | 0.2448 | [0.1314, 0.3130] | insufficient |
| high | 1d | 2423 | 0 | 74 | 0 | 181 | 99 | 0.5766 | 0.3525 | 0.0710 | 0.2241 | -0.0426 | 0.2667 | [0.1186, 0.3384] | insufficient |
| extreme | 5m | 2468 | 0 | 0 | 0 | 172 | 172 | 0.4571 | 0.4485 | 0.0944 | 0.0085 | 0.0056 | 0.0029 | [-0.0524, 0.0546] | insufficient |
| extreme | 15m | 2468 | 0 | 0 | 0 | 172 | 172 | 0.4733 | 0.4356 | 0.0912 | 0.0377 | 0.0049 | 0.0328 | [-0.0486, 0.0963] | insufficient |
| extreme | 1h | 2468 | 0 | 0 | 0 | 172 | 172 | 0.4874 | 0.4173 | 0.0952 | 0.0701 | 0.0126 | 0.0575 | [-0.0503, 0.1574] | insufficient |
| extreme | 4h | 2468 | 0 | 0 | 0 | 172 | 138 | 0.4542 | 0.4976 | 0.0482 | -0.0434 | 0.0180 | -0.0614 | [-0.1875, 0.1051] | insufficient |
| extreme | 1d | 2453 | 0 | 15 | 0 | 170 | 84 | 0.4647 | 0.5026 | 0.0326 | -0.0379 | 0.0699 | -0.1078 | [-0.2300, 0.0899] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 856 | 0 | 0 | 0 | 133 | 133 | 0.5023 | 0.4100 | 0.0876 | 0.0923 | -0.0093 | 0.1016 | [0.0006, 0.1814] | insufficient |
| value_edge | 15m | 856 | 0 | 0 | 0 | 133 | 133 | 0.4895 | 0.4404 | 0.0701 | 0.0491 | -0.0232 | 0.0722 | [-0.0400, 0.1711] | insufficient |
| value_edge | 1h | 856 | 0 | 0 | 0 | 133 | 133 | 0.5491 | 0.3773 | 0.0736 | 0.1717 | 0.0069 | 0.1649 | [0.0229, 0.2740] | insufficient |
| value_edge | 4h | 856 | 0 | 0 | 0 | 133 | 125 | 0.5257 | 0.3680 | 0.1063 | 0.1577 | 0.0049 | 0.1528 | [0.0113, 0.2412] | insufficient |
| value_edge | 1d | 844 | 0 | 12 | 0 | 132 | 93 | 0.4301 | 0.4633 | 0.1066 | -0.0332 | -0.0704 | 0.0372 | [-0.1333, 0.1625] | insufficient |
| structural_extreme | 5m | 93 | 0 | 0 | 0 | 46 | 46 | 0.4731 | 0.4839 | 0.0430 | -0.0108 | 0.0358 | -0.0466 | [-0.3804, 0.1578] | insufficient |
| structural_extreme | 15m | 93 | 0 | 0 | 0 | 46 | 46 | 0.5484 | 0.4301 | 0.0215 | 0.1183 | 0.0618 | 0.0565 | [-0.2045, 0.2409] | insufficient |
| structural_extreme | 1h | 93 | 0 | 0 | 0 | 46 | 46 | 0.6344 | 0.3656 | 0.0000 | 0.2688 | 0.0535 | 0.2153 | [-0.1259, 0.4612] | insufficient |
| structural_extreme | 4h | 93 | 0 | 0 | 0 | 46 | 45 | 0.5914 | 0.3011 | 0.1075 | 0.2903 | 0.0426 | 0.2477 | [-0.1480, 0.4916] | insufficient |
| structural_extreme | 1d | 92 | 0 | 1 | 0 | 45 | 39 | 0.4674 | 0.4674 | 0.0652 | 0.0000 | -0.0219 | 0.0219 | [-0.3460, 0.2322] | insufficient |
| neither | 5m | 8764 | 0 | 0 | 0 | 692 | 692 | 0.4555 | 0.4577 | 0.0868 | -0.0022 | -0.0027 | 0.0005 | [-0.0329, 0.0301] | inconclusive |
| neither | 15m | 8764 | 0 | 0 | 0 | 692 | 692 | 0.4806 | 0.4548 | 0.0646 | 0.0258 | 0.0034 | 0.0224 | [-0.0179, 0.0583] | inconclusive |
| neither | 1h | 8764 | 0 | 0 | 0 | 692 | 692 | 0.5003 | 0.4231 | 0.0766 | 0.0772 | 0.0159 | 0.0613 | [0.0001, 0.1145] | hypothesis (block-sensitive (28d: inconclusive)) |
| neither | 4h | 8715 | 0 | 49 | 0 | 688 | 525 | 0.4982 | 0.4273 | 0.0745 | 0.0709 | 0.0174 | 0.0535 | [-0.0199, 0.1147] | inconclusive |
| neither | 1d | 8512 | 0 | 252 | 0 | 668 | 232 | 0.4602 | 0.4613 | 0.0785 | -0.0012 | 0.0147 | -0.0159 | [-0.0757, 0.0542] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

##### by cvd

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| with | 5m | 6402 | 0 | 0 | 0 | 451 | 451 | 0.4552 | 0.4599 | 0.0850 | -0.0047 | -0.0033 | -0.0014 | [-0.0461, 0.0300] | inconclusive |
| with | 15m | 6402 | 0 | 0 | 0 | 451 | 451 | 0.4819 | 0.4569 | 0.0612 | 0.0250 | 0.0014 | 0.0236 | [-0.0354, 0.0710] | inconclusive |
| with | 1h | 6402 | 0 | 0 | 0 | 451 | 451 | 0.4930 | 0.4214 | 0.0856 | 0.0715 | 0.0165 | 0.0550 | [-0.0455, 0.1293] | inconclusive |
| with | 4h | 6353 | 0 | 49 | 0 | 447 | 344 | 0.4998 | 0.4127 | 0.0875 | 0.0870 | 0.0206 | 0.0665 | [-0.0575, 0.1609] | insufficient |
| with | 1d | 6275 | 0 | 127 | 0 | 437 | 169 | 0.4716 | 0.4421 | 0.0864 | 0.0295 | 0.0129 | 0.0166 | [-0.0669, 0.1030] | insufficient |
| against | 5m | 3300 | 0 | 0 | 0 | 297 | 297 | 0.4691 | 0.4421 | 0.0888 | 0.0270 | -0.0023 | 0.0292 | [-0.0102, 0.0731] | insufficient |
| against | 15m | 3300 | 0 | 0 | 0 | 297 | 297 | 0.4824 | 0.4464 | 0.0712 | 0.0361 | 0.0016 | 0.0345 | [-0.0135, 0.0855] | insufficient |
| against | 1h | 3300 | 0 | 0 | 0 | 297 | 297 | 0.5306 | 0.4130 | 0.0564 | 0.1176 | 0.0134 | 0.1042 | [0.0306, 0.1922] | insufficient |
| against | 4h | 3300 | 0 | 0 | 0 | 297 | 241 | 0.5045 | 0.4373 | 0.0582 | 0.0673 | 0.0085 | 0.0588 | [-0.0418, 0.1772] | insufficient |
| against | 1d | 3162 | 0 | 138 | 0 | 285 | 143 | 0.4295 | 0.5006 | 0.0699 | -0.0712 | -0.0052 | -0.0660 | [-0.1696, 0.0798] | insufficient |
| flat | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P1-sell w=5m

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 13187 | 0 | 0 | 0 | 6266 | 6241 | 0.4706 | 0.4670 | 0.0624 | 0.0036 | 0.0022 | 0.0015 | [-0.0189, 0.0220] | inconclusive |
| all | 15m | 13187 | 0 | 0 | 0 | 6266 | 5247 | 0.4697 | 0.4768 | 0.0535 | -0.0071 | -0.0037 | -0.0033 | [-0.0288, 0.0228] | inconclusive |
| all | 1h | 13187 | 0 | 0 | 0 | 6266 | 3313 | 0.4581 | 0.4878 | 0.0541 | -0.0297 | -0.0129 | -0.0168 | [-0.0422, 0.0126] | inconclusive |
| all | 4h | 13147 | 1 | 39 | 0 | 6242 | 1490 | 0.4663 | 0.4615 | 0.0722 | 0.0049 | -0.0137 | 0.0185 | [0.0017, 0.0345] | hypothesis |
| all | 1d | 12611 | 11 | 565 | 0 | 6006 | 327 | 0.4634 | 0.4629 | 0.0737 | 0.0005 | -0.0114 | 0.0118 | [-0.0217, 0.0540] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 3726 | 0 | 0 | 0 | 1741 | 1732 | 0.4632 | 0.4691 | 0.0676 | -0.0059 | 0.0143 | -0.0202 | [-0.0561, 0.0153] | inconclusive |
| low | 15m | 3726 | 0 | 0 | 0 | 1741 | 1450 | 0.4689 | 0.4777 | 0.0534 | -0.0089 | 0.0082 | -0.0171 | [-0.0496, 0.0237] | inconclusive |
| low | 1h | 3726 | 0 | 0 | 0 | 1741 | 963 | 0.4563 | 0.4729 | 0.0709 | -0.0166 | 0.0123 | -0.0289 | [-0.0612, 0.0153] | inconclusive |
| low | 4h | 3726 | 0 | 0 | 0 | 1741 | 474 | 0.4259 | 0.4222 | 0.1519 | 0.0038 | 0.0210 | -0.0172 | [-0.0553, 0.0192] | inconclusive |
| low | 1d | 3726 | 0 | 0 | 0 | 1741 | 148 | 0.4536 | 0.4020 | 0.1444 | 0.0515 | 0.0354 | 0.0161 | [-0.0430, 0.0791] | insufficient |
| medium | 5m | 3937 | 0 | 0 | 0 | 1886 | 1879 | 0.4707 | 0.4727 | 0.0566 | -0.0020 | -0.0053 | 0.0033 | [-0.0298, 0.0413] | inconclusive |
| medium | 15m | 3937 | 0 | 0 | 0 | 1886 | 1579 | 0.4740 | 0.4790 | 0.0470 | -0.0051 | -0.0189 | 0.0138 | [-0.0293, 0.0596] | inconclusive |
| medium | 1h | 3937 | 0 | 0 | 0 | 1886 | 1016 | 0.4669 | 0.4879 | 0.0452 | -0.0211 | -0.0314 | 0.0103 | [-0.0232, 0.0469] | inconclusive |
| medium | 4h | 3937 | 0 | 0 | 0 | 1886 | 527 | 0.4780 | 0.4765 | 0.0455 | 0.0015 | -0.0293 | 0.0308 | [-0.0126, 0.0724] | inconclusive |
| medium | 1d | 3898 | 0 | 39 | 0 | 1867 | 193 | 0.4584 | 0.4897 | 0.0518 | -0.0313 | -0.0201 | -0.0112 | [-0.0578, 0.0303] | insufficient |
| high | 5m | 2417 | 0 | 0 | 0 | 1161 | 1156 | 0.4746 | 0.4688 | 0.0567 | 0.0058 | -0.0080 | 0.0138 | [-0.0119, 0.0409] | inconclusive |
| high | 15m | 2417 | 0 | 0 | 0 | 1161 | 1000 | 0.4659 | 0.4808 | 0.0534 | -0.0149 | -0.0127 | -0.0022 | [-0.0333, 0.0311] | inconclusive |
| high | 1h | 2417 | 0 | 0 | 0 | 1161 | 659 | 0.4357 | 0.5126 | 0.0517 | -0.0770 | -0.0257 | -0.0512 | [-0.0843, 0.0009] | inconclusive |
| high | 4h | 2416 | 1 | 0 | 0 | 1160 | 386 | 0.4876 | 0.4677 | 0.0447 | 0.0199 | -0.0220 | 0.0419 | [-0.0114, 0.0893] | inconclusive |
| high | 1d | 2333 | 1 | 83 | 0 | 1122 | 166 | 0.4929 | 0.4719 | 0.0351 | 0.0210 | 0.0065 | 0.0145 | [-0.0399, 0.0726] | insufficient |
| extreme | 5m | 2812 | 0 | 0 | 0 | 1369 | 1366 | 0.4794 | 0.4506 | 0.0701 | 0.0288 | -0.0084 | 0.0372 | [-0.0034, 0.0810] | inconclusive |
| extreme | 15m | 2812 | 0 | 0 | 0 | 1369 | 1147 | 0.4712 | 0.4694 | 0.0594 | 0.0018 | -0.0210 | 0.0227 | [-0.0275, 0.0799] | inconclusive |
| extreme | 1h | 2812 | 0 | 0 | 0 | 1369 | 742 | 0.4680 | 0.4861 | 0.0459 | -0.0181 | -0.0389 | 0.0208 | [-0.0619, 0.1209] | inconclusive |
| extreme | 4h | 2812 | 0 | 0 | 0 | 1369 | 371 | 0.4847 | 0.4833 | 0.0320 | 0.0014 | -0.0384 | 0.0398 | [-0.0272, 0.1095] | insufficient |
| extreme | 1d | 2654 | 10 | 148 | 0 | 1305 | 135 | 0.4586 | 0.5011 | 0.0403 | -0.0426 | -0.0646 | 0.0220 | [-0.0294, 0.0669] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 1351 | 0 | 0 | 0 | 763 | 760 | 0.4560 | 0.4722 | 0.0718 | -0.0163 | 0.0040 | -0.0203 | [-0.0925, 0.0713] | inconclusive |
| value_edge | 15m | 1351 | 0 | 0 | 0 | 763 | 695 | 0.4611 | 0.4833 | 0.0555 | -0.0222 | 0.0131 | -0.0353 | [-0.1112, 0.0512] | inconclusive |
| value_edge | 1h | 1351 | 0 | 0 | 0 | 763 | 587 | 0.4545 | 0.4715 | 0.0740 | -0.0170 | 0.0208 | -0.0378 | [-0.1071, 0.0373] | inconclusive |
| value_edge | 4h | 1347 | 0 | 4 | 0 | 761 | 436 | 0.4380 | 0.4402 | 0.1218 | -0.0022 | 0.0329 | -0.0351 | [-0.0932, 0.0200] | inconclusive |
| value_edge | 1d | 1327 | 4 | 20 | 0 | 750 | 203 | 0.4529 | 0.4537 | 0.0934 | -0.0008 | 0.0244 | -0.0252 | [-0.0968, 0.0395] | insufficient |
| structural_extreme | 5m | 286 | 0 | 0 | 0 | 168 | 168 | 0.4476 | 0.4126 | 0.1399 | 0.0350 | 0.0079 | 0.0271 | [-0.1312, 0.2045] | insufficient |
| structural_extreme | 15m | 286 | 0 | 0 | 0 | 168 | 161 | 0.4860 | 0.4161 | 0.0979 | 0.0699 | 0.0030 | 0.0670 | [-0.0872, 0.2031] | insufficient |
| structural_extreme | 1h | 286 | 0 | 0 | 0 | 168 | 153 | 0.4301 | 0.4441 | 0.1259 | -0.0140 | 0.0629 | -0.0769 | [-0.1774, 0.0476] | insufficient |
| structural_extreme | 4h | 283 | 0 | 3 | 0 | 166 | 136 | 0.4346 | 0.3604 | 0.2049 | 0.0742 | 0.0547 | 0.0195 | [-0.1146, 0.1080] | insufficient |
| structural_extreme | 1d | 277 | 0 | 9 | 0 | 162 | 89 | 0.3935 | 0.3357 | 0.2708 | 0.0578 | 0.0345 | 0.0233 | [-0.0531, 0.0914] | insufficient |
| neither | 5m | 11611 | 0 | 0 | 0 | 5665 | 5642 | 0.4733 | 0.4665 | 0.0602 | 0.0069 | 0.0021 | 0.0048 | [-0.0112, 0.0210] | inconclusive |
| neither | 15m | 11611 | 0 | 0 | 0 | 5665 | 4799 | 0.4718 | 0.4760 | 0.0522 | -0.0042 | -0.0053 | 0.0011 | [-0.0245, 0.0293] | inconclusive |
| neither | 1h | 11611 | 0 | 0 | 0 | 5665 | 3083 | 0.4600 | 0.4894 | 0.0506 | -0.0294 | -0.0172 | -0.0122 | [-0.0380, 0.0169] | inconclusive |
| neither | 4h | 11578 | 1 | 32 | 0 | 5644 | 1436 | 0.4705 | 0.4651 | 0.0644 | 0.0054 | -0.0198 | 0.0252 | [0.0050, 0.0450] | hypothesis |
| neither | 1d | 11068 | 7 | 536 | 0 | 5417 | 322 | 0.4666 | 0.4655 | 0.0679 | 0.0011 | -0.0155 | 0.0166 | [-0.0231, 0.0614] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

##### by cvd

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| with | 5m | 8113 | 0 | 0 | 0 | 3833 | 3814 | 0.4784 | 0.4620 | 0.0597 | 0.0164 | 0.0015 | 0.0149 | [-0.0136, 0.0438] | inconclusive |
| with | 15m | 8113 | 0 | 0 | 0 | 3833 | 3225 | 0.4776 | 0.4716 | 0.0508 | 0.0060 | -0.0050 | 0.0111 | [-0.0200, 0.0418] | inconclusive |
| with | 1h | 8113 | 0 | 0 | 0 | 3833 | 2058 | 0.4556 | 0.4914 | 0.0530 | -0.0359 | -0.0154 | -0.0205 | [-0.0593, 0.0187] | inconclusive |
| with | 4h | 8100 | 0 | 13 | 0 | 3825 | 1009 | 0.4656 | 0.4549 | 0.0795 | 0.0106 | -0.0137 | 0.0243 | [-0.0009, 0.0504] | inconclusive |
| with | 1d | 7710 | 4 | 399 | 0 | 3650 | 270 | 0.4545 | 0.4690 | 0.0765 | -0.0145 | -0.0098 | -0.0047 | [-0.0532, 0.0435] | insufficient |
| against | 5m | 5074 | 0 | 0 | 0 | 2481 | 2475 | 0.4582 | 0.4750 | 0.0668 | -0.0168 | 0.0032 | -0.0199 | [-0.0518, 0.0173] | inconclusive |
| against | 15m | 5074 | 0 | 0 | 0 | 2481 | 2114 | 0.4570 | 0.4850 | 0.0579 | -0.0280 | -0.0016 | -0.0263 | [-0.0606, 0.0072] | inconclusive |
| against | 1h | 5074 | 0 | 0 | 0 | 2481 | 1427 | 0.4622 | 0.4819 | 0.0560 | -0.0197 | -0.0089 | -0.0109 | [-0.0522, 0.0279] | inconclusive |
| against | 4h | 5047 | 1 | 26 | 0 | 2465 | 726 | 0.4676 | 0.4720 | 0.0604 | -0.0044 | -0.0137 | 0.0093 | [-0.0440, 0.0564] | inconclusive |
| against | 1d | 4901 | 7 | 166 | 0 | 2403 | 231 | 0.4775 | 0.4534 | 0.0692 | 0.0241 | -0.0138 | 0.0379 | [-0.0183, 0.0975] | insufficient |
| flat | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P1-sell w=15m

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 11953 | 0 | 0 | 0 | 2613 | 2613 | 0.4677 | 0.4530 | 0.0793 | 0.0146 | 0.0016 | 0.0130 | [-0.0123, 0.0395] | inconclusive |
| all | 15m | 11953 | 0 | 0 | 0 | 2613 | 2611 | 0.4805 | 0.4625 | 0.0571 | 0.0180 | -0.0053 | 0.0233 | [-0.0100, 0.0641] | inconclusive |
| all | 1h | 11953 | 0 | 0 | 0 | 2613 | 2008 | 0.4780 | 0.4677 | 0.0543 | 0.0104 | -0.0152 | 0.0255 | [-0.0178, 0.0726] | inconclusive |
| all | 4h | 11931 | 0 | 22 | 0 | 2604 | 1119 | 0.4811 | 0.4434 | 0.0755 | 0.0377 | -0.0128 | 0.0505 | [0.0067, 0.0910] | hypothesis |
| all | 1d | 11350 | 5 | 598 | 0 | 2495 | 300 | 0.4701 | 0.4608 | 0.0691 | 0.0093 | -0.0104 | 0.0197 | [-0.0362, 0.0810] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 3399 | 0 | 0 | 0 | 732 | 732 | 0.4801 | 0.4345 | 0.0853 | 0.0456 | 0.0083 | 0.0373 | [-0.0012, 0.0712] | inconclusive |
| low | 15m | 3399 | 0 | 0 | 0 | 732 | 731 | 0.5031 | 0.4322 | 0.0647 | 0.0709 | 0.0028 | 0.0681 | [0.0055, 0.1414] | hypothesis |
| low | 1h | 3399 | 0 | 0 | 0 | 732 | 579 | 0.4984 | 0.4166 | 0.0850 | 0.0818 | 0.0079 | 0.0739 | [0.0066, 0.1458] | hypothesis |
| low | 4h | 3399 | 0 | 0 | 0 | 732 | 352 | 0.4584 | 0.3831 | 0.1586 | 0.0753 | 0.0218 | 0.0535 | [-0.0211, 0.1303] | insufficient |
| low | 1d | 3399 | 0 | 0 | 0 | 732 | 132 | 0.4492 | 0.4054 | 0.1453 | 0.0438 | 0.0302 | 0.0137 | [-0.0703, 0.1073] | insufficient |
| medium | 5m | 3551 | 0 | 0 | 0 | 783 | 783 | 0.4503 | 0.4801 | 0.0696 | -0.0299 | -0.0008 | -0.0291 | [-0.0718, 0.0232] | inconclusive |
| medium | 15m | 3551 | 0 | 0 | 0 | 783 | 783 | 0.4582 | 0.4923 | 0.0496 | -0.0341 | -0.0142 | -0.0198 | [-0.0714, 0.0369] | inconclusive |
| medium | 1h | 3551 | 0 | 0 | 0 | 783 | 611 | 0.4624 | 0.4970 | 0.0406 | -0.0346 | -0.0269 | -0.0077 | [-0.0786, 0.0612] | inconclusive |
| medium | 4h | 3551 | 0 | 0 | 0 | 783 | 369 | 0.4832 | 0.4711 | 0.0456 | 0.0121 | -0.0235 | 0.0357 | [-0.0509, 0.1137] | insufficient |
| medium | 1d | 3516 | 0 | 35 | 0 | 773 | 166 | 0.4960 | 0.4662 | 0.0378 | 0.0299 | -0.0096 | 0.0395 | [-0.0262, 0.0858] | insufficient |
| high | 5m | 2291 | 0 | 0 | 0 | 498 | 498 | 0.4657 | 0.4574 | 0.0768 | 0.0083 | -0.0073 | 0.0156 | [-0.0368, 0.0535] | inconclusive |
| high | 15m | 2291 | 0 | 0 | 0 | 498 | 498 | 0.4810 | 0.4627 | 0.0563 | 0.0183 | -0.0086 | 0.0269 | [-0.0401, 0.0863] | inconclusive |
| high | 1h | 2291 | 0 | 0 | 0 | 498 | 404 | 0.4544 | 0.4972 | 0.0485 | -0.0428 | -0.0246 | -0.0182 | [-0.0878, 0.0675] | inconclusive |
| high | 4h | 2291 | 0 | 0 | 0 | 498 | 282 | 0.4871 | 0.4644 | 0.0485 | 0.0227 | -0.0147 | 0.0374 | [-0.0302, 0.1012] | insufficient |
| high | 1d | 2192 | 0 | 99 | 0 | 480 | 139 | 0.4804 | 0.4786 | 0.0411 | 0.0018 | 0.0004 | 0.0015 | [-0.0785, 0.0783] | insufficient |
| extreme | 5m | 2436 | 0 | 0 | 0 | 575 | 575 | 0.4717 | 0.4401 | 0.0883 | 0.0316 | -0.0057 | 0.0374 | [-0.0063, 0.0859] | inconclusive |
| extreme | 15m | 2436 | 0 | 0 | 0 | 575 | 574 | 0.4823 | 0.4618 | 0.0558 | 0.0205 | -0.0199 | 0.0405 | [-0.0250, 0.1034] | inconclusive |
| extreme | 1h | 2436 | 0 | 0 | 0 | 575 | 440 | 0.4959 | 0.4635 | 0.0406 | 0.0324 | -0.0421 | 0.0745 | [-0.0308, 0.1711] | inconclusive |
| extreme | 4h | 2436 | 0 | 0 | 0 | 575 | 274 | 0.5111 | 0.4532 | 0.0357 | 0.0579 | -0.0389 | 0.0968 | [-0.0045, 0.1864] | insufficient |
| extreme | 1d | 2243 | 5 | 188 | 0 | 546 | 115 | 0.4512 | 0.5189 | 0.0299 | -0.0678 | -0.0740 | 0.0062 | [-0.0495, 0.0683] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 1192 | 0 | 0 | 0 | 388 | 388 | 0.4589 | 0.4606 | 0.0805 | -0.0017 | -0.0009 | -0.0007 | [-0.0600, 0.0562] | inconclusive |
| value_edge | 15m | 1192 | 0 | 0 | 0 | 388 | 387 | 0.4807 | 0.4572 | 0.0621 | 0.0235 | 0.0066 | 0.0169 | [-0.0773, 0.1022] | inconclusive |
| value_edge | 1h | 1192 | 0 | 0 | 0 | 388 | 354 | 0.4564 | 0.4698 | 0.0738 | -0.0134 | 0.0192 | -0.0327 | [-0.1162, 0.0608] | insufficient |
| value_edge | 4h | 1192 | 0 | 0 | 0 | 388 | 286 | 0.4715 | 0.4111 | 0.1174 | 0.0604 | 0.0470 | 0.0134 | [-0.0744, 0.1089] | insufficient |
| value_edge | 1d | 1171 | 4 | 17 | 0 | 377 | 162 | 0.4987 | 0.4253 | 0.0760 | 0.0734 | 0.0416 | 0.0318 | [-0.0453, 0.1257] | insufficient |
| structural_extreme | 5m | 198 | 0 | 0 | 0 | 74 | 74 | 0.4495 | 0.3535 | 0.1970 | 0.0960 | -0.0119 | 0.1078 | [-0.0369, 0.2038] | insufficient |
| structural_extreme | 15m | 198 | 0 | 0 | 0 | 74 | 74 | 0.4242 | 0.3939 | 0.1818 | 0.0303 | -0.0315 | 0.0618 | [-0.1141, 0.2265] | insufficient |
| structural_extreme | 1h | 198 | 0 | 0 | 0 | 74 | 71 | 0.4394 | 0.4899 | 0.0707 | -0.0505 | 0.0582 | -0.1087 | [-0.2518, 0.0628] | insufficient |
| structural_extreme | 4h | 198 | 0 | 0 | 0 | 74 | 66 | 0.5253 | 0.2980 | 0.1768 | 0.2273 | 0.1054 | 0.1218 | [-0.0947, 0.2203] | insufficient |
| structural_extreme | 1d | 196 | 0 | 2 | 0 | 72 | 55 | 0.3112 | 0.3418 | 0.3469 | -0.0306 | 0.0063 | -0.0370 | [-0.1735, 0.1008] | insufficient |
| neither | 5m | 10600 | 0 | 0 | 0 | 2430 | 2430 | 0.4695 | 0.4531 | 0.0774 | 0.0164 | 0.0023 | 0.0141 | [-0.0157, 0.0451] | inconclusive |
| neither | 15m | 10600 | 0 | 0 | 0 | 2430 | 2429 | 0.4826 | 0.4633 | 0.0541 | 0.0193 | -0.0057 | 0.0250 | [-0.0101, 0.0676] | inconclusive |
| neither | 1h | 10600 | 0 | 0 | 0 | 2430 | 1880 | 0.4825 | 0.4657 | 0.0518 | 0.0169 | -0.0187 | 0.0355 | [-0.0195, 0.0890] | inconclusive |
| neither | 4h | 10578 | 0 | 22 | 0 | 2421 | 1075 | 0.4816 | 0.4485 | 0.0700 | 0.0331 | -0.0210 | 0.0540 | [0.0074, 0.0977] | hypothesis |
| neither | 1d | 10020 | 1 | 579 | 0 | 2317 | 295 | 0.4705 | 0.4658 | 0.0638 | 0.0047 | -0.0153 | 0.0200 | [-0.0421, 0.0844] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

##### by cvd

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| with | 5m | 8048 | 0 | 0 | 0 | 1728 | 1728 | 0.4672 | 0.4559 | 0.0769 | 0.0113 | 0.0006 | 0.0107 | [-0.0173, 0.0366] | inconclusive |
| with | 15m | 8048 | 0 | 0 | 0 | 1728 | 1728 | 0.4825 | 0.4642 | 0.0533 | 0.0183 | -0.0065 | 0.0248 | [-0.0143, 0.0661] | inconclusive |
| with | 1h | 8048 | 0 | 0 | 0 | 1728 | 1338 | 0.4795 | 0.4688 | 0.0517 | 0.0107 | -0.0166 | 0.0273 | [-0.0274, 0.0863] | inconclusive |
| with | 4h | 8038 | 0 | 10 | 0 | 1724 | 785 | 0.4747 | 0.4435 | 0.0817 | 0.0312 | -0.0115 | 0.0428 | [0.0002, 0.0854] | hypothesis (block-sensitive (28d: inconclusive)) |
| with | 1d | 7575 | 0 | 473 | 0 | 1642 | 243 | 0.4483 | 0.4759 | 0.0758 | -0.0276 | -0.0103 | -0.0173 | [-0.0822, 0.0495] | insufficient |
| against | 5m | 3905 | 0 | 0 | 0 | 932 | 932 | 0.4686 | 0.4471 | 0.0843 | 0.0215 | 0.0037 | 0.0178 | [-0.0271, 0.0604] | inconclusive |
| against | 15m | 3905 | 0 | 0 | 0 | 932 | 930 | 0.4763 | 0.4589 | 0.0648 | 0.0174 | -0.0030 | 0.0204 | [-0.0307, 0.0724] | inconclusive |
| against | 1h | 3905 | 0 | 0 | 0 | 932 | 743 | 0.4750 | 0.4653 | 0.0597 | 0.0097 | -0.0122 | 0.0219 | [-0.0384, 0.0761] | inconclusive |
| against | 4h | 3893 | 0 | 12 | 0 | 927 | 468 | 0.4942 | 0.4431 | 0.0627 | 0.0511 | -0.0155 | 0.0666 | [-0.0072, 0.1370] | inconclusive |
| against | 1d | 3775 | 5 | 125 | 0 | 897 | 192 | 0.5139 | 0.4305 | 0.0556 | 0.0834 | -0.0107 | 0.0942 | [0.0186, 0.1775] | insufficient |
| flat | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P1-sell w=1h

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 11015 | 0 | 0 | 0 | 800 | 800 | 0.4522 | 0.4577 | 0.0901 | -0.0055 | 0.0023 | -0.0079 | [-0.0305, 0.0206] | inconclusive |
| all | 15m | 11015 | 0 | 0 | 0 | 800 | 800 | 0.4624 | 0.4725 | 0.0651 | -0.0102 | -0.0054 | -0.0048 | [-0.0386, 0.0365] | inconclusive |
| all | 1h | 11015 | 0 | 0 | 0 | 800 | 800 | 0.4541 | 0.4757 | 0.0702 | -0.0216 | -0.0160 | -0.0056 | [-0.0570, 0.0550] | inconclusive |
| all | 4h | 11003 | 0 | 12 | 0 | 798 | 599 | 0.5049 | 0.4093 | 0.0858 | 0.0955 | -0.0099 | 0.1055 | [0.0562, 0.1552] | hypothesis |
| all | 1d | 10310 | 0 | 705 | 0 | 770 | 246 | 0.4831 | 0.4368 | 0.0801 | 0.0464 | -0.0144 | 0.0607 | [0.0072, 0.1213] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 2840 | 0 | 0 | 0 | 231 | 231 | 0.4458 | 0.4585 | 0.0958 | -0.0127 | 0.0057 | -0.0184 | [-0.0577, 0.0228] | insufficient |
| low | 15m | 2840 | 0 | 0 | 0 | 231 | 231 | 0.4570 | 0.4732 | 0.0697 | -0.0162 | -0.0049 | -0.0113 | [-0.0648, 0.0613] | insufficient |
| low | 1h | 2840 | 0 | 0 | 0 | 231 | 231 | 0.4359 | 0.4465 | 0.1176 | -0.0106 | -0.0075 | -0.0031 | [-0.0701, 0.0966] | insufficient |
| low | 4h | 2840 | 0 | 0 | 0 | 231 | 186 | 0.4246 | 0.3556 | 0.2197 | 0.0690 | 0.0243 | 0.0447 | [-0.0391, 0.1328] | insufficient |
| low | 1d | 2840 | 0 | 0 | 0 | 231 | 94 | 0.4289 | 0.3930 | 0.1782 | 0.0359 | 0.0348 | 0.0012 | [-0.1111, 0.1304] | insufficient |
| medium | 5m | 3275 | 0 | 0 | 0 | 248 | 248 | 0.4553 | 0.4623 | 0.0824 | -0.0070 | 0.0014 | -0.0084 | [-0.0380, 0.0300] | insufficient |
| medium | 15m | 3275 | 0 | 0 | 0 | 248 | 248 | 0.4647 | 0.4718 | 0.0635 | -0.0070 | -0.0155 | 0.0084 | [-0.0433, 0.0737] | insufficient |
| medium | 1h | 3275 | 0 | 0 | 0 | 248 | 248 | 0.4449 | 0.4925 | 0.0626 | -0.0476 | -0.0241 | -0.0235 | [-0.1123, 0.0761] | insufficient |
| medium | 4h | 3275 | 0 | 0 | 0 | 248 | 196 | 0.5395 | 0.4250 | 0.0354 | 0.1145 | -0.0205 | 0.1350 | [0.0266, 0.2319] | insufficient |
| medium | 1d | 3229 | 0 | 46 | 0 | 244 | 123 | 0.5618 | 0.3952 | 0.0430 | 0.1666 | -0.0048 | 0.1714 | [0.0329, 0.2959] | insufficient |
| high | 5m | 2168 | 0 | 0 | 0 | 162 | 162 | 0.4608 | 0.4493 | 0.0899 | 0.0115 | 0.0033 | 0.0082 | [-0.0400, 0.0539] | insufficient |
| high | 15m | 2168 | 0 | 0 | 0 | 162 | 162 | 0.4788 | 0.4451 | 0.0761 | 0.0337 | -0.0010 | 0.0347 | [-0.0360, 0.0933] | insufficient |
| high | 1h | 2168 | 0 | 0 | 0 | 162 | 162 | 0.5231 | 0.4234 | 0.0535 | 0.0996 | -0.0101 | 0.1097 | [0.0213, 0.1855] | insufficient |
| high | 4h | 2168 | 0 | 0 | 0 | 162 | 136 | 0.6190 | 0.3339 | 0.0470 | 0.2851 | 0.0049 | 0.2801 | [0.1374, 0.3638] | insufficient |
| high | 1d | 2098 | 0 | 70 | 0 | 158 | 96 | 0.5262 | 0.4285 | 0.0453 | 0.0977 | 0.0051 | 0.0926 | [-0.0537, 0.2182] | insufficient |
| extreme | 5m | 2380 | 0 | 0 | 0 | 188 | 188 | 0.4576 | 0.4508 | 0.0916 | 0.0067 | -0.0119 | 0.0186 | [-0.0239, 0.0609] | insufficient |
| extreme | 15m | 2380 | 0 | 0 | 0 | 188 | 188 | 0.4559 | 0.4882 | 0.0559 | -0.0324 | -0.0306 | -0.0018 | [-0.0615, 0.0675] | insufficient |
| extreme | 1h | 2380 | 0 | 0 | 0 | 188 | 188 | 0.4496 | 0.5130 | 0.0374 | -0.0634 | -0.0453 | -0.0181 | [-0.1228, 0.0980] | insufficient |
| extreme | 4h | 2380 | 0 | 0 | 0 | 188 | 156 | 0.4731 | 0.4945 | 0.0324 | -0.0214 | -0.0064 | -0.0151 | [-0.1253, 0.0657] | insufficient |
| extreme | 1d | 2143 | 0 | 237 | 0 | 179 | 87 | 0.3943 | 0.5656 | 0.0401 | -0.1713 | -0.0371 | -0.1341 | [-0.1926, -0.0285] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 1105 | 0 | 0 | 0 | 166 | 166 | 0.4317 | 0.4525 | 0.1158 | -0.0208 | 0.0083 | -0.0291 | [-0.0877, 0.0240] | insufficient |
| value_edge | 15m | 1105 | 0 | 0 | 0 | 166 | 166 | 0.4299 | 0.4570 | 0.1131 | -0.0271 | 0.0128 | -0.0400 | [-0.1276, 0.0591] | insufficient |
| value_edge | 1h | 1105 | 0 | 0 | 0 | 166 | 166 | 0.4471 | 0.4534 | 0.0995 | -0.0063 | 0.0167 | -0.0230 | [-0.1737, 0.1684] | insufficient |
| value_edge | 4h | 1105 | 0 | 0 | 0 | 166 | 152 | 0.5122 | 0.3421 | 0.1457 | 0.1701 | 0.0675 | 0.1026 | [-0.0063, 0.1949] | insufficient |
| value_edge | 1d | 1103 | 0 | 2 | 0 | 165 | 104 | 0.5286 | 0.3753 | 0.0961 | 0.1532 | 0.0008 | 0.1524 | [-0.0232, 0.3157] | insufficient |
| structural_extreme | 5m | 151 | 0 | 0 | 0 | 46 | 46 | 0.3907 | 0.3775 | 0.2318 | 0.0132 | -0.0419 | 0.0551 | [-0.1460, 0.2554] | insufficient |
| structural_extreme | 15m | 151 | 0 | 0 | 0 | 46 | 46 | 0.3113 | 0.4768 | 0.2119 | -0.1656 | -0.0778 | -0.0878 | [-0.2637, 0.1939] | insufficient |
| structural_extreme | 1h | 151 | 0 | 0 | 0 | 46 | 46 | 0.4437 | 0.4834 | 0.0728 | -0.0397 | -0.0076 | -0.0322 | [-0.3008, 0.3737] | insufficient |
| structural_extreme | 4h | 151 | 0 | 0 | 0 | 46 | 46 | 0.7152 | 0.2053 | 0.0795 | 0.5099 | 0.1764 | 0.3335 | [0.1192, 0.4595] | insufficient |
| structural_extreme | 1d | 142 | 0 | 9 | 0 | 45 | 39 | 0.3310 | 0.3451 | 0.3239 | -0.0141 | -0.0673 | 0.0532 | [-0.1646, 0.3381] | insufficient |
| neither | 5m | 9771 | 0 | 0 | 0 | 767 | 767 | 0.4554 | 0.4594 | 0.0851 | -0.0040 | 0.0024 | -0.0064 | [-0.0321, 0.0254] | inconclusive |
| neither | 15m | 9771 | 0 | 0 | 0 | 767 | 767 | 0.4683 | 0.4742 | 0.0575 | -0.0058 | -0.0062 | 0.0003 | [-0.0356, 0.0443] | inconclusive |
| neither | 1h | 9771 | 0 | 0 | 0 | 767 | 767 | 0.4552 | 0.4779 | 0.0668 | -0.0227 | -0.0196 | -0.0031 | [-0.0516, 0.0597] | inconclusive |
| neither | 4h | 9759 | 0 | 12 | 0 | 765 | 586 | 0.5007 | 0.4198 | 0.0795 | 0.0808 | -0.0214 | 0.1023 | [0.0448, 0.1624] | hypothesis |
| neither | 1d | 9077 | 0 | 694 | 0 | 737 | 242 | 0.4804 | 0.4453 | 0.0743 | 0.0351 | -0.0151 | 0.0502 | [-0.0075, 0.1152] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

##### by cvd

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| with | 5m | 8474 | 0 | 0 | 0 | 600 | 600 | 0.4614 | 0.4543 | 0.0843 | 0.0071 | 0.0023 | 0.0048 | [-0.0273, 0.0457] | inconclusive |
| with | 15m | 8474 | 0 | 0 | 0 | 600 | 600 | 0.4744 | 0.4691 | 0.0565 | 0.0053 | -0.0051 | 0.0104 | [-0.0346, 0.0652] | inconclusive |
| with | 1h | 8474 | 0 | 0 | 0 | 600 | 600 | 0.4652 | 0.4773 | 0.0575 | -0.0122 | -0.0166 | 0.0044 | [-0.0621, 0.0814] | inconclusive |
| with | 4h | 8464 | 0 | 10 | 0 | 599 | 458 | 0.5098 | 0.4037 | 0.0865 | 0.1061 | -0.0100 | 0.1161 | [0.0541, 0.1863] | hypothesis |
| with | 1d | 7897 | 0 | 577 | 0 | 578 | 203 | 0.4618 | 0.4498 | 0.0884 | 0.0120 | -0.0178 | 0.0298 | [-0.0507, 0.1133] | insufficient |
| against | 5m | 2541 | 0 | 0 | 0 | 242 | 242 | 0.4215 | 0.4691 | 0.1094 | -0.0476 | 0.0023 | -0.0499 | [-0.1085, -0.0015] | insufficient |
| against | 15m | 2541 | 0 | 0 | 0 | 242 | 242 | 0.4223 | 0.4841 | 0.0937 | -0.0618 | -0.0062 | -0.0556 | [-0.1287, -0.0031] | insufficient |
| against | 1h | 2541 | 0 | 0 | 0 | 242 | 242 | 0.4172 | 0.4703 | 0.1126 | -0.0531 | -0.0142 | -0.0389 | [-0.1369, 0.0342] | insufficient |
| against | 4h | 2539 | 0 | 2 | 0 | 241 | 192 | 0.4884 | 0.4281 | 0.0835 | 0.0603 | -0.0098 | 0.0700 | [-0.0303, 0.1487] | insufficient |
| against | 1d | 2413 | 0 | 128 | 0 | 232 | 116 | 0.5528 | 0.3941 | 0.0530 | 0.1587 | -0.0032 | 0.1619 | [0.0290, 0.3084] | insufficient |
| flat | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P2 w=5m

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 56318 | 0 | 0 | 0 | 15033 | 14866 | 0.4690 | 0.4971 | 0.0339 | -0.0280 | 0.0015 | -0.0295 | [-0.0473, -0.0144] | counter |
| all | 15m | 56318 | 0 | 0 | 0 | 15033 | 11119 | 0.4785 | 0.4926 | 0.0289 | -0.0141 | 0.0024 | -0.0165 | [-0.0284, -0.0070] | counter |
| all | 1h | 56317 | 1 | 0 | 0 | 15032 | 5362 | 0.4829 | 0.4866 | 0.0306 | -0.0037 | 0.0035 | -0.0072 | [-0.0207, -0.0001] | counter (block-sensitive (28d: inconclusive)) |
| all | 4h | 56129 | 3 | 186 | 0 | 14968 | 1830 | 0.4694 | 0.4696 | 0.0610 | -0.0002 | 0.0044 | -0.0047 | [-0.0248, 0.0083] | inconclusive |
| all | 1d | 54111 | 28 | 2179 | 0 | 14472 | 339 | 0.4650 | 0.4683 | 0.0668 | -0.0033 | -0.0006 | -0.0027 | [-0.0227, 0.0106] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 15648 | 0 | 0 | 0 | 4153 | 4103 | 0.4748 | 0.4919 | 0.0333 | -0.0171 | 0.0068 | -0.0238 | [-0.0569, 0.0035] | inconclusive (block-sensitive (28d: counter)) |
| low | 15m | 15648 | 0 | 0 | 0 | 4153 | 3107 | 0.4856 | 0.4912 | 0.0233 | -0.0056 | 0.0096 | -0.0152 | [-0.0392, -0.0028] | counter |
| low | 1h | 15648 | 0 | 0 | 0 | 4153 | 1530 | 0.4865 | 0.4762 | 0.0374 | 0.0103 | 0.0129 | -0.0026 | [-0.0364, 0.0093] | inconclusive |
| low | 4h | 15648 | 0 | 0 | 0 | 4153 | 581 | 0.4428 | 0.4240 | 0.1332 | 0.0188 | 0.0104 | 0.0084 | [-0.0312, 0.0223] | inconclusive |
| low | 1d | 15624 | 0 | 24 | 0 | 4149 | 157 | 0.4366 | 0.4474 | 0.1160 | -0.0108 | 0.0031 | -0.0139 | [-0.0459, 0.0059] | insufficient |
| medium | 5m | 16186 | 0 | 0 | 0 | 4316 | 4261 | 0.4753 | 0.4956 | 0.0292 | -0.0203 | 0.0039 | -0.0242 | [-0.0438, -0.0122] | counter |
| medium | 15m | 16186 | 0 | 0 | 0 | 4316 | 3181 | 0.4853 | 0.4911 | 0.0236 | -0.0058 | 0.0074 | -0.0132 | [-0.0381, 0.0000] | inconclusive |
| medium | 1h | 16186 | 0 | 0 | 0 | 4316 | 1571 | 0.4863 | 0.4889 | 0.0248 | -0.0027 | 0.0135 | -0.0162 | [-0.0443, -0.0072] | counter |
| medium | 4h | 16186 | 0 | 0 | 0 | 4316 | 639 | 0.4937 | 0.4734 | 0.0329 | 0.0203 | 0.0170 | 0.0033 | [-0.0354, 0.0169] | inconclusive |
| medium | 1d | 16077 | 0 | 109 | 0 | 4286 | 204 | 0.4823 | 0.4673 | 0.0504 | 0.0151 | 0.0116 | 0.0035 | [-0.0230, 0.0237] | insufficient |
| high | 5m | 10680 | 0 | 0 | 0 | 2963 | 2938 | 0.4636 | 0.5034 | 0.0331 | -0.0398 | 0.0039 | -0.0437 | [-0.0662, -0.0264] | counter |
| high | 15m | 10680 | 0 | 0 | 0 | 2963 | 2222 | 0.4743 | 0.4936 | 0.0320 | -0.0193 | 0.0070 | -0.0263 | [-0.0550, -0.0105] | counter |
| high | 1h | 10679 | 1 | 0 | 0 | 2962 | 1136 | 0.4837 | 0.4875 | 0.0288 | -0.0038 | 0.0148 | -0.0186 | [-0.0508, -0.0077] | counter |
| high | 4h | 10677 | 3 | 0 | 0 | 2961 | 506 | 0.4679 | 0.4944 | 0.0377 | -0.0265 | 0.0180 | -0.0445 | [-0.0805, -0.0340] | counter |
| high | 1d | 10362 | 4 | 314 | 0 | 2869 | 183 | 0.4664 | 0.4829 | 0.0507 | -0.0165 | 0.0047 | -0.0212 | [-0.0642, 0.0001] | insufficient |
| extreme | 5m | 12534 | 0 | 0 | 0 | 3450 | 3416 | 0.4522 | 0.5062 | 0.0416 | -0.0540 | 0.0066 | -0.0606 | [-0.0825, -0.0460] | counter |
| extreme | 15m | 12534 | 0 | 0 | 0 | 3450 | 2548 | 0.4603 | 0.4989 | 0.0408 | -0.0386 | 0.0123 | -0.0509 | [-0.0759, -0.0428] | counter |
| extreme | 1h | 12534 | 0 | 0 | 0 | 3450 | 1274 | 0.4701 | 0.4980 | 0.0319 | -0.0279 | 0.0189 | -0.0469 | [-0.0863, -0.0356] | counter |
| extreme | 4h | 12534 | 0 | 0 | 0 | 3450 | 489 | 0.4674 | 0.5024 | 0.0302 | -0.0349 | 0.0167 | -0.0517 | [-0.0938, -0.0337] | counter |
| extreme | 1d | 12048 | 24 | 462 | 0 | 3332 | 149 | 0.4773 | 0.4841 | 0.0385 | -0.0068 | 0.0123 | -0.0191 | [-0.0624, 0.0049] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 5176 | 0 | 0 | 0 | 2403 | 2397 | 0.4716 | 0.4934 | 0.0350 | -0.0218 | 0.0035 | -0.0253 | [-0.0493, -0.0031] | counter (block-sensitive (28d: inconclusive)) |
| value_edge | 15m | 5176 | 0 | 0 | 0 | 2403 | 2159 | 0.4811 | 0.4853 | 0.0336 | -0.0043 | 0.0049 | -0.0091 | [-0.0304, 0.0148] | inconclusive |
| value_edge | 1h | 5176 | 0 | 0 | 0 | 2403 | 1488 | 0.4749 | 0.4840 | 0.0412 | -0.0091 | 0.0014 | -0.0104 | [-0.0376, 0.0191] | inconclusive |
| value_edge | 4h | 5171 | 0 | 5 | 0 | 2400 | 839 | 0.4606 | 0.4411 | 0.0982 | 0.0195 | 0.0043 | 0.0152 | [-0.0334, 0.0535] | inconclusive |
| value_edge | 1d | 5073 | 6 | 97 | 0 | 2349 | 267 | 0.4652 | 0.4510 | 0.0838 | 0.0142 | -0.0078 | 0.0219 | [-0.0230, 0.0652] | insufficient |
| structural_extreme | 5m | 1860 | 0 | 0 | 0 | 1214 | 1214 | 0.5011 | 0.4720 | 0.0269 | 0.0290 | 0.0202 | 0.0089 | [-0.0519, 0.0662] | inconclusive |
| structural_extreme | 15m | 1860 | 0 | 0 | 0 | 1214 | 1167 | 0.5097 | 0.4613 | 0.0290 | 0.0484 | 0.0265 | 0.0219 | [-0.0404, 0.0888] | inconclusive |
| structural_extreme | 1h | 1860 | 0 | 0 | 0 | 1214 | 1024 | 0.5204 | 0.4280 | 0.0516 | 0.0925 | 0.0081 | 0.0844 | [0.0132, 0.1499] | hypothesis (block-sensitive (28d: inconclusive)) |
| structural_extreme | 4h | 1856 | 0 | 4 | 0 | 1210 | 714 | 0.4526 | 0.3895 | 0.1579 | 0.0630 | 0.0271 | 0.0359 | [-0.0095, 0.0717] | inconclusive |
| structural_extreme | 1d | 1803 | 0 | 57 | 0 | 1169 | 257 | 0.4204 | 0.4443 | 0.1353 | -0.0238 | -0.0067 | -0.0172 | [-0.0645, 0.0277] | insufficient |
| neither | 5m | 49625 | 0 | 0 | 0 | 13846 | 13717 | 0.4678 | 0.4981 | 0.0342 | -0.0303 | 0.0011 | -0.0314 | [-0.0486, -0.0165] | counter |
| neither | 15m | 49625 | 0 | 0 | 0 | 13846 | 10425 | 0.4772 | 0.4943 | 0.0285 | -0.0171 | 0.0019 | -0.0190 | [-0.0307, -0.0098] | counter |
| neither | 1h | 49624 | 1 | 0 | 0 | 13845 | 5135 | 0.4824 | 0.4887 | 0.0289 | -0.0063 | 0.0039 | -0.0102 | [-0.0246, -0.0028] | counter |
| neither | 4h | 49445 | 3 | 177 | 0 | 13786 | 1794 | 0.4706 | 0.4750 | 0.0544 | -0.0044 | 0.0038 | -0.0082 | [-0.0278, 0.0049] | inconclusive |
| neither | 1d | 47576 | 22 | 2027 | 0 | 13319 | 339 | 0.4657 | 0.4714 | 0.0630 | -0.0057 | 0.0001 | -0.0058 | [-0.0255, 0.0073] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

##### by side

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| buy | 5m | 27934 | 0 | 0 | 0 | 8462 | 8448 | 0.4669 | 0.4980 | 0.0351 | -0.0311 | 0.0043 | -0.0354 | [-0.0556, -0.0166] | counter |
| buy | 15m | 27934 | 0 | 0 | 0 | 8462 | 7075 | 0.4735 | 0.4958 | 0.0306 | -0.0223 | 0.0002 | -0.0225 | [-0.0389, -0.0094] | counter |
| buy | 1h | 27934 | 0 | 0 | 0 | 8462 | 4118 | 0.4774 | 0.4893 | 0.0334 | -0.0119 | -0.0081 | -0.0037 | [-0.0213, 0.0090] | inconclusive |
| buy | 4h | 27826 | 0 | 108 | 0 | 8421 | 1646 | 0.4620 | 0.4723 | 0.0657 | -0.0102 | -0.0087 | -0.0015 | [-0.0284, 0.0185] | inconclusive |
| buy | 1d | 26902 | 10 | 1022 | 0 | 8157 | 333 | 0.4577 | 0.4706 | 0.0717 | -0.0129 | -0.0090 | -0.0039 | [-0.0373, 0.0207] | insufficient |
| sell | 5m | 28384 | 0 | 0 | 0 | 8377 | 8359 | 0.4711 | 0.4962 | 0.0327 | -0.0250 | -0.0013 | -0.0237 | [-0.0413, -0.0092] | counter |
| sell | 15m | 28384 | 0 | 0 | 0 | 8377 | 6989 | 0.4834 | 0.4894 | 0.0271 | -0.0060 | 0.0046 | -0.0106 | [-0.0234, -0.0013] | counter (block-sensitive (28d: inconclusive)) |
| sell | 1h | 28383 | 1 | 0 | 0 | 8376 | 4063 | 0.4883 | 0.4839 | 0.0278 | 0.0043 | 0.0150 | -0.0106 | [-0.0274, 0.0021] | inconclusive |
| sell | 4h | 28303 | 3 | 78 | 0 | 8347 | 1636 | 0.4766 | 0.4671 | 0.0563 | 0.0096 | 0.0174 | -0.0078 | [-0.0380, 0.0142] | inconclusive |
| sell | 1d | 27209 | 18 | 1157 | 0 | 8061 | 331 | 0.4722 | 0.4659 | 0.0619 | 0.0062 | 0.0078 | -0.0016 | [-0.0381, 0.0263] | insufficient |

##### by cvd

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| with | 5m | 32231 | 0 | 0 | 0 | 9227 | 9199 | 0.4727 | 0.4999 | 0.0274 | -0.0271 | 0.0012 | -0.0284 | [-0.0478, -0.0134] | counter |
| with | 15m | 32231 | 0 | 0 | 0 | 9227 | 7597 | 0.4818 | 0.4937 | 0.0245 | -0.0119 | 0.0027 | -0.0147 | [-0.0312, -0.0022] | counter |
| with | 1h | 32231 | 0 | 0 | 0 | 9227 | 4325 | 0.4861 | 0.4865 | 0.0274 | -0.0003 | 0.0049 | -0.0052 | [-0.0283, 0.0079] | inconclusive |
| with | 4h | 32134 | 0 | 97 | 0 | 9191 | 1685 | 0.4756 | 0.4657 | 0.0587 | 0.0099 | 0.0053 | 0.0046 | [-0.0224, 0.0231] | inconclusive |
| with | 1d | 30939 | 16 | 1276 | 0 | 8881 | 335 | 0.4667 | 0.4662 | 0.0671 | 0.0005 | -0.0031 | 0.0036 | [-0.0253, 0.0252] | insufficient |
| against | 5m | 24087 | 0 | 0 | 0 | 7850 | 7841 | 0.4641 | 0.4933 | 0.0426 | -0.0292 | 0.0018 | -0.0310 | [-0.0486, -0.0144] | counter |
| against | 15m | 24087 | 0 | 0 | 0 | 7850 | 6643 | 0.4741 | 0.4911 | 0.0347 | -0.0170 | 0.0019 | -0.0190 | [-0.0327, -0.0069] | counter |
| against | 1h | 24086 | 1 | 0 | 0 | 7849 | 3914 | 0.4785 | 0.4867 | 0.0348 | -0.0082 | 0.0016 | -0.0099 | [-0.0267, 0.0041] | inconclusive |
| against | 4h | 23995 | 3 | 89 | 0 | 7814 | 1598 | 0.4611 | 0.4748 | 0.0641 | -0.0138 | 0.0033 | -0.0170 | [-0.0454, 0.0068] | inconclusive |
| against | 1d | 23172 | 12 | 903 | 0 | 7565 | 330 | 0.4626 | 0.4710 | 0.0663 | -0.0084 | 0.0028 | -0.0112 | [-0.0553, 0.0329] | insufficient |
| flat | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P2 w=15m

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 58785 | 0 | 0 | 0 | 5569 | 5569 | 0.4747 | 0.4906 | 0.0348 | -0.0159 | 0.0016 | -0.0176 | [-0.0388, -0.0021] | counter |
| all | 15m | 58785 | 0 | 0 | 0 | 5569 | 5551 | 0.4814 | 0.4917 | 0.0269 | -0.0103 | 0.0029 | -0.0132 | [-0.0313, -0.0008] | counter |
| all | 1h | 58785 | 0 | 0 | 0 | 5569 | 3716 | 0.4864 | 0.4891 | 0.0245 | -0.0027 | 0.0052 | -0.0079 | [-0.0277, 0.0022] | inconclusive |
| all | 4h | 58607 | 0 | 178 | 0 | 5547 | 1585 | 0.4753 | 0.4699 | 0.0549 | 0.0054 | 0.0062 | -0.0008 | [-0.0234, 0.0125] | inconclusive |
| all | 1d | 56527 | 28 | 2230 | 0 | 5359 | 331 | 0.4734 | 0.4602 | 0.0664 | 0.0131 | 0.0008 | 0.0124 | [-0.0102, 0.0267] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 16798 | 0 | 0 | 0 | 1606 | 1606 | 0.4743 | 0.4907 | 0.0349 | -0.0164 | 0.0079 | -0.0243 | [-0.0550, -0.0062] | counter |
| low | 15m | 16798 | 0 | 0 | 0 | 1606 | 1598 | 0.4845 | 0.4964 | 0.0190 | -0.0119 | 0.0106 | -0.0225 | [-0.0527, -0.0083] | counter |
| low | 1h | 16798 | 0 | 0 | 0 | 1606 | 1077 | 0.4864 | 0.4832 | 0.0305 | 0.0032 | 0.0166 | -0.0133 | [-0.0593, 0.0008] | inconclusive |
| low | 4h | 16798 | 0 | 0 | 0 | 1606 | 503 | 0.4546 | 0.4254 | 0.1200 | 0.0292 | 0.0167 | 0.0124 | [-0.0387, 0.0320] | inconclusive |
| low | 1d | 16783 | 0 | 15 | 0 | 1605 | 149 | 0.4419 | 0.4492 | 0.1089 | -0.0073 | 0.0079 | -0.0152 | [-0.0632, 0.0102] | insufficient |
| medium | 5m | 16776 | 0 | 0 | 0 | 1673 | 1673 | 0.4857 | 0.4850 | 0.0293 | 0.0007 | 0.0052 | -0.0045 | [-0.0311, 0.0081] | inconclusive |
| medium | 15m | 16776 | 0 | 0 | 0 | 1673 | 1669 | 0.4911 | 0.4832 | 0.0257 | 0.0079 | 0.0116 | -0.0037 | [-0.0361, 0.0085] | inconclusive |
| medium | 1h | 16776 | 0 | 0 | 0 | 1673 | 1153 | 0.5000 | 0.4809 | 0.0191 | 0.0191 | 0.0218 | -0.0028 | [-0.0457, 0.0118] | inconclusive |
| medium | 4h | 16776 | 0 | 0 | 0 | 1673 | 560 | 0.5053 | 0.4645 | 0.0302 | 0.0408 | 0.0263 | 0.0145 | [-0.0393, 0.0329] | inconclusive |
| medium | 1d | 16648 | 0 | 128 | 0 | 1656 | 199 | 0.4870 | 0.4614 | 0.0517 | 0.0256 | 0.0205 | 0.0051 | [-0.0360, 0.0379] | insufficient |
| high | 5m | 10931 | 0 | 0 | 0 | 1171 | 1171 | 0.4718 | 0.4930 | 0.0352 | -0.0212 | 0.0043 | -0.0256 | [-0.0486, -0.0172] | counter |
| high | 15m | 10931 | 0 | 0 | 0 | 1171 | 1168 | 0.4755 | 0.4942 | 0.0303 | -0.0187 | 0.0105 | -0.0292 | [-0.0599, -0.0204] | counter |
| high | 1h | 10931 | 0 | 0 | 0 | 1171 | 800 | 0.4777 | 0.4988 | 0.0235 | -0.0210 | 0.0193 | -0.0403 | [-0.0865, -0.0228] | counter |
| high | 4h | 10931 | 0 | 0 | 0 | 1171 | 438 | 0.4592 | 0.5078 | 0.0329 | -0.0486 | 0.0243 | -0.0729 | [-0.1253, -0.0524] | counter |
| high | 1d | 10649 | 1 | 281 | 0 | 1136 | 172 | 0.4722 | 0.4770 | 0.0508 | -0.0049 | 0.0138 | -0.0186 | [-0.0763, 0.0098] | insufficient |
| extreme | 5m | 12945 | 0 | 0 | 0 | 1286 | 1286 | 0.4597 | 0.4983 | 0.0420 | -0.0385 | 0.0074 | -0.0459 | [-0.0841, -0.0226] | counter |
| extreme | 15m | 12945 | 0 | 0 | 0 | 1286 | 1284 | 0.4676 | 0.4956 | 0.0368 | -0.0280 | 0.0133 | -0.0413 | [-0.0853, -0.0278] | counter |
| extreme | 1h | 12945 | 0 | 0 | 0 | 1286 | 890 | 0.4747 | 0.5004 | 0.0249 | -0.0257 | 0.0212 | -0.0469 | [-0.1038, -0.0358] | counter |
| extreme | 4h | 12945 | 0 | 0 | 0 | 1286 | 421 | 0.4745 | 0.5022 | 0.0233 | -0.0277 | 0.0219 | -0.0495 | [-0.1064, -0.0292] | counter |
| extreme | 1d | 12447 | 27 | 471 | 0 | 1240 | 146 | 0.4988 | 0.4592 | 0.0420 | 0.0395 | 0.0188 | 0.0207 | [-0.0511, 0.0548] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 5205 | 0 | 0 | 0 | 1285 | 1285 | 0.4870 | 0.4722 | 0.0407 | 0.0148 | 0.0019 | 0.0129 | [-0.0282, 0.0433] | inconclusive |
| value_edge | 15m | 5205 | 0 | 0 | 0 | 1285 | 1285 | 0.4928 | 0.4740 | 0.0332 | 0.0188 | 0.0048 | 0.0140 | [-0.0314, 0.0510] | inconclusive |
| value_edge | 1h | 5205 | 0 | 0 | 0 | 1285 | 1120 | 0.4830 | 0.4767 | 0.0403 | 0.0063 | 0.0059 | 0.0004 | [-0.0590, 0.0468] | inconclusive |
| value_edge | 4h | 5198 | 0 | 7 | 0 | 1283 | 715 | 0.4625 | 0.4363 | 0.1012 | 0.0262 | 0.0150 | 0.0111 | [-0.0397, 0.0510] | inconclusive |
| value_edge | 1d | 5100 | 6 | 99 | 0 | 1254 | 253 | 0.4571 | 0.4576 | 0.0853 | -0.0006 | 0.0021 | -0.0026 | [-0.0749, 0.0590] | insufficient |
| structural_extreme | 5m | 1468 | 0 | 0 | 0 | 772 | 772 | 0.4952 | 0.4666 | 0.0381 | 0.0286 | 0.0123 | 0.0163 | [-0.0606, 0.0856] | inconclusive |
| structural_extreme | 15m | 1468 | 0 | 0 | 0 | 772 | 772 | 0.5020 | 0.4687 | 0.0293 | 0.0334 | 0.0254 | 0.0080 | [-0.0631, 0.0774] | inconclusive |
| structural_extreme | 1h | 1468 | 0 | 0 | 0 | 772 | 722 | 0.5327 | 0.4257 | 0.0416 | 0.1069 | 0.0192 | 0.0878 | [0.0085, 0.1511] | hypothesis (block-sensitive (28d: inconclusive)) |
| structural_extreme | 4h | 1465 | 0 | 3 | 0 | 769 | 553 | 0.4614 | 0.4000 | 0.1386 | 0.0614 | 0.0465 | 0.0149 | [-0.0408, 0.0578] | inconclusive |
| structural_extreme | 1d | 1437 | 0 | 31 | 0 | 748 | 228 | 0.4050 | 0.4649 | 0.1301 | -0.0598 | -0.0006 | -0.0592 | [-0.1344, 0.0004] | insufficient |
| neither | 5m | 52375 | 0 | 0 | 0 | 5284 | 5284 | 0.4730 | 0.4927 | 0.0343 | -0.0198 | 0.0017 | -0.0215 | [-0.0413, -0.0075] | counter |
| neither | 15m | 52375 | 0 | 0 | 0 | 5284 | 5271 | 0.4798 | 0.4939 | 0.0263 | -0.0141 | 0.0026 | -0.0168 | [-0.0348, -0.0050] | counter |
| neither | 1h | 52375 | 0 | 0 | 0 | 5284 | 3577 | 0.4856 | 0.4916 | 0.0228 | -0.0060 | 0.0051 | -0.0111 | [-0.0293, -0.0013] | counter |
| neither | 4h | 52207 | 0 | 168 | 0 | 5264 | 1555 | 0.4767 | 0.4745 | 0.0488 | 0.0022 | 0.0047 | -0.0025 | [-0.0263, 0.0121] | inconclusive |
| neither | 1d | 50252 | 22 | 2101 | 0 | 5080 | 327 | 0.4761 | 0.4606 | 0.0633 | 0.0155 | 0.0007 | 0.0148 | [-0.0073, 0.0279] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

##### by side

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| buy | 5m | 28882 | 0 | 0 | 0 | 3377 | 3377 | 0.4684 | 0.4911 | 0.0405 | -0.0227 | 0.0047 | -0.0274 | [-0.0482, -0.0125] | counter |
| buy | 15m | 28882 | 0 | 0 | 0 | 3377 | 3375 | 0.4726 | 0.4952 | 0.0323 | -0.0226 | 0.0009 | -0.0235 | [-0.0394, -0.0137] | counter |
| buy | 1h | 28882 | 0 | 0 | 0 | 3377 | 2608 | 0.4774 | 0.4935 | 0.0291 | -0.0162 | -0.0066 | -0.0096 | [-0.0322, 0.0024] | inconclusive |
| buy | 4h | 28768 | 0 | 114 | 0 | 3365 | 1316 | 0.4688 | 0.4689 | 0.0623 | -0.0001 | -0.0077 | 0.0076 | [-0.0216, 0.0283] | inconclusive |
| buy | 1d | 27816 | 19 | 1047 | 0 | 3260 | 313 | 0.4695 | 0.4589 | 0.0716 | 0.0105 | -0.0083 | 0.0188 | [-0.0121, 0.0386] | insufficient |
| sell | 5m | 29903 | 0 | 0 | 0 | 3366 | 3366 | 0.4807 | 0.4901 | 0.0292 | -0.0094 | -0.0013 | -0.0081 | [-0.0346, 0.0124] | inconclusive |
| sell | 15m | 29903 | 0 | 0 | 0 | 3366 | 3365 | 0.4899 | 0.4883 | 0.0218 | 0.0016 | 0.0049 | -0.0033 | [-0.0330, 0.0188] | inconclusive |
| sell | 1h | 29903 | 0 | 0 | 0 | 3366 | 2585 | 0.4951 | 0.4848 | 0.0201 | 0.0104 | 0.0167 | -0.0063 | [-0.0330, 0.0142] | inconclusive |
| sell | 4h | 29839 | 0 | 64 | 0 | 3353 | 1317 | 0.4815 | 0.4709 | 0.0477 | 0.0106 | 0.0195 | -0.0090 | [-0.0406, 0.0132] | inconclusive |
| sell | 1d | 28711 | 9 | 1183 | 0 | 3233 | 316 | 0.4772 | 0.4615 | 0.0613 | 0.0157 | 0.0095 | 0.0061 | [-0.0301, 0.0342] | insufficient |

##### by cvd

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| with | 5m | 37781 | 0 | 0 | 0 | 3893 | 3893 | 0.4799 | 0.4935 | 0.0266 | -0.0136 | 0.0015 | -0.0150 | [-0.0355, 0.0001] | inconclusive (block-sensitive (28d: counter)) |
| with | 15m | 37781 | 0 | 0 | 0 | 3893 | 3886 | 0.4848 | 0.4956 | 0.0197 | -0.0108 | 0.0030 | -0.0138 | [-0.0323, -0.0017] | counter |
| with | 1h | 37781 | 0 | 0 | 0 | 3893 | 2910 | 0.4883 | 0.4910 | 0.0206 | -0.0027 | 0.0063 | -0.0091 | [-0.0287, 0.0006] | inconclusive (block-sensitive (28d: counter)) |
| with | 4h | 37672 | 0 | 109 | 0 | 3879 | 1408 | 0.4823 | 0.4661 | 0.0516 | 0.0162 | 0.0075 | 0.0087 | [-0.0153, 0.0244] | inconclusive |
| with | 1d | 36300 | 23 | 1458 | 0 | 3739 | 320 | 0.4726 | 0.4633 | 0.0641 | 0.0094 | -0.0009 | 0.0103 | [-0.0210, 0.0323] | insufficient |
| against | 5m | 21004 | 0 | 0 | 0 | 2979 | 2979 | 0.4652 | 0.4854 | 0.0494 | -0.0202 | 0.0019 | -0.0221 | [-0.0466, -0.0038] | counter |
| against | 15m | 21004 | 0 | 0 | 0 | 2979 | 2979 | 0.4753 | 0.4847 | 0.0400 | -0.0093 | 0.0027 | -0.0120 | [-0.0424, 0.0101] | inconclusive |
| against | 1h | 21004 | 0 | 0 | 0 | 2979 | 2371 | 0.4830 | 0.4855 | 0.0315 | -0.0026 | 0.0033 | -0.0059 | [-0.0345, 0.0181] | inconclusive |
| against | 4h | 20935 | 0 | 69 | 0 | 2968 | 1248 | 0.4625 | 0.4767 | 0.0608 | -0.0142 | 0.0037 | -0.0180 | [-0.0584, 0.0110] | inconclusive |
| against | 1d | 20227 | 5 | 772 | 0 | 2877 | 312 | 0.4747 | 0.4548 | 0.0705 | 0.0199 | 0.0038 | 0.0161 | [-0.0312, 0.0581] | insufficient |
| flat | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P2 w=1h

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 61163 | 0 | 0 | 0 | 1610 | 1610 | 0.4693 | 0.4841 | 0.0467 | -0.0148 | 0.0016 | -0.0163 | [-0.0299, -0.0060] | counter |
| all | 15m | 61163 | 0 | 0 | 0 | 1610 | 1610 | 0.4811 | 0.4912 | 0.0277 | -0.0101 | 0.0031 | -0.0131 | [-0.0334, -0.0003] | counter |
| all | 1h | 61163 | 0 | 0 | 0 | 1610 | 1610 | 0.4874 | 0.4892 | 0.0234 | -0.0018 | 0.0066 | -0.0083 | [-0.0325, 0.0067] | inconclusive |
| all | 4h | 61033 | 0 | 130 | 0 | 1606 | 1078 | 0.4980 | 0.4472 | 0.0548 | 0.0508 | 0.0058 | 0.0449 | [0.0130, 0.0676] | hypothesis |
| all | 1d | 59041 | 19 | 2103 | 0 | 1542 | 296 | 0.4814 | 0.4507 | 0.0679 | 0.0308 | 0.0021 | 0.0287 | [-0.0116, 0.0639] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 17461 | 0 | 0 | 0 | 509 | 509 | 0.4665 | 0.4816 | 0.0519 | -0.0151 | 0.0074 | -0.0225 | [-0.0535, -0.0049] | counter |
| low | 15m | 17461 | 0 | 0 | 0 | 509 | 509 | 0.4761 | 0.5012 | 0.0227 | -0.0251 | 0.0103 | -0.0354 | [-0.0765, -0.0126] | counter |
| low | 1h | 17461 | 0 | 0 | 0 | 509 | 509 | 0.4787 | 0.4948 | 0.0265 | -0.0162 | 0.0172 | -0.0333 | [-0.0824, -0.0119] | counter |
| low | 4h | 17461 | 0 | 0 | 0 | 509 | 362 | 0.4891 | 0.3911 | 0.1198 | 0.0980 | 0.0112 | 0.0868 | [0.0140, 0.1123] | insufficient |
| low | 1d | 17451 | 0 | 10 | 0 | 508 | 138 | 0.4409 | 0.4451 | 0.1140 | -0.0042 | 0.0092 | -0.0135 | [-0.1053, 0.0500] | insufficient |
| medium | 5m | 16910 | 0 | 0 | 0 | 598 | 598 | 0.4768 | 0.4843 | 0.0389 | -0.0076 | 0.0044 | -0.0120 | [-0.0413, 0.0005] | inconclusive (block-sensitive (28d: counter)) |
| medium | 15m | 16910 | 0 | 0 | 0 | 598 | 598 | 0.4873 | 0.4873 | 0.0253 | 0.0000 | 0.0139 | -0.0139 | [-0.0453, -0.0020] | counter |
| medium | 1h | 16910 | 0 | 0 | 0 | 598 | 598 | 0.4939 | 0.4858 | 0.0203 | 0.0081 | 0.0263 | -0.0182 | [-0.0764, 0.0098] | inconclusive |
| medium | 4h | 16910 | 0 | 0 | 0 | 598 | 427 | 0.5235 | 0.4428 | 0.0336 | 0.0807 | 0.0325 | 0.0482 | [-0.0252, 0.0859] | inconclusive |
| medium | 1d | 16815 | 0 | 95 | 0 | 591 | 184 | 0.4862 | 0.4571 | 0.0567 | 0.0291 | 0.0034 | 0.0257 | [-0.0493, 0.0931] | insufficient |
| high | 5m | 11649 | 0 | 0 | 0 | 426 | 426 | 0.4691 | 0.4854 | 0.0454 | -0.0163 | 0.0029 | -0.0192 | [-0.0460, -0.0065] | counter |
| high | 15m | 11649 | 0 | 0 | 0 | 426 | 426 | 0.4777 | 0.4913 | 0.0310 | -0.0136 | 0.0080 | -0.0216 | [-0.0618, -0.0032] | counter |
| high | 1h | 11649 | 0 | 0 | 0 | 426 | 426 | 0.4862 | 0.4915 | 0.0223 | -0.0052 | 0.0191 | -0.0244 | [-0.0761, -0.0034] | counter |
| high | 4h | 11649 | 0 | 0 | 0 | 426 | 332 | 0.4838 | 0.4851 | 0.0311 | -0.0013 | 0.0185 | -0.0198 | [-0.0770, 0.0122] | insufficient |
| high | 1d | 11337 | 1 | 311 | 0 | 415 | 157 | 0.4788 | 0.4702 | 0.0510 | 0.0086 | 0.0086 | -0.0001 | [-0.0768, 0.0564] | insufficient |
| extreme | 5m | 13829 | 0 | 0 | 0 | 428 | 428 | 0.4632 | 0.4861 | 0.0507 | -0.0229 | 0.0081 | -0.0309 | [-0.0585, -0.0148] | counter |
| extreme | 15m | 13829 | 0 | 0 | 0 | 428 | 428 | 0.4842 | 0.4815 | 0.0343 | 0.0027 | 0.0172 | -0.0146 | [-0.0593, 0.0010] | inconclusive |
| extreme | 1h | 13829 | 0 | 0 | 0 | 428 | 428 | 0.4910 | 0.4843 | 0.0247 | 0.0067 | 0.0216 | -0.0150 | [-0.0903, 0.0192] | inconclusive |
| extreme | 4h | 13829 | 0 | 0 | 0 | 428 | 302 | 0.4839 | 0.4953 | 0.0208 | -0.0114 | 0.0140 | -0.0254 | [-0.1139, 0.0184] | insufficient |
| extreme | 1d | 13438 | 18 | 373 | 0 | 413 | 128 | 0.5304 | 0.4334 | 0.0362 | 0.0970 | 0.0288 | 0.0682 | [-0.0433, 0.1493] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 5050 | 0 | 0 | 0 | 561 | 561 | 0.4646 | 0.4861 | 0.0493 | -0.0216 | -0.0014 | -0.0202 | [-0.0536, 0.0017] | inconclusive |
| value_edge | 15m | 5050 | 0 | 0 | 0 | 561 | 561 | 0.4754 | 0.4943 | 0.0303 | -0.0188 | 0.0025 | -0.0213 | [-0.0677, 0.0117] | inconclusive |
| value_edge | 1h | 5050 | 0 | 0 | 0 | 561 | 561 | 0.4733 | 0.4960 | 0.0307 | -0.0228 | 0.0208 | -0.0436 | [-0.0957, -0.0003] | counter (block-sensitive (28d: inconclusive)) |
| value_edge | 4h | 5050 | 0 | 0 | 0 | 561 | 485 | 0.4717 | 0.4287 | 0.0996 | 0.0430 | 0.0183 | 0.0247 | [-0.0590, 0.0913] | inconclusive |
| value_edge | 1d | 4920 | 2 | 128 | 0 | 548 | 215 | 0.4378 | 0.4797 | 0.0825 | -0.0419 | 0.0076 | -0.0494 | [-0.1434, 0.0315] | insufficient |
| structural_extreme | 5m | 938 | 0 | 0 | 0 | 360 | 360 | 0.4691 | 0.4947 | 0.0362 | -0.0256 | 0.0021 | -0.0277 | [-0.0827, 0.0351] | insufficient |
| structural_extreme | 15m | 938 | 0 | 0 | 0 | 360 | 360 | 0.4616 | 0.5032 | 0.0352 | -0.0416 | 0.0084 | -0.0500 | [-0.1321, 0.0388] | insufficient |
| structural_extreme | 1h | 938 | 0 | 0 | 0 | 360 | 360 | 0.5416 | 0.4200 | 0.0384 | 0.1215 | 0.0421 | 0.0794 | [0.0007, 0.1232] | insufficient |
| structural_extreme | 4h | 937 | 0 | 1 | 0 | 359 | 328 | 0.4952 | 0.3543 | 0.1505 | 0.1409 | 0.0213 | 0.1196 | [0.0352, 0.1572] | insufficient |
| structural_extreme | 1d | 925 | 0 | 13 | 0 | 351 | 179 | 0.4357 | 0.4595 | 0.1049 | -0.0238 | -0.0284 | 0.0046 | [-0.0905, 0.0756] | insufficient |
| neither | 5m | 55363 | 0 | 0 | 0 | 1567 | 1567 | 0.4696 | 0.4838 | 0.0466 | -0.0142 | 0.0018 | -0.0160 | [-0.0298, -0.0050] | counter |
| neither | 15m | 55363 | 0 | 0 | 0 | 1567 | 1567 | 0.4817 | 0.4909 | 0.0274 | -0.0092 | 0.0031 | -0.0124 | [-0.0326, 0.0011] | inconclusive |
| neither | 1h | 55363 | 0 | 0 | 0 | 1567 | 1567 | 0.4878 | 0.4897 | 0.0225 | -0.0020 | 0.0047 | -0.0066 | [-0.0317, 0.0103] | inconclusive |
| neither | 4h | 55234 | 0 | 129 | 0 | 1563 | 1055 | 0.5002 | 0.4500 | 0.0499 | 0.0502 | 0.0046 | 0.0456 | [0.0135, 0.0696] | hypothesis |
| neither | 1d | 53382 | 17 | 1964 | 0 | 1500 | 294 | 0.4853 | 0.4486 | 0.0661 | 0.0367 | 0.0016 | 0.0351 | [-0.0035, 0.0690] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

##### by side

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| buy | 5m | 29130 | 0 | 0 | 0 | 1020 | 1020 | 0.4638 | 0.4829 | 0.0533 | -0.0192 | 0.0048 | -0.0240 | [-0.0420, -0.0081] | counter |
| buy | 15m | 29130 | 0 | 0 | 0 | 1020 | 1020 | 0.4721 | 0.4943 | 0.0337 | -0.0222 | 0.0014 | -0.0236 | [-0.0519, -0.0020] | counter |
| buy | 1h | 29130 | 0 | 0 | 0 | 1020 | 1020 | 0.4771 | 0.4939 | 0.0290 | -0.0168 | -0.0052 | -0.0116 | [-0.0469, 0.0159] | inconclusive |
| buy | 4h | 29031 | 0 | 99 | 0 | 1017 | 775 | 0.4925 | 0.4469 | 0.0606 | 0.0456 | -0.0058 | 0.0514 | [0.0037, 0.0887] | hypothesis |
| buy | 1d | 28275 | 11 | 844 | 0 | 985 | 268 | 0.4843 | 0.4421 | 0.0736 | 0.0422 | -0.0050 | 0.0471 | [-0.0032, 0.0849] | insufficient |
| sell | 5m | 32033 | 0 | 0 | 0 | 1015 | 1015 | 0.4743 | 0.4851 | 0.0406 | -0.0108 | -0.0014 | -0.0093 | [-0.0254, 0.0009] | inconclusive |
| sell | 15m | 32033 | 0 | 0 | 0 | 1015 | 1015 | 0.4894 | 0.4884 | 0.0223 | 0.0010 | 0.0047 | -0.0037 | [-0.0222, 0.0076] | inconclusive |
| sell | 1h | 32033 | 0 | 0 | 0 | 1015 | 1015 | 0.4968 | 0.4849 | 0.0183 | 0.0119 | 0.0172 | -0.0054 | [-0.0315, 0.0125] | inconclusive |
| sell | 4h | 32002 | 0 | 31 | 0 | 1013 | 785 | 0.5030 | 0.4475 | 0.0495 | 0.0555 | 0.0164 | 0.0391 | [-0.0037, 0.0735] | inconclusive |
| sell | 1d | 30766 | 8 | 1259 | 0 | 971 | 268 | 0.4788 | 0.4586 | 0.0626 | 0.0202 | 0.0085 | 0.0117 | [-0.0526, 0.0748] | insufficient |

##### by cvd

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| with | 5m | 47669 | 0 | 0 | 0 | 1331 | 1331 | 0.4719 | 0.4852 | 0.0428 | -0.0133 | 0.0010 | -0.0143 | [-0.0269, -0.0052] | counter |
| with | 15m | 47669 | 0 | 0 | 0 | 1331 | 1331 | 0.4856 | 0.4920 | 0.0224 | -0.0064 | 0.0027 | -0.0091 | [-0.0288, 0.0042] | inconclusive |
| with | 1h | 47669 | 0 | 0 | 0 | 1331 | 1331 | 0.4916 | 0.4896 | 0.0188 | 0.0020 | 0.0064 | -0.0044 | [-0.0269, 0.0100] | inconclusive |
| with | 4h | 47565 | 0 | 104 | 0 | 1327 | 959 | 0.5051 | 0.4458 | 0.0490 | 0.0593 | 0.0060 | 0.0533 | [0.0210, 0.0774] | hypothesis |
| with | 1d | 46012 | 12 | 1645 | 0 | 1276 | 289 | 0.4797 | 0.4559 | 0.0644 | 0.0238 | 0.0008 | 0.0231 | [-0.0166, 0.0584] | insufficient |
| against | 5m | 13494 | 0 | 0 | 0 | 749 | 749 | 0.4599 | 0.4798 | 0.0602 | -0.0199 | 0.0035 | -0.0234 | [-0.0476, 0.0008] | inconclusive |
| against | 15m | 13494 | 0 | 0 | 0 | 749 | 749 | 0.4653 | 0.4884 | 0.0463 | -0.0230 | 0.0043 | -0.0274 | [-0.0632, 0.0016] | inconclusive |
| against | 1h | 13494 | 0 | 0 | 0 | 749 | 749 | 0.4727 | 0.4878 | 0.0394 | -0.0151 | 0.0073 | -0.0224 | [-0.0660, 0.0060] | inconclusive |
| against | 4h | 13468 | 0 | 26 | 0 | 748 | 609 | 0.4728 | 0.4522 | 0.0751 | 0.0206 | 0.0051 | 0.0155 | [-0.0495, 0.0623] | inconclusive |
| against | 1d | 13029 | 7 | 458 | 0 | 727 | 247 | 0.4875 | 0.4323 | 0.0803 | 0.0552 | 0.0067 | 0.0485 | [-0.0384, 0.1213] | insufficient |
| flat | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| flat | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P3 w=5m

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 2273 | 0 | 0 | 0 | 447 | 447 | 0.4839 | 0.4650 | 0.0510 | 0.0189 | 0.0058 | 0.0131 | [-0.0276, 0.0482] | inconclusive |
| all | 15m | 2273 | 0 | 0 | 0 | 447 | 440 | 0.4646 | 0.4932 | 0.0422 | -0.0286 | 0.0028 | -0.0314 | [-0.0602, -0.0049] | counter (block-sensitive (28d: inconclusive)) |
| all | 1h | 2273 | 0 | 0 | 0 | 447 | 399 | 0.4773 | 0.4910 | 0.0317 | -0.0136 | 0.0035 | -0.0171 | [-0.0775, 0.0340] | inconclusive |
| all | 4h | 2273 | 0 | 0 | 0 | 447 | 314 | 0.4897 | 0.4655 | 0.0449 | 0.0242 | 0.0133 | 0.0109 | [-0.0611, 0.0894] | insufficient |
| all | 1d | 2249 | 0 | 24 | 0 | 438 | 166 | 0.5082 | 0.4424 | 0.0494 | 0.0658 | 0.0169 | 0.0490 | [-0.0383, 0.1271] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 557 | 0 | 0 | 0 | 110 | 110 | 0.4704 | 0.4740 | 0.0557 | -0.0036 | 0.0183 | -0.0219 | [-0.0956, 0.0451] | insufficient |
| low | 15m | 557 | 0 | 0 | 0 | 110 | 109 | 0.4776 | 0.4955 | 0.0269 | -0.0180 | 0.0206 | -0.0385 | [-0.1574, 0.0610] | insufficient |
| low | 1h | 557 | 0 | 0 | 0 | 110 | 102 | 0.4506 | 0.5063 | 0.0431 | -0.0557 | 0.0356 | -0.0912 | [-0.1878, 0.0362] | insufficient |
| low | 4h | 557 | 0 | 0 | 0 | 110 | 84 | 0.4345 | 0.4722 | 0.0934 | -0.0377 | 0.0617 | -0.0994 | [-0.2516, 0.0785] | insufficient |
| low | 1d | 557 | 0 | 0 | 0 | 110 | 56 | 0.4022 | 0.5189 | 0.0790 | -0.1167 | 0.0161 | -0.1328 | [-0.2851, 0.0089] | insufficient |
| medium | 5m | 623 | 0 | 0 | 0 | 140 | 140 | 0.4575 | 0.4831 | 0.0594 | -0.0257 | -0.0012 | -0.0245 | [-0.1351, 0.0734] | insufficient |
| medium | 15m | 623 | 0 | 0 | 0 | 140 | 138 | 0.4398 | 0.5313 | 0.0289 | -0.0915 | -0.0121 | -0.0794 | [-0.1784, 0.0108] | insufficient |
| medium | 1h | 623 | 0 | 0 | 0 | 140 | 127 | 0.4302 | 0.5377 | 0.0321 | -0.1075 | 0.0203 | -0.1278 | [-0.2965, 0.0178] | insufficient |
| medium | 4h | 623 | 0 | 0 | 0 | 140 | 112 | 0.4735 | 0.5008 | 0.0257 | -0.0273 | -0.0034 | -0.0239 | [-0.1795, 0.1135] | insufficient |
| medium | 1d | 623 | 0 | 0 | 0 | 140 | 84 | 0.5104 | 0.4494 | 0.0401 | 0.0610 | -0.0199 | 0.0809 | [-0.0657, 0.2219] | insufficient |
| high | 5m | 641 | 0 | 0 | 0 | 118 | 118 | 0.5023 | 0.4587 | 0.0390 | 0.0437 | 0.0074 | 0.0363 | [-0.0449, 0.1224] | insufficient |
| high | 15m | 641 | 0 | 0 | 0 | 118 | 118 | 0.4696 | 0.4805 | 0.0499 | -0.0109 | 0.0111 | -0.0220 | [-0.1300, 0.0773] | insufficient |
| high | 1h | 641 | 0 | 0 | 0 | 118 | 103 | 0.5148 | 0.4602 | 0.0250 | 0.0546 | 0.0220 | 0.0326 | [-0.1109, 0.1470] | insufficient |
| high | 4h | 641 | 0 | 0 | 0 | 118 | 83 | 0.5429 | 0.4212 | 0.0359 | 0.1217 | 0.0242 | 0.0975 | [-0.0520, 0.2189] | insufficient |
| high | 1d | 629 | 0 | 12 | 0 | 113 | 54 | 0.5898 | 0.3816 | 0.0286 | 0.2083 | 0.0309 | 0.1774 | [-0.0279, 0.2798] | insufficient |
| extreme | 5m | 446 | 0 | 0 | 0 | 104 | 104 | 0.5112 | 0.4395 | 0.0493 | 0.0717 | 0.0039 | 0.0679 | [-0.0335, 0.1421] | insufficient |
| extreme | 15m | 446 | 0 | 0 | 0 | 104 | 100 | 0.4776 | 0.4619 | 0.0605 | 0.0157 | 0.0095 | 0.0062 | [-0.1270, 0.1222] | insufficient |
| extreme | 1h | 446 | 0 | 0 | 0 | 104 | 94 | 0.5157 | 0.4574 | 0.0269 | 0.0583 | 0.0104 | 0.0479 | [-0.1170, 0.2029] | insufficient |
| extreme | 4h | 446 | 0 | 0 | 0 | 104 | 74 | 0.4978 | 0.4776 | 0.0247 | 0.0202 | 0.0308 | -0.0106 | [-0.1666, 0.1512] | insufficient |
| extreme | 1d | 440 | 0 | 6 | 0 | 102 | 52 | 0.5227 | 0.4227 | 0.0545 | 0.1000 | -0.0419 | 0.1419 | [-0.0505, 0.3778] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 599 | 0 | 0 | 0 | 243 | 243 | 0.4775 | 0.4658 | 0.0568 | 0.0117 | 0.0093 | 0.0024 | [-0.0920, 0.0826] | insufficient |
| value_edge | 15m | 599 | 0 | 0 | 0 | 243 | 238 | 0.4658 | 0.4925 | 0.0417 | -0.0267 | 0.0005 | -0.0272 | [-0.1424, 0.0625] | insufficient |
| value_edge | 1h | 599 | 0 | 0 | 0 | 243 | 223 | 0.5075 | 0.4541 | 0.0384 | 0.0534 | 0.0053 | 0.0481 | [-0.0763, 0.1423] | insufficient |
| value_edge | 4h | 599 | 0 | 0 | 0 | 243 | 193 | 0.5125 | 0.4274 | 0.0601 | 0.0851 | 0.0273 | 0.0578 | [-0.0620, 0.1716] | insufficient |
| value_edge | 1d | 586 | 0 | 13 | 0 | 238 | 127 | 0.4590 | 0.4761 | 0.0648 | -0.0171 | 0.0161 | -0.0332 | [-0.1401, 0.0840] | insufficient |
| structural_extreme | 5m | 4 | 0 | 0 | 0 | 3 | 3 | 0.2500 | 0.7500 | 0.0000 | -0.5000 | -0.1234 | -0.3766 | [-1.0980, 0.9000] | insufficient (undefined 281) |
| structural_extreme | 15m | 4 | 0 | 0 | 0 | 3 | 3 | 0.5000 | 0.5000 | 0.0000 | 0.0000 | -0.2851 | 0.2851 | [-0.5432, 1.1451] | insufficient (undefined 281) |
| structural_extreme | 1h | 4 | 0 | 0 | 0 | 3 | 3 | 0.5000 | 0.5000 | 0.0000 | 0.0000 | -0.1479 | 0.1479 | [-0.8491, 1.2000] | insufficient (undefined 281) |
| structural_extreme | 4h | 4 | 0 | 0 | 0 | 3 | 3 | 0.5000 | 0.5000 | 0.0000 | 0.0000 | 0.0758 | -0.0758 | [-0.7123, 1.1225] | insufficient (undefined 281) |
| structural_extreme | 1d | 4 | 0 | 0 | 0 | 3 | 3 | 0.0000 | 0.5000 | 0.5000 | -0.5000 | -0.0828 | -0.4172 | [-1.1991, 0.2227] | insufficient (undefined 281) |
| neither | 5m | 1672 | 0 | 0 | 0 | 340 | 340 | 0.4862 | 0.4647 | 0.0490 | 0.0215 | 0.0045 | 0.0170 | [-0.0241, 0.0607] | insufficient |
| neither | 15m | 1672 | 0 | 0 | 0 | 340 | 334 | 0.4635 | 0.4940 | 0.0425 | -0.0305 | 0.0036 | -0.0341 | [-0.0729, 0.0132] | insufficient |
| neither | 1h | 1672 | 0 | 0 | 0 | 340 | 317 | 0.4659 | 0.5048 | 0.0293 | -0.0389 | 0.0029 | -0.0418 | [-0.1132, 0.0240] | insufficient |
| neither | 4h | 1672 | 0 | 0 | 0 | 340 | 260 | 0.4809 | 0.4797 | 0.0395 | 0.0012 | 0.0081 | -0.0069 | [-0.0932, 0.0900] | insufficient |
| neither | 1d | 1661 | 0 | 11 | 0 | 335 | 148 | 0.5262 | 0.4299 | 0.0439 | 0.0963 | 0.0174 | 0.0789 | [-0.0313, 0.1788] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

##### by side

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| buy | 5m | 981 | 0 | 0 | 0 | 196 | 196 | 0.5199 | 0.4424 | 0.0377 | 0.0775 | 0.0074 | 0.0700 | [-0.0018, 0.1429] | insufficient |
| buy | 15m | 981 | 0 | 0 | 0 | 196 | 195 | 0.4913 | 0.4720 | 0.0367 | 0.0194 | 0.0097 | 0.0097 | [-0.0726, 0.0890] | insufficient |
| buy | 1h | 981 | 0 | 0 | 0 | 196 | 176 | 0.4954 | 0.4750 | 0.0296 | 0.0204 | 0.0248 | -0.0044 | [-0.1076, 0.0899] | insufficient |
| buy | 4h | 981 | 0 | 0 | 0 | 196 | 146 | 0.5056 | 0.4322 | 0.0622 | 0.0734 | 0.0278 | 0.0456 | [-0.0441, 0.1251] | insufficient |
| buy | 1d | 967 | 0 | 14 | 0 | 191 | 103 | 0.5016 | 0.4509 | 0.0476 | 0.0507 | 0.0125 | 0.0381 | [-0.1000, 0.1961] | insufficient |
| sell | 5m | 1292 | 0 | 0 | 0 | 251 | 251 | 0.4567 | 0.4822 | 0.0611 | -0.0255 | 0.0045 | -0.0301 | [-0.1225, 0.0445] | insufficient |
| sell | 15m | 1292 | 0 | 0 | 0 | 251 | 247 | 0.4443 | 0.5093 | 0.0464 | -0.0650 | -0.0025 | -0.0626 | [-0.1571, 0.0140] | insufficient |
| sell | 1h | 1292 | 0 | 0 | 0 | 251 | 227 | 0.4636 | 0.5031 | 0.0333 | -0.0395 | -0.0127 | -0.0268 | [-0.1305, 0.0608] | insufficient |
| sell | 4h | 1292 | 0 | 0 | 0 | 251 | 184 | 0.4776 | 0.4907 | 0.0317 | -0.0132 | 0.0023 | -0.0155 | [-0.1333, 0.1042] | insufficient |
| sell | 1d | 1282 | 0 | 10 | 0 | 247 | 119 | 0.5133 | 0.4360 | 0.0507 | 0.0772 | 0.0201 | 0.0571 | [-0.0575, 0.1611] | insufficient |

#### P3 w=15m

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 2006 | 0 | 0 | 0 | 294 | 294 | 0.4751 | 0.4811 | 0.0439 | -0.0060 | 0.0031 | -0.0091 | [-0.0756, 0.0565] | insufficient |
| all | 15m | 2006 | 0 | 0 | 0 | 294 | 294 | 0.4925 | 0.4831 | 0.0244 | 0.0095 | 0.0006 | 0.0089 | [-0.0728, 0.0794] | insufficient |
| all | 1h | 2006 | 0 | 0 | 0 | 294 | 277 | 0.4970 | 0.4865 | 0.0165 | 0.0105 | -0.0051 | 0.0156 | [-0.0858, 0.1093] | insufficient |
| all | 4h | 2006 | 0 | 0 | 0 | 294 | 232 | 0.5229 | 0.4467 | 0.0304 | 0.0763 | 0.0042 | 0.0721 | [-0.0148, 0.1750] | insufficient |
| all | 1d | 1974 | 0 | 32 | 0 | 289 | 147 | 0.5466 | 0.4215 | 0.0319 | 0.1251 | 0.0136 | 0.1115 | [-0.0047, 0.2350] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 507 | 0 | 0 | 0 | 78 | 78 | 0.4832 | 0.4694 | 0.0473 | 0.0138 | 0.0007 | 0.0131 | [-0.0766, 0.1331] | insufficient |
| low | 15m | 507 | 0 | 0 | 0 | 78 | 78 | 0.5069 | 0.4714 | 0.0217 | 0.0355 | -0.0142 | 0.0497 | [-0.0631, 0.1569] | insufficient |
| low | 1h | 507 | 0 | 0 | 0 | 78 | 74 | 0.5069 | 0.4911 | 0.0020 | 0.0158 | -0.0223 | 0.0381 | [-0.1405, 0.1934] | insufficient |
| low | 4h | 507 | 0 | 0 | 0 | 78 | 63 | 0.4655 | 0.4655 | 0.0690 | 0.0000 | 0.0153 | -0.0153 | [-0.1858, 0.1833] | insufficient |
| low | 1d | 507 | 0 | 0 | 0 | 78 | 52 | 0.5030 | 0.4320 | 0.0651 | 0.0710 | -0.0089 | 0.0799 | [-0.1075, 0.2288] | insufficient |
| medium | 5m | 480 | 0 | 0 | 0 | 84 | 84 | 0.4458 | 0.5167 | 0.0375 | -0.0708 | -0.0046 | -0.0663 | [-0.1909, 0.0824] | insufficient |
| medium | 15m | 480 | 0 | 0 | 0 | 84 | 84 | 0.4646 | 0.5250 | 0.0104 | -0.0604 | -0.0098 | -0.0506 | [-0.1830, 0.0678] | insufficient |
| medium | 1h | 480 | 0 | 0 | 0 | 84 | 82 | 0.4750 | 0.5188 | 0.0063 | -0.0437 | -0.0014 | -0.0423 | [-0.1596, 0.1055] | insufficient |
| medium | 4h | 480 | 0 | 0 | 0 | 84 | 77 | 0.5208 | 0.4771 | 0.0021 | 0.0437 | -0.0037 | 0.0474 | [-0.0900, 0.1603] | insufficient |
| medium | 1d | 479 | 0 | 1 | 0 | 83 | 64 | 0.4739 | 0.5031 | 0.0230 | -0.0292 | -0.0346 | 0.0054 | [-0.1631, 0.1742] | insufficient |
| high | 5m | 585 | 0 | 0 | 0 | 82 | 82 | 0.4957 | 0.4547 | 0.0496 | 0.0410 | 0.0029 | 0.0381 | [-0.0508, 0.1276] | insufficient |
| high | 15m | 585 | 0 | 0 | 0 | 82 | 82 | 0.4974 | 0.4581 | 0.0444 | 0.0393 | -0.0122 | 0.0515 | [-0.0691, 0.1682] | insufficient |
| high | 1h | 585 | 0 | 0 | 0 | 82 | 75 | 0.5299 | 0.4342 | 0.0359 | 0.0957 | -0.0367 | 0.1324 | [-0.0194, 0.2493] | insufficient |
| high | 4h | 585 | 0 | 0 | 0 | 82 | 63 | 0.6051 | 0.3521 | 0.0427 | 0.2530 | -0.0236 | 0.2766 | [0.0764, 0.4571] | insufficient |
| high | 1d | 570 | 0 | 15 | 0 | 79 | 41 | 0.6895 | 0.2947 | 0.0158 | 0.3947 | 0.0444 | 0.3503 | [0.0975, 0.5083] | insufficient |
| extreme | 5m | 432 | 0 | 0 | 0 | 66 | 66 | 0.4699 | 0.4907 | 0.0394 | -0.0208 | -0.0049 | -0.0159 | [-0.1524, 0.0855] | insufficient |
| extreme | 15m | 432 | 0 | 0 | 0 | 66 | 66 | 0.5000 | 0.4838 | 0.0162 | 0.0162 | -0.0026 | 0.0188 | [-0.1877, 0.1804] | insufficient |
| extreme | 1h | 432 | 0 | 0 | 0 | 66 | 63 | 0.4676 | 0.5139 | 0.0185 | -0.0463 | -0.0122 | -0.0341 | [-0.2364, 0.1584] | insufficient |
| extreme | 4h | 432 | 0 | 0 | 0 | 66 | 53 | 0.4838 | 0.5162 | 0.0000 | -0.0324 | 0.0107 | -0.0431 | [-0.2152, 0.1633] | insufficient |
| extreme | 1d | 418 | 0 | 14 | 0 | 65 | 39 | 0.4880 | 0.4880 | 0.0239 | 0.0000 | -0.0553 | 0.0553 | [-0.1165, 0.3768] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 533 | 0 | 0 | 0 | 186 | 186 | 0.4747 | 0.4916 | 0.0338 | -0.0169 | 0.0084 | -0.0253 | [-0.1268, 0.0597] | insufficient |
| value_edge | 15m | 533 | 0 | 0 | 0 | 186 | 186 | 0.4653 | 0.5253 | 0.0094 | -0.0600 | -0.0004 | -0.0596 | [-0.2012, 0.0543] | insufficient |
| value_edge | 1h | 533 | 0 | 0 | 0 | 186 | 178 | 0.4465 | 0.5441 | 0.0094 | -0.0976 | -0.0202 | -0.0773 | [-0.2408, 0.0448] | insufficient |
| value_edge | 4h | 533 | 0 | 0 | 0 | 186 | 158 | 0.5197 | 0.4334 | 0.0469 | 0.0863 | 0.0059 | 0.0804 | [-0.0580, 0.1939] | insufficient |
| value_edge | 1d | 523 | 0 | 10 | 0 | 181 | 115 | 0.5277 | 0.4073 | 0.0650 | 0.1205 | 0.0203 | 0.1002 | [-0.0330, 0.2209] | insufficient |
| structural_extreme | 5m | 5 | 0 | 0 | 0 | 3 | 3 | 0.6000 | 0.4000 | 0.0000 | 0.2000 | -0.1015 | 0.3015 | [0.0874, 1.0000] | insufficient (undefined 945) |
| structural_extreme | 15m | 5 | 0 | 0 | 0 | 3 | 3 | 0.6000 | 0.4000 | 0.0000 | 0.2000 | -0.1128 | 0.3128 | [0.0000, 1.0526] | insufficient (undefined 945) |
| structural_extreme | 1h | 5 | 0 | 0 | 0 | 3 | 3 | 0.6000 | 0.4000 | 0.0000 | 0.2000 | 0.1116 | 0.0884 | [-0.1680, 0.9231] | insufficient (undefined 945) |
| structural_extreme | 4h | 5 | 0 | 0 | 0 | 3 | 3 | 0.6000 | 0.4000 | 0.0000 | 0.2000 | 0.2534 | -0.0534 | [-0.3465, 1.1111] | insufficient (undefined 945) |
| structural_extreme | 1d | 5 | 0 | 0 | 0 | 3 | 2 | 0.2000 | 0.4000 | 0.4000 | -0.2000 | -0.1525 | -0.0475 | [-0.4987, 1.5789] | insufficient (undefined 945) |
| neither | 5m | 1470 | 0 | 0 | 0 | 244 | 244 | 0.4741 | 0.4782 | 0.0476 | -0.0041 | 0.0011 | -0.0052 | [-0.0730, 0.0691] | insufficient |
| neither | 15m | 1470 | 0 | 0 | 0 | 244 | 244 | 0.5014 | 0.4687 | 0.0299 | 0.0327 | 0.0005 | 0.0321 | [-0.0456, 0.1069] | insufficient |
| neither | 1h | 1470 | 0 | 0 | 0 | 244 | 234 | 0.5143 | 0.4667 | 0.0190 | 0.0476 | -0.0004 | 0.0480 | [-0.0452, 0.1470] | insufficient |
| neither | 4h | 1470 | 0 | 0 | 0 | 244 | 197 | 0.5231 | 0.4524 | 0.0245 | 0.0707 | 0.0027 | 0.0680 | [-0.0223, 0.1877] | insufficient |
| neither | 1d | 1448 | 0 | 22 | 0 | 239 | 128 | 0.5539 | 0.4261 | 0.0200 | 0.1278 | 0.0119 | 0.1159 | [-0.0274, 0.2769] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

##### by side

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| buy | 5m | 883 | 0 | 0 | 0 | 135 | 135 | 0.4802 | 0.4813 | 0.0385 | -0.0011 | 0.0035 | -0.0046 | [-0.0672, 0.0509] | insufficient |
| buy | 15m | 883 | 0 | 0 | 0 | 135 | 135 | 0.4892 | 0.4768 | 0.0340 | 0.0125 | 0.0053 | 0.0072 | [-0.0592, 0.0646] | insufficient |
| buy | 1h | 883 | 0 | 0 | 0 | 135 | 128 | 0.4983 | 0.4836 | 0.0181 | 0.0147 | 0.0140 | 0.0008 | [-0.1604, 0.1295] | insufficient |
| buy | 4h | 883 | 0 | 0 | 0 | 135 | 112 | 0.5504 | 0.3986 | 0.0510 | 0.1518 | 0.0138 | 0.1380 | [0.0233, 0.2622] | insufficient |
| buy | 1d | 869 | 0 | 14 | 0 | 134 | 90 | 0.5316 | 0.4304 | 0.0380 | 0.1013 | 0.0209 | 0.0803 | [-0.0443, 0.2464] | insufficient |
| sell | 5m | 1123 | 0 | 0 | 0 | 161 | 161 | 0.4711 | 0.4809 | 0.0481 | -0.0098 | 0.0028 | -0.0126 | [-0.1380, 0.1018] | insufficient |
| sell | 15m | 1123 | 0 | 0 | 0 | 161 | 161 | 0.4951 | 0.4880 | 0.0169 | 0.0071 | -0.0032 | 0.0103 | [-0.1132, 0.1241] | insufficient |
| sell | 1h | 1123 | 0 | 0 | 0 | 161 | 153 | 0.4960 | 0.4889 | 0.0151 | 0.0071 | -0.0201 | 0.0273 | [-0.1138, 0.1676] | insufficient |
| sell | 4h | 1123 | 0 | 0 | 0 | 161 | 138 | 0.5013 | 0.4844 | 0.0142 | 0.0169 | -0.0033 | 0.0202 | [-0.1492, 0.1918] | insufficient |
| sell | 1d | 1105 | 0 | 18 | 0 | 157 | 102 | 0.5584 | 0.4145 | 0.0271 | 0.1439 | 0.0079 | 0.1360 | [-0.0422, 0.3094] | insufficient |

#### P3 w=1h

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 3653 | 0 | 0 | 0 | 211 | 211 | 0.4692 | 0.4810 | 0.0498 | -0.0118 | 0.0012 | -0.0130 | [-0.0537, 0.0265] | insufficient |
| all | 15m | 3653 | 0 | 0 | 0 | 211 | 211 | 0.4771 | 0.4947 | 0.0282 | -0.0175 | 0.0003 | -0.0178 | [-0.0776, 0.0365] | insufficient |
| all | 1h | 3653 | 0 | 0 | 0 | 211 | 211 | 0.4914 | 0.4796 | 0.0290 | 0.0118 | -0.0016 | 0.0134 | [-0.0657, 0.0838] | insufficient |
| all | 4h | 3653 | 0 | 0 | 0 | 211 | 189 | 0.4730 | 0.4812 | 0.0457 | -0.0082 | 0.0034 | -0.0116 | [-0.0755, 0.0512] | insufficient |
| all | 1d | 3552 | 0 | 101 | 0 | 205 | 128 | 0.5124 | 0.4299 | 0.0577 | 0.0825 | -0.0039 | 0.0864 | [-0.0631, 0.2148] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 1024 | 0 | 0 | 0 | 58 | 58 | 0.4717 | 0.4658 | 0.0625 | 0.0059 | 0.0173 | -0.0114 | [-0.0709, 0.0448] | insufficient |
| low | 15m | 1024 | 0 | 0 | 0 | 58 | 58 | 0.4932 | 0.4941 | 0.0127 | -0.0010 | 0.0142 | -0.0152 | [-0.1094, 0.0719] | insufficient |
| low | 1h | 1024 | 0 | 0 | 0 | 58 | 58 | 0.5127 | 0.4580 | 0.0293 | 0.0547 | 0.0228 | 0.0319 | [-0.0887, 0.1464] | insufficient |
| low | 4h | 1024 | 0 | 0 | 0 | 58 | 55 | 0.3838 | 0.4971 | 0.1191 | -0.1133 | 0.0498 | -0.1631 | [-0.3184, -0.0024] | insufficient |
| low | 1d | 1024 | 0 | 0 | 0 | 58 | 45 | 0.4453 | 0.4473 | 0.1074 | -0.0020 | 0.0377 | -0.0397 | [-0.3284, 0.2330] | insufficient |
| medium | 5m | 832 | 0 | 0 | 0 | 58 | 58 | 0.4796 | 0.4880 | 0.0325 | -0.0084 | -0.0067 | -0.0017 | [-0.0590, 0.0485] | insufficient |
| medium | 15m | 832 | 0 | 0 | 0 | 58 | 58 | 0.4760 | 0.5144 | 0.0096 | -0.0385 | -0.0163 | -0.0222 | [-0.1247, 0.0681] | insufficient |
| medium | 1h | 832 | 0 | 0 | 0 | 58 | 58 | 0.4784 | 0.5216 | 0.0000 | -0.0433 | -0.0056 | -0.0376 | [-0.1578, 0.0817] | insufficient |
| medium | 4h | 832 | 0 | 0 | 0 | 58 | 57 | 0.4832 | 0.5156 | 0.0012 | -0.0325 | -0.0085 | -0.0239 | [-0.2765, 0.2151] | insufficient |
| medium | 1d | 832 | 0 | 0 | 0 | 58 | 49 | 0.5877 | 0.3810 | 0.0312 | 0.2067 | 0.0031 | 0.2036 | [-0.0780, 0.3846] | insufficient |
| high | 5m | 993 | 0 | 0 | 0 | 57 | 57 | 0.4864 | 0.4773 | 0.0363 | 0.0091 | 0.0012 | 0.0079 | [-0.0638, 0.0494] | insufficient |
| high | 15m | 993 | 0 | 0 | 0 | 57 | 57 | 0.4985 | 0.4552 | 0.0463 | 0.0433 | 0.0088 | 0.0345 | [-0.0636, 0.0925] | insufficient |
| high | 1h | 993 | 0 | 0 | 0 | 57 | 57 | 0.5680 | 0.3988 | 0.0332 | 0.1692 | 0.0376 | 0.1316 | [0.0184, 0.2064] | insufficient |
| high | 4h | 993 | 0 | 0 | 0 | 57 | 51 | 0.5750 | 0.3837 | 0.0413 | 0.1913 | 0.0125 | 0.1789 | [0.0403, 0.3277] | insufficient |
| high | 1d | 918 | 0 | 75 | 0 | 54 | 40 | 0.6340 | 0.3279 | 0.0381 | 0.3061 | 0.0125 | 0.2936 | [0.0651, 0.4891] | insufficient |
| extreme | 5m | 795 | 0 | 0 | 0 | 57 | 57 | 0.4352 | 0.4969 | 0.0679 | -0.0616 | -0.0123 | -0.0494 | [-0.1320, 0.0513] | insufficient |
| extreme | 15m | 795 | 0 | 0 | 0 | 57 | 57 | 0.4352 | 0.5195 | 0.0453 | -0.0843 | -0.0080 | -0.0763 | [-0.1556, 0.0297] | insufficient |
| extreme | 1h | 795 | 0 | 0 | 0 | 57 | 57 | 0.3874 | 0.5585 | 0.0541 | -0.1711 | -0.0245 | -0.1466 | [-0.2524, 0.0670] | insufficient |
| extreme | 4h | 795 | 0 | 0 | 0 | 57 | 53 | 0.4553 | 0.5409 | 0.0038 | -0.0855 | 0.0051 | -0.0906 | [-0.2274, 0.1279] | insufficient |
| extreme | 1d | 778 | 0 | 17 | 0 | 56 | 42 | 0.3766 | 0.5797 | 0.0437 | -0.2031 | -0.0435 | -0.1596 | [-0.3354, 0.1859] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 970 | 0 | 0 | 0 | 173 | 173 | 0.4722 | 0.4835 | 0.0443 | -0.0113 | -0.0013 | -0.0101 | [-0.0640, 0.0271] | insufficient |
| value_edge | 15m | 970 | 0 | 0 | 0 | 173 | 173 | 0.4938 | 0.4835 | 0.0227 | 0.0103 | -0.0055 | 0.0159 | [-0.0964, 0.0931] | insufficient |
| value_edge | 1h | 970 | 0 | 0 | 0 | 173 | 173 | 0.5247 | 0.4402 | 0.0351 | 0.0845 | -0.0165 | 0.1010 | [-0.0318, 0.1787] | insufficient |
| value_edge | 4h | 970 | 0 | 0 | 0 | 173 | 161 | 0.4588 | 0.4557 | 0.0856 | 0.0031 | 0.0124 | -0.0093 | [-0.1697, 0.1307] | insufficient |
| value_edge | 1d | 943 | 0 | 27 | 0 | 167 | 114 | 0.4666 | 0.4422 | 0.0912 | 0.0244 | -0.0300 | 0.0544 | [-0.0902, 0.1907] | insufficient |
| structural_extreme | 5m | 7 | 0 | 0 | 0 | 6 | 6 | 0.2857 | 0.7143 | 0.0000 | -0.4286 | 0.0939 | -0.5225 | [-0.9758, 0.2299] | insufficient |
| structural_extreme | 15m | 7 | 0 | 0 | 0 | 6 | 6 | 0.2857 | 0.7143 | 0.0000 | -0.4286 | 0.0987 | -0.5273 | [-0.9693, 0.2296] | insufficient |
| structural_extreme | 1h | 7 | 0 | 0 | 0 | 6 | 6 | 0.2857 | 0.7143 | 0.0000 | -0.4286 | -0.0098 | -0.4187 | [-1.0688, 0.3841] | insufficient |
| structural_extreme | 4h | 7 | 0 | 0 | 0 | 6 | 6 | 0.4286 | 0.5714 | 0.0000 | -0.1429 | 0.2606 | -0.4035 | [-0.9651, 0.3333] | insufficient |
| structural_extreme | 1d | 7 | 0 | 0 | 0 | 6 | 6 | 0.2857 | 0.7143 | 0.0000 | -0.4286 | -0.0686 | -0.3600 | [-1.0846, 0.3465] | insufficient |
| neither | 5m | 2677 | 0 | 0 | 0 | 190 | 190 | 0.4684 | 0.4796 | 0.0519 | -0.0112 | 0.0019 | -0.0131 | [-0.0609, 0.0358] | insufficient |
| neither | 15m | 2677 | 0 | 0 | 0 | 190 | 190 | 0.4714 | 0.4983 | 0.0303 | -0.0269 | 0.0022 | -0.0291 | [-0.0896, 0.0296] | insufficient |
| neither | 1h | 2677 | 0 | 0 | 0 | 190 | 190 | 0.4796 | 0.4935 | 0.0269 | -0.0138 | 0.0034 | -0.0173 | [-0.0972, 0.0626] | insufficient |
| neither | 4h | 2677 | 0 | 0 | 0 | 190 | 172 | 0.4785 | 0.4901 | 0.0314 | -0.0116 | -0.0002 | -0.0114 | [-0.0880, 0.0674] | insufficient |
| neither | 1d | 2603 | 0 | 74 | 0 | 184 | 120 | 0.5298 | 0.4245 | 0.0457 | 0.1053 | 0.0058 | 0.0995 | [-0.0796, 0.2517] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

##### by side

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| buy | 5m | 1469 | 0 | 0 | 0 | 91 | 91 | 0.4690 | 0.4847 | 0.0463 | -0.0157 | -0.0032 | -0.0124 | [-0.0563, 0.0292] | insufficient |
| buy | 15m | 1469 | 0 | 0 | 0 | 91 | 91 | 0.4854 | 0.4758 | 0.0388 | 0.0095 | -0.0017 | 0.0113 | [-0.0781, 0.0950] | insufficient |
| buy | 1h | 1469 | 0 | 0 | 0 | 91 | 91 | 0.4922 | 0.4656 | 0.0422 | 0.0265 | 0.0152 | 0.0113 | [-0.1043, 0.1048] | insufficient |
| buy | 4h | 1469 | 0 | 0 | 0 | 91 | 85 | 0.4622 | 0.4935 | 0.0442 | -0.0313 | 0.0122 | -0.0435 | [-0.2150, 0.0914] | insufficient |
| buy | 1d | 1446 | 0 | 23 | 0 | 89 | 73 | 0.4675 | 0.4371 | 0.0954 | 0.0304 | 0.0020 | 0.0284 | [-0.1689, 0.2310] | insufficient |
| sell | 5m | 2184 | 0 | 0 | 0 | 124 | 124 | 0.4693 | 0.4785 | 0.0522 | -0.0092 | 0.0042 | -0.0134 | [-0.0758, 0.0481] | insufficient |
| sell | 15m | 2184 | 0 | 0 | 0 | 124 | 124 | 0.4716 | 0.5073 | 0.0211 | -0.0357 | 0.0017 | -0.0374 | [-0.1134, 0.0365] | insufficient |
| sell | 1h | 2184 | 0 | 0 | 0 | 124 | 124 | 0.4908 | 0.4890 | 0.0201 | 0.0018 | -0.0129 | 0.0148 | [-0.0977, 0.1036] | insufficient |
| sell | 4h | 2184 | 0 | 0 | 0 | 124 | 114 | 0.4803 | 0.4730 | 0.0467 | 0.0073 | -0.0025 | 0.0099 | [-0.1203, 0.1204] | insufficient |
| sell | 1d | 2106 | 0 | 78 | 0 | 120 | 85 | 0.5432 | 0.4250 | 0.0318 | 0.1182 | -0.0079 | 0.1262 | [-0.0317, 0.2661] | insufficient |

#### P4 trigger.failed_auction@1

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 697 | 0 | 0 | 0 | 697 | 681 | 0.4749 | 0.4792 | 0.0459 | -0.0043 | 0.0037 | -0.0080 | [-0.0600, 0.0373] | inconclusive |
| all | 15m | 697 | 0 | 0 | 0 | 697 | 626 | 0.4577 | 0.4835 | 0.0588 | -0.0258 | 0.0093 | -0.0351 | [-0.0897, 0.0213] | inconclusive |
| all | 1h | 697 | 0 | 0 | 0 | 697 | 468 | 0.4950 | 0.4476 | 0.0574 | 0.0473 | 0.0074 | 0.0399 | [-0.0317, 0.1008] | inconclusive |
| all | 4h | 697 | 0 | 0 | 0 | 697 | 342 | 0.5093 | 0.4132 | 0.0775 | 0.0961 | 0.0154 | 0.0807 | [0.0093, 0.1486] | insufficient |
| all | 1d | 682 | 0 | 15 | 0 | 682 | 178 | 0.4941 | 0.4282 | 0.0777 | 0.0660 | 0.0106 | 0.0553 | [-0.0278, 0.1299] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 167 | 0 | 0 | 0 | 167 | 165 | 0.4551 | 0.4671 | 0.0778 | -0.0120 | 0.0415 | -0.0535 | [-0.1600, 0.0689] | insufficient |
| low | 15m | 167 | 0 | 0 | 0 | 167 | 155 | 0.5090 | 0.4611 | 0.0299 | 0.0479 | 0.0543 | -0.0064 | [-0.1256, 0.1443] | insufficient |
| low | 1h | 167 | 0 | 0 | 0 | 167 | 128 | 0.5808 | 0.3593 | 0.0599 | 0.2216 | 0.0944 | 0.1271 | [-0.0320, 0.2543] | insufficient |
| low | 4h | 167 | 0 | 0 | 0 | 167 | 99 | 0.4491 | 0.4371 | 0.1138 | 0.0120 | 0.0207 | -0.0087 | [-0.1736, 0.0847] | insufficient |
| low | 1d | 167 | 0 | 0 | 0 | 167 | 63 | 0.3234 | 0.5329 | 0.1437 | -0.2096 | -0.0264 | -0.1832 | [-0.4688, 0.0668] | insufficient |
| medium | 5m | 182 | 0 | 0 | 0 | 182 | 178 | 0.4945 | 0.4670 | 0.0385 | 0.0275 | -0.0054 | 0.0329 | [-0.0990, 0.1426] | insufficient |
| medium | 15m | 182 | 0 | 0 | 0 | 182 | 170 | 0.4670 | 0.4835 | 0.0495 | -0.0165 | -0.0079 | -0.0086 | [-0.1644, 0.1469] | insufficient |
| medium | 1h | 182 | 0 | 0 | 0 | 182 | 132 | 0.5055 | 0.4560 | 0.0385 | 0.0495 | -0.0052 | 0.0547 | [-0.0715, 0.1743] | insufficient |
| medium | 4h | 182 | 0 | 0 | 0 | 182 | 103 | 0.5165 | 0.4396 | 0.0440 | 0.0769 | 0.0112 | 0.0658 | [-0.0576, 0.1509] | insufficient |
| medium | 1d | 181 | 0 | 1 | 0 | 181 | 75 | 0.5138 | 0.4254 | 0.0608 | 0.0884 | -0.0078 | 0.0962 | [-0.0547, 0.2364] | insufficient |
| high | 5m | 153 | 0 | 0 | 0 | 153 | 149 | 0.5163 | 0.4641 | 0.0196 | 0.0523 | 0.0254 | 0.0269 | [-0.1229, 0.1328] | insufficient |
| high | 15m | 153 | 0 | 0 | 0 | 153 | 137 | 0.4902 | 0.4379 | 0.0719 | 0.0523 | 0.0633 | -0.0110 | [-0.1568, 0.0707] | insufficient |
| high | 1h | 153 | 0 | 0 | 0 | 153 | 107 | 0.5163 | 0.4314 | 0.0523 | 0.0850 | 0.0653 | 0.0196 | [-0.1459, 0.1000] | insufficient |
| high | 4h | 153 | 0 | 0 | 0 | 153 | 83 | 0.5294 | 0.4183 | 0.0523 | 0.1111 | 0.0988 | 0.0123 | [-0.1123, 0.1072] | insufficient |
| high | 1d | 151 | 0 | 2 | 0 | 151 | 59 | 0.6093 | 0.3377 | 0.0530 | 0.2715 | 0.0684 | 0.2031 | [-0.0132, 0.2721] | insufficient |
| extreme | 5m | 187 | 0 | 0 | 0 | 187 | 181 | 0.4439 | 0.5134 | 0.0428 | -0.0695 | 0.0165 | -0.0860 | [-0.2266, 0.0482] | insufficient |
| extreme | 15m | 187 | 0 | 0 | 0 | 187 | 157 | 0.3904 | 0.5294 | 0.0802 | -0.1390 | 0.0242 | -0.1633 | [-0.2796, -0.0259] | insufficient |
| extreme | 1h | 187 | 0 | 0 | 0 | 187 | 105 | 0.4064 | 0.5294 | 0.0642 | -0.1230 | 0.0249 | -0.1479 | [-0.3110, -0.0371] | insufficient |
| extreme | 4h | 187 | 0 | 0 | 0 | 187 | 81 | 0.5561 | 0.3636 | 0.0802 | 0.1925 | 0.1354 | 0.0572 | [-0.0741, 0.1388] | insufficient |
| extreme | 1d | 183 | 0 | 4 | 0 | 183 | 55 | 0.5355 | 0.4098 | 0.0546 | 0.1257 | 0.0804 | 0.0452 | [-0.1109, 0.2293] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 697 | 0 | 0 | 0 | 697 | 681 | 0.4749 | 0.4792 | 0.0459 | -0.0043 | 0.0037 | -0.0080 | [-0.0600, 0.0373] | inconclusive |
| value_edge | 15m | 697 | 0 | 0 | 0 | 697 | 626 | 0.4577 | 0.4835 | 0.0588 | -0.0258 | 0.0093 | -0.0351 | [-0.0897, 0.0213] | inconclusive |
| value_edge | 1h | 697 | 0 | 0 | 0 | 697 | 468 | 0.4950 | 0.4476 | 0.0574 | 0.0473 | 0.0074 | 0.0399 | [-0.0317, 0.1008] | inconclusive |
| value_edge | 4h | 697 | 0 | 0 | 0 | 697 | 342 | 0.5093 | 0.4132 | 0.0775 | 0.0961 | 0.0154 | 0.0807 | [0.0093, 0.1486] | insufficient |
| value_edge | 1d | 682 | 0 | 15 | 0 | 682 | 178 | 0.4941 | 0.4282 | 0.0777 | 0.0660 | 0.0106 | 0.0553 | [-0.0278, 0.1299] | insufficient |
| structural_extreme | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| structural_extreme | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| structural_extreme | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| structural_extreme | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| structural_extreme | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| neither | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| neither | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| neither | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| neither | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| neither | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P4 trigger.sfp.15m@1

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 4265 | 0 | 0 | 2 | 3313 | 3313 | 0.4858 | 0.4797 | 0.0345 | 0.0061 | -0.0005 | 0.0066 | [-0.0523, 0.0665] | inconclusive |
| all | 15m | 4265 | 0 | 0 | 2 | 3313 | 3205 | 0.4710 | 0.4910 | 0.0380 | -0.0199 | 0.0002 | -0.0201 | [-0.0743, 0.0422] | inconclusive |
| all | 1h | 4264 | 1 | 0 | 2 | 3312 | 2612 | 0.4449 | 0.4838 | 0.0713 | -0.0389 | -0.0037 | -0.0353 | [-0.0861, 0.0183] | inconclusive |
| all | 4h | 4253 | 1 | 11 | 2 | 3302 | 1375 | 0.4326 | 0.4623 | 0.1051 | -0.0296 | -0.0070 | -0.0226 | [-0.0588, 0.0127] | inconclusive |
| all | 1d | 4104 | 1 | 160 | 2 | 3180 | 321 | 0.4457 | 0.4537 | 0.1006 | -0.0080 | -0.0044 | -0.0036 | [-0.0343, 0.0299] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 1318 | 0 | 0 | 1 | 996 | 996 | 0.4901 | 0.4697 | 0.0402 | 0.0205 | -0.0041 | 0.0245 | [-0.0370, 0.0908] | inconclusive |
| low | 15m | 1318 | 0 | 0 | 1 | 996 | 961 | 0.4863 | 0.4788 | 0.0349 | 0.0076 | -0.0021 | 0.0097 | [-0.0642, 0.1007] | inconclusive |
| low | 1h | 1318 | 0 | 0 | 1 | 996 | 795 | 0.4461 | 0.4643 | 0.0895 | -0.0182 | -0.0023 | -0.0159 | [-0.0609, 0.0480] | inconclusive |
| low | 4h | 1318 | 0 | 0 | 1 | 996 | 449 | 0.4044 | 0.3983 | 0.1973 | 0.0061 | 0.0012 | 0.0048 | [-0.0379, 0.0597] | inconclusive |
| low | 1d | 1317 | 0 | 1 | 1 | 995 | 142 | 0.4184 | 0.4009 | 0.1807 | 0.0175 | -0.0065 | 0.0239 | [-0.0446, 0.1056] | insufficient |
| medium | 5m | 1264 | 0 | 0 | 1 | 973 | 973 | 0.4873 | 0.4842 | 0.0285 | 0.0032 | 0.0016 | 0.0016 | [-0.0653, 0.0861] | inconclusive |
| medium | 15m | 1264 | 0 | 0 | 1 | 973 | 950 | 0.4565 | 0.5032 | 0.0403 | -0.0467 | -0.0001 | -0.0465 | [-0.1120, 0.0262] | inconclusive |
| medium | 1h | 1264 | 0 | 0 | 1 | 973 | 781 | 0.4351 | 0.5047 | 0.0601 | -0.0696 | -0.0162 | -0.0534 | [-0.1288, 0.0270] | inconclusive |
| medium | 4h | 1264 | 0 | 0 | 1 | 973 | 465 | 0.4169 | 0.5166 | 0.0665 | -0.0997 | -0.0299 | -0.0697 | [-0.1094, -0.0245] | counter |
| medium | 1d | 1252 | 0 | 12 | 1 | 963 | 189 | 0.4601 | 0.4776 | 0.0623 | -0.0176 | -0.0063 | -0.0112 | [-0.0569, 0.0367] | insufficient |
| high | 5m | 860 | 0 | 0 | 2 | 680 | 680 | 0.4930 | 0.4837 | 0.0233 | 0.0093 | 0.0048 | 0.0045 | [-0.0777, 0.0815] | inconclusive |
| high | 15m | 860 | 0 | 0 | 2 | 680 | 662 | 0.4826 | 0.4849 | 0.0326 | -0.0023 | 0.0080 | -0.0103 | [-0.0744, 0.0771] | inconclusive |
| high | 1h | 859 | 1 | 0 | 2 | 679 | 554 | 0.4563 | 0.4773 | 0.0664 | -0.0210 | 0.0019 | -0.0228 | [-0.1045, 0.0768] | inconclusive |
| high | 4h | 859 | 1 | 0 | 2 | 679 | 352 | 0.4610 | 0.4726 | 0.0664 | -0.0116 | 0.0149 | -0.0266 | [-0.0768, 0.0356] | insufficient |
| high | 1d | 826 | 1 | 33 | 2 | 653 | 155 | 0.4782 | 0.4443 | 0.0775 | 0.0339 | 0.0006 | 0.0333 | [-0.0105, 0.0780] | insufficient |
| extreme | 5m | 736 | 0 | 0 | 0 | 602 | 602 | 0.4674 | 0.4837 | 0.0489 | -0.0163 | -0.0041 | -0.0122 | [-0.1097, 0.1040] | inconclusive |
| extreme | 15m | 736 | 0 | 0 | 0 | 602 | 585 | 0.4565 | 0.4973 | 0.0462 | -0.0408 | -0.0088 | -0.0320 | [-0.1090, 0.0660] | inconclusive |
| extreme | 1h | 736 | 0 | 0 | 0 | 602 | 502 | 0.4416 | 0.4946 | 0.0639 | -0.0530 | -0.0220 | -0.0310 | [-0.0717, 0.0316] | inconclusive |
| extreme | 4h | 736 | 0 | 0 | 0 | 602 | 306 | 0.4728 | 0.4660 | 0.0611 | 0.0068 | -0.0173 | 0.0241 | [-0.0443, 0.1122] | insufficient |
| extreme | 1d | 707 | 0 | 29 | 0 | 578 | 120 | 0.4328 | 0.5205 | 0.0467 | -0.0877 | -0.0129 | -0.0748 | [-0.1286, -0.0038] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 465 | 0 | 0 | 2 | 403 | 403 | 0.5204 | 0.4409 | 0.0387 | 0.0796 | 0.0063 | 0.0732 | [-0.0831, 0.2143] | inconclusive |
| value_edge | 15m | 465 | 0 | 0 | 2 | 403 | 400 | 0.5097 | 0.4581 | 0.0323 | 0.0516 | 0.0098 | 0.0418 | [-0.0945, 0.1689] | inconclusive |
| value_edge | 1h | 465 | 0 | 0 | 2 | 403 | 383 | 0.4409 | 0.4753 | 0.0839 | -0.0344 | 0.0034 | -0.0378 | [-0.1120, 0.0328] | insufficient |
| value_edge | 4h | 463 | 0 | 2 | 2 | 401 | 325 | 0.4190 | 0.4255 | 0.1555 | -0.0065 | -0.0020 | -0.0045 | [-0.1089, 0.1026] | insufficient |
| value_edge | 1d | 454 | 0 | 11 | 2 | 392 | 172 | 0.4405 | 0.4405 | 0.1189 | 0.0000 | 0.0103 | -0.0103 | [-0.0715, 0.0720] | insufficient |
| structural_extreme | 5m | 150 | 0 | 0 | 2 | 149 | 149 | 0.5467 | 0.3800 | 0.0733 | 0.1667 | -0.0081 | 0.1748 | [0.0478, 0.3086] | insufficient |
| structural_extreme | 15m | 150 | 0 | 0 | 2 | 149 | 147 | 0.5400 | 0.3933 | 0.0667 | 0.1467 | 0.0103 | 0.1363 | [-0.0194, 0.2764] | insufficient |
| structural_extreme | 1h | 150 | 0 | 0 | 2 | 149 | 140 | 0.4067 | 0.4333 | 0.1600 | -0.0267 | -0.0171 | -0.0096 | [-0.1490, 0.1134] | insufficient |
| structural_extreme | 4h | 150 | 0 | 0 | 2 | 149 | 120 | 0.3667 | 0.3733 | 0.2600 | -0.0067 | -0.0399 | 0.0332 | [-0.1089, 0.1275] | insufficient |
| structural_extreme | 1d | 149 | 0 | 1 | 2 | 148 | 82 | 0.4161 | 0.3557 | 0.2282 | 0.0604 | 0.0151 | 0.0453 | [-0.0551, 0.1398] | insufficient |
| neither | 5m | 3673 | 0 | 0 | 0 | 2917 | 2917 | 0.4800 | 0.4873 | 0.0327 | -0.0074 | -0.0011 | -0.0063 | [-0.0673, 0.0565] | inconclusive |
| neither | 15m | 3673 | 0 | 0 | 0 | 2917 | 2837 | 0.4637 | 0.4985 | 0.0378 | -0.0348 | -0.0013 | -0.0336 | [-0.0873, 0.0286] | inconclusive |
| neither | 1h | 3672 | 1 | 0 | 0 | 2916 | 2378 | 0.4469 | 0.4872 | 0.0659 | -0.0403 | -0.0040 | -0.0363 | [-0.0905, 0.0235] | inconclusive |
| neither | 4h | 3663 | 1 | 9 | 0 | 2908 | 1304 | 0.4365 | 0.4698 | 0.0936 | -0.0333 | -0.0073 | -0.0260 | [-0.0600, 0.0118] | inconclusive |
| neither | 1d | 3524 | 1 | 148 | 0 | 2792 | 318 | 0.4475 | 0.4594 | 0.0931 | -0.0119 | -0.0060 | -0.0059 | [-0.0369, 0.0278] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P4 trigger.sfp.1h@1

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 1063 | 0 | 0 | 0 | 818 | 818 | 0.4911 | 0.4807 | 0.0282 | 0.0103 | 0.0029 | 0.0074 | [-0.0782, 0.0947] | inconclusive |
| all | 15m | 1063 | 0 | 0 | 0 | 818 | 818 | 0.4826 | 0.4901 | 0.0273 | -0.0075 | 0.0025 | -0.0101 | [-0.1102, 0.0947] | inconclusive |
| all | 1h | 1063 | 0 | 0 | 0 | 818 | 787 | 0.4713 | 0.4864 | 0.0423 | -0.0151 | 0.0041 | -0.0191 | [-0.1022, 0.0582] | inconclusive |
| all | 4h | 1061 | 0 | 2 | 0 | 816 | 654 | 0.4656 | 0.4637 | 0.0707 | 0.0019 | -0.0019 | 0.0038 | [-0.0768, 0.0862] | inconclusive |
| all | 1d | 1032 | 0 | 31 | 0 | 791 | 257 | 0.4680 | 0.4506 | 0.0814 | 0.0174 | -0.0035 | 0.0210 | [-0.0303, 0.0714] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 269 | 0 | 0 | 2 | 214 | 214 | 0.4572 | 0.5093 | 0.0335 | -0.0520 | 0.0030 | -0.0550 | [-0.1967, 0.0523] | insufficient |
| low | 15m | 269 | 0 | 0 | 2 | 214 | 214 | 0.4647 | 0.5056 | 0.0297 | -0.0409 | -0.0012 | -0.0397 | [-0.1872, 0.0959] | insufficient |
| low | 1h | 269 | 0 | 0 | 2 | 214 | 212 | 0.4164 | 0.5167 | 0.0669 | -0.1004 | 0.0078 | -0.1082 | [-0.2123, 0.0069] | insufficient |
| low | 4h | 269 | 0 | 0 | 2 | 214 | 183 | 0.4201 | 0.4201 | 0.1599 | 0.0000 | 0.0028 | -0.0028 | [-0.1375, 0.1447] | insufficient |
| low | 1d | 269 | 0 | 0 | 2 | 214 | 93 | 0.4647 | 0.3606 | 0.1747 | 0.1041 | -0.0039 | 0.1080 | [-0.0566, 0.2513] | insufficient |
| medium | 5m | 304 | 0 | 0 | 0 | 232 | 232 | 0.5164 | 0.4605 | 0.0230 | 0.0559 | -0.0028 | 0.0587 | [-0.0844, 0.2188] | insufficient |
| medium | 15m | 304 | 0 | 0 | 0 | 232 | 232 | 0.5263 | 0.4572 | 0.0164 | 0.0691 | -0.0071 | 0.0762 | [-0.0738, 0.2274] | insufficient |
| medium | 1h | 304 | 0 | 0 | 0 | 232 | 226 | 0.5164 | 0.4507 | 0.0329 | 0.0658 | -0.0132 | 0.0790 | [-0.0147, 0.2027] | insufficient |
| medium | 4h | 304 | 0 | 0 | 0 | 232 | 202 | 0.4605 | 0.4868 | 0.0526 | -0.0263 | -0.0224 | -0.0039 | [-0.1110, 0.1094] | insufficient |
| medium | 1d | 301 | 0 | 3 | 0 | 230 | 125 | 0.4950 | 0.4452 | 0.0598 | 0.0498 | 0.0210 | 0.0288 | [-0.0622, 0.1426] | insufficient |
| high | 5m | 215 | 0 | 0 | 1 | 160 | 160 | 0.5209 | 0.4605 | 0.0186 | 0.0605 | 0.0102 | 0.0503 | [-0.1272, 0.2301] | insufficient |
| high | 15m | 215 | 0 | 0 | 1 | 160 | 160 | 0.5209 | 0.4558 | 0.0233 | 0.0651 | 0.0049 | 0.0602 | [-0.1244, 0.2325] | insufficient |
| high | 1h | 215 | 0 | 0 | 1 | 160 | 158 | 0.4791 | 0.4977 | 0.0233 | -0.0186 | 0.0233 | -0.0419 | [-0.1950, 0.1238] | insufficient |
| high | 4h | 215 | 0 | 0 | 1 | 160 | 149 | 0.5070 | 0.4512 | 0.0419 | 0.0558 | 0.0112 | 0.0446 | [-0.1174, 0.1701] | insufficient |
| high | 1d | 210 | 0 | 5 | 1 | 156 | 97 | 0.4143 | 0.5190 | 0.0667 | -0.1048 | -0.0353 | -0.0694 | [-0.2324, 0.0606] | insufficient |
| extreme | 5m | 253 | 0 | 0 | 2 | 199 | 199 | 0.4585 | 0.5020 | 0.0395 | -0.0435 | -0.0087 | -0.0347 | [-0.1687, 0.1169] | insufficient |
| extreme | 15m | 253 | 0 | 0 | 2 | 199 | 199 | 0.4150 | 0.5415 | 0.0435 | -0.1265 | -0.0038 | -0.1227 | [-0.2773, 0.0471] | insufficient |
| extreme | 1h | 253 | 0 | 0 | 2 | 199 | 191 | 0.4664 | 0.4862 | 0.0474 | -0.0198 | -0.0012 | -0.0186 | [-0.1832, 0.1760] | insufficient |
| extreme | 4h | 253 | 0 | 0 | 2 | 199 | 164 | 0.4980 | 0.4822 | 0.0198 | 0.0158 | 0.0114 | 0.0044 | [-0.1323, 0.1783] | insufficient |
| extreme | 1d | 247 | 0 | 6 | 2 | 193 | 91 | 0.4858 | 0.4939 | 0.0202 | -0.0081 | -0.0042 | -0.0039 | [-0.1223, 0.1153] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 115 | 0 | 0 | 0 | 100 | 100 | 0.4870 | 0.4696 | 0.0435 | 0.0174 | 0.0257 | -0.0083 | [-0.1645, 0.1326] | insufficient |
| value_edge | 15m | 115 | 0 | 0 | 0 | 100 | 100 | 0.4870 | 0.4783 | 0.0348 | 0.0087 | 0.0414 | -0.0327 | [-0.2342, 0.1638] | insufficient |
| value_edge | 1h | 115 | 0 | 0 | 0 | 100 | 100 | 0.5217 | 0.4696 | 0.0087 | 0.0522 | 0.0449 | 0.0073 | [-0.1771, 0.1972] | insufficient |
| value_edge | 4h | 115 | 0 | 0 | 0 | 100 | 98 | 0.4609 | 0.4522 | 0.0870 | 0.0087 | 0.0007 | 0.0080 | [-0.2609, 0.2370] | insufficient |
| value_edge | 1d | 112 | 0 | 3 | 0 | 97 | 80 | 0.4643 | 0.4375 | 0.0982 | 0.0268 | -0.0296 | 0.0563 | [-0.1086, 0.2056] | insufficient |
| structural_extreme | 5m | 45 | 0 | 0 | 0 | 43 | 43 | 0.4889 | 0.4000 | 0.1111 | 0.0889 | 0.0181 | 0.0708 | [-0.2538, 0.4632] | insufficient |
| structural_extreme | 15m | 45 | 0 | 0 | 0 | 43 | 43 | 0.4222 | 0.5333 | 0.0444 | -0.1111 | -0.0290 | -0.0821 | [-0.4187, 0.3212] | insufficient |
| structural_extreme | 1h | 45 | 0 | 0 | 0 | 43 | 43 | 0.4000 | 0.4889 | 0.1111 | -0.0889 | 0.0047 | -0.0936 | [-0.5191, 0.3269] | insufficient |
| structural_extreme | 4h | 45 | 0 | 0 | 0 | 43 | 42 | 0.4222 | 0.4222 | 0.1556 | 0.0000 | 0.0216 | -0.0216 | [-0.3386, 0.4056] | insufficient |
| structural_extreme | 1d | 45 | 0 | 0 | 0 | 43 | 39 | 0.5556 | 0.3111 | 0.1333 | 0.2444 | -0.0573 | 0.3018 | [0.0843, 0.5508] | insufficient |
| neither | 5m | 907 | 0 | 0 | 0 | 718 | 718 | 0.4917 | 0.4862 | 0.0221 | 0.0055 | 0.0001 | 0.0055 | [-0.0804, 0.0931] | inconclusive |
| neither | 15m | 907 | 0 | 0 | 0 | 718 | 718 | 0.4840 | 0.4906 | 0.0254 | -0.0066 | -0.0004 | -0.0063 | [-0.1034, 0.0958] | inconclusive |
| neither | 1h | 907 | 0 | 0 | 0 | 718 | 694 | 0.4664 | 0.4906 | 0.0430 | -0.0243 | -0.0012 | -0.0230 | [-0.1060, 0.0514] | inconclusive |
| neither | 4h | 905 | 0 | 2 | 0 | 716 | 592 | 0.4663 | 0.4685 | 0.0652 | -0.0022 | -0.0025 | 0.0003 | [-0.0760, 0.0738] | inconclusive |
| neither | 1d | 879 | 0 | 28 | 0 | 694 | 245 | 0.4630 | 0.4608 | 0.0762 | 0.0023 | 0.0002 | 0.0021 | [-0.0602, 0.0633] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P4 trigger.sfp.4h@1

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 257 | 0 | 0 | 0 | 203 | 203 | 0.4397 | 0.5370 | 0.0233 | -0.0973 | 0.0007 | -0.0980 | [-0.2705, 0.0530] | insufficient |
| all | 15m | 257 | 0 | 0 | 0 | 203 | 203 | 0.4475 | 0.5097 | 0.0428 | -0.0623 | 0.0054 | -0.0677 | [-0.2383, 0.1030] | insufficient |
| all | 1h | 257 | 0 | 0 | 0 | 203 | 203 | 0.4864 | 0.4669 | 0.0467 | 0.0195 | 0.0062 | 0.0132 | [-0.1093, 0.1573] | insufficient |
| all | 4h | 257 | 0 | 0 | 0 | 203 | 197 | 0.4786 | 0.4669 | 0.0545 | 0.0117 | -0.0065 | 0.0182 | [-0.0800, 0.1226] | insufficient |
| all | 1d | 250 | 3 | 4 | 0 | 198 | 142 | 0.4440 | 0.4920 | 0.0640 | -0.0480 | 0.0070 | -0.0550 | [-0.2253, 0.1447] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 37 | 0 | 0 | 0 | 33 | 33 | 0.2973 | 0.6757 | 0.0270 | -0.3784 | -0.0195 | -0.3589 | [-0.7531, -0.0378] | insufficient |
| low | 15m | 37 | 0 | 0 | 0 | 33 | 33 | 0.4324 | 0.5676 | 0.0000 | -0.1351 | -0.0200 | -0.1151 | [-0.4526, 0.1423] | insufficient |
| low | 1h | 37 | 0 | 0 | 0 | 33 | 33 | 0.5135 | 0.4865 | 0.0000 | 0.0270 | -0.0545 | 0.0815 | [-0.2501, 0.3690] | insufficient |
| low | 4h | 37 | 0 | 0 | 0 | 33 | 31 | 0.5405 | 0.3784 | 0.0811 | 0.1622 | -0.0522 | 0.2144 | [-0.0341, 0.5451] | insufficient |
| low | 1d | 37 | 0 | 0 | 0 | 33 | 25 | 0.3243 | 0.4865 | 0.1892 | -0.1622 | -0.0060 | -0.1561 | [-0.2898, 0.0843] | insufficient |
| medium | 5m | 68 | 0 | 0 | 0 | 53 | 53 | 0.3529 | 0.6029 | 0.0441 | -0.2500 | -0.0125 | -0.2375 | [-0.5068, 0.0440] | insufficient |
| medium | 15m | 68 | 0 | 0 | 0 | 53 | 53 | 0.4265 | 0.4853 | 0.0882 | -0.0588 | -0.0021 | -0.0567 | [-0.3566, 0.2854] | insufficient |
| medium | 1h | 68 | 0 | 0 | 0 | 53 | 53 | 0.4853 | 0.4559 | 0.0588 | 0.0294 | -0.0106 | 0.0400 | [-0.3134, 0.4796] | insufficient |
| medium | 4h | 68 | 0 | 0 | 0 | 53 | 53 | 0.4265 | 0.5147 | 0.0588 | -0.0882 | -0.0281 | -0.0602 | [-0.3152, 0.1844] | insufficient |
| medium | 1d | 68 | 0 | 0 | 0 | 53 | 45 | 0.5147 | 0.4265 | 0.0588 | 0.0882 | 0.0233 | 0.0650 | [-0.1568, 0.2570] | insufficient |
| high | 5m | 44 | 0 | 0 | 0 | 39 | 39 | 0.6136 | 0.3864 | 0.0000 | 0.2273 | 0.0132 | 0.2141 | [-0.0542, 0.4617] | insufficient |
| high | 15m | 44 | 0 | 0 | 0 | 39 | 39 | 0.6364 | 0.3409 | 0.0227 | 0.2955 | 0.0204 | 0.2750 | [0.1053, 0.4718] | insufficient |
| high | 1h | 44 | 0 | 0 | 0 | 39 | 39 | 0.5455 | 0.3864 | 0.0682 | 0.1591 | 0.0618 | 0.0973 | [-0.2765, 0.4693] | insufficient |
| high | 4h | 44 | 0 | 0 | 0 | 39 | 39 | 0.5909 | 0.3864 | 0.0227 | 0.2045 | 0.0634 | 0.1412 | [-0.0712, 0.4381] | insufficient |
| high | 1d | 44 | 0 | 0 | 0 | 39 | 37 | 0.4091 | 0.5682 | 0.0227 | -0.1591 | 0.0028 | -0.1619 | [-0.3518, 0.1240] | insufficient |
| extreme | 5m | 102 | 0 | 0 | 2 | 74 | 74 | 0.4804 | 0.5000 | 0.0196 | -0.0196 | -0.0348 | 0.0151 | [-0.1777, 0.1822] | insufficient |
| extreme | 15m | 102 | 0 | 0 | 2 | 74 | 74 | 0.4020 | 0.5588 | 0.0392 | -0.1569 | -0.0482 | -0.1087 | [-0.3571, 0.1539] | insufficient |
| extreme | 1h | 102 | 0 | 0 | 2 | 74 | 74 | 0.4706 | 0.4902 | 0.0392 | -0.0196 | -0.0445 | 0.0249 | [-0.1506, 0.2229] | insufficient |
| extreme | 4h | 102 | 0 | 0 | 2 | 74 | 73 | 0.4510 | 0.4902 | 0.0588 | -0.0392 | -0.0436 | 0.0044 | [-0.1866, 0.1822] | insufficient |
| extreme | 1d | 99 | 3 | 0 | 2 | 73 | 59 | 0.4444 | 0.5152 | 0.0404 | -0.0707 | -0.0917 | 0.0210 | [-0.2729, 0.3047] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 16 | 0 | 0 | 0 | 16 | 16 | 0.3125 | 0.6250 | 0.0625 | -0.3125 | 0.0130 | -0.3255 | [-0.8075, 0.1165] | insufficient |
| value_edge | 15m | 16 | 0 | 0 | 0 | 16 | 16 | 0.3750 | 0.6250 | 0.0000 | -0.2500 | 0.0304 | -0.2804 | [-0.8043, 0.2383] | insufficient |
| value_edge | 1h | 16 | 0 | 0 | 0 | 16 | 16 | 0.5000 | 0.4375 | 0.0625 | 0.0625 | 0.0239 | 0.0386 | [-0.4233, 0.5328] | insufficient |
| value_edge | 4h | 16 | 0 | 0 | 0 | 16 | 16 | 0.5000 | 0.5000 | 0.0000 | 0.0000 | -0.0126 | 0.0126 | [-0.3682, 0.4809] | insufficient |
| value_edge | 1d | 16 | 0 | 0 | 0 | 16 | 16 | 0.3125 | 0.6250 | 0.0625 | -0.3125 | 0.0344 | -0.3469 | [-0.8346, 0.0611] | insufficient |
| structural_extreme | 5m | 15 | 0 | 0 | 0 | 15 | 15 | 0.4000 | 0.6000 | 0.0000 | -0.2000 | 0.0027 | -0.2027 | [-0.7809, 0.2142] | insufficient |
| structural_extreme | 15m | 15 | 0 | 0 | 0 | 15 | 15 | 0.6000 | 0.4000 | 0.0000 | 0.2000 | 0.0423 | 0.1577 | [-0.2619, 0.5940] | insufficient |
| structural_extreme | 1h | 15 | 0 | 0 | 0 | 15 | 15 | 0.6667 | 0.3333 | 0.0000 | 0.3333 | 0.0417 | 0.2917 | [-0.1688, 0.6940] | insufficient |
| structural_extreme | 4h | 15 | 0 | 0 | 0 | 15 | 15 | 0.6667 | 0.2667 | 0.0667 | 0.4000 | 0.0154 | 0.3846 | [-0.0266, 0.8263] | insufficient |
| structural_extreme | 1d | 15 | 0 | 0 | 0 | 15 | 15 | 0.6667 | 0.2667 | 0.0667 | 0.4000 | 0.0866 | 0.3134 | [0.0156, 0.7139] | insufficient |
| neither | 5m | 226 | 0 | 0 | 0 | 178 | 178 | 0.4513 | 0.5265 | 0.0221 | -0.0752 | -0.0003 | -0.0749 | [-0.2500, 0.0804] | insufficient |
| neither | 15m | 226 | 0 | 0 | 0 | 178 | 178 | 0.4425 | 0.5088 | 0.0487 | -0.0664 | 0.0012 | -0.0676 | [-0.2584, 0.1445] | insufficient |
| neither | 1h | 226 | 0 | 0 | 0 | 178 | 178 | 0.4735 | 0.4779 | 0.0487 | -0.0044 | 0.0026 | -0.0070 | [-0.1279, 0.1307] | insufficient |
| neither | 4h | 226 | 0 | 0 | 0 | 178 | 175 | 0.4646 | 0.4779 | 0.0575 | -0.0133 | -0.0076 | -0.0057 | [-0.1120, 0.0956] | insufficient |
| neither | 1d | 219 | 3 | 4 | 0 | 173 | 131 | 0.4384 | 0.4977 | 0.0639 | -0.0594 | -0.0005 | -0.0589 | [-0.2510, 0.1442] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P4 trigger.sfp.1d@1

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 44 | 0 | 0 | 0 | 35 | 35 | 0.5227 | 0.4773 | 0.0000 | 0.0455 | -0.0031 | 0.0486 | [-0.3459, 0.4515] | insufficient |
| all | 15m | 44 | 0 | 0 | 0 | 35 | 35 | 0.4545 | 0.5227 | 0.0227 | -0.0682 | 0.0050 | -0.0732 | [-0.4370, 0.3144] | insufficient |
| all | 1h | 44 | 0 | 0 | 0 | 35 | 35 | 0.3864 | 0.5227 | 0.0909 | -0.1364 | 0.0109 | -0.1473 | [-0.4068, 0.0784] | insufficient |
| all | 4h | 44 | 0 | 0 | 0 | 35 | 35 | 0.4091 | 0.5227 | 0.0682 | -0.1136 | 0.0353 | -0.1489 | [-0.4854, 0.1200] | insufficient |
| all | 1d | 44 | 0 | 0 | 0 | 35 | 32 | 0.5000 | 0.4091 | 0.0909 | 0.0909 | -0.0058 | 0.0967 | [-0.1859, 0.3515] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 1 | 0 | 0 | 0 | 1 | 1 | 1.0000 | 0.0000 | 0.0000 | 1.0000 | 0.0145 | 0.9855 | [0.9006, 1.0775] | insufficient (undefined 3379) |
| low | 15m | 1 | 0 | 0 | 0 | 1 | 1 | 1.0000 | 0.0000 | 0.0000 | 1.0000 | 0.0487 | 0.9513 | [0.8566, 1.0545] | insufficient (undefined 3379) |
| low | 1h | 1 | 0 | 0 | 0 | 1 | 1 | 1.0000 | 0.0000 | 0.0000 | 1.0000 | 0.0816 | 0.9184 | [0.7930, 1.0553] | insufficient (undefined 3379) |
| low | 4h | 1 | 0 | 0 | 0 | 1 | 1 | 0.0000 | 1.0000 | 0.0000 | -1.0000 | 0.1053 | -1.1053 | [-1.2826, -0.9021] | insufficient (undefined 3379) |
| low | 1d | 1 | 0 | 0 | 0 | 1 | 1 | 0.0000 | 1.0000 | 0.0000 | -1.0000 | -0.2447 | -0.7553 | [-1.2063, -0.4073] | insufficient (undefined 3379) |
| medium | 5m | 11 | 0 | 0 | 0 | 10 | 10 | 0.5455 | 0.4545 | 0.0000 | 0.0909 | -0.0609 | 0.1518 | [-0.4106, 1.1226] | insufficient |
| medium | 15m | 11 | 0 | 0 | 0 | 10 | 10 | 0.5455 | 0.4545 | 0.0000 | 0.0909 | -0.0397 | 0.1306 | [-0.4581, 0.9987] | insufficient |
| medium | 1h | 11 | 0 | 0 | 0 | 10 | 10 | 0.4545 | 0.3636 | 0.1818 | 0.0909 | 0.0423 | 0.0486 | [-0.5960, 0.6008] | insufficient |
| medium | 4h | 11 | 0 | 0 | 0 | 10 | 10 | 0.6364 | 0.3636 | 0.0000 | 0.2727 | -0.0159 | 0.2886 | [-0.4433, 0.9533] | insufficient |
| medium | 1d | 11 | 0 | 0 | 0 | 10 | 10 | 0.5455 | 0.3636 | 0.0909 | 0.1818 | 0.0314 | 0.1504 | [-0.8000, 0.6559] | insufficient |
| high | 5m | 7 | 0 | 0 | 0 | 6 | 6 | 0.2857 | 0.7143 | 0.0000 | -0.4286 | 0.0712 | -0.4998 | [-1.1213, 0.5684] | insufficient (undefined 1) |
| high | 15m | 7 | 0 | 0 | 0 | 6 | 6 | 0.1429 | 0.7143 | 0.1429 | -0.5714 | -0.0314 | -0.5400 | [-0.9225, 0.1248] | insufficient (undefined 1) |
| high | 1h | 7 | 0 | 0 | 0 | 6 | 6 | 0.2857 | 0.7143 | 0.0000 | -0.4286 | -0.0999 | -0.3286 | [-0.8197, 0.2860] | insufficient (undefined 1) |
| high | 4h | 7 | 0 | 0 | 0 | 6 | 6 | 0.2857 | 0.4286 | 0.2857 | -0.1429 | 0.2778 | -0.4206 | [-1.0638, 0.2491] | insufficient (undefined 1) |
| high | 1d | 7 | 0 | 0 | 0 | 6 | 6 | 0.8571 | 0.1429 | 0.0000 | 0.7143 | 0.3506 | 0.3637 | [-0.2595, 0.7706] | insufficient (undefined 1) |
| extreme | 5m | 25 | 0 | 0 | 0 | 19 | 19 | 0.5600 | 0.4400 | 0.0000 | 0.1200 | 0.0143 | 0.1057 | [-0.4301, 0.6502] | insufficient |
| extreme | 15m | 25 | 0 | 0 | 0 | 19 | 19 | 0.4800 | 0.5200 | 0.0000 | -0.0400 | 0.0347 | -0.0747 | [-0.6634, 0.5589] | insufficient |
| extreme | 1h | 25 | 0 | 0 | 0 | 19 | 19 | 0.3600 | 0.5600 | 0.0800 | -0.2000 | 0.0080 | -0.2080 | [-0.5516, 0.1930] | insufficient |
| extreme | 4h | 25 | 0 | 0 | 0 | 19 | 19 | 0.3600 | 0.6000 | 0.0400 | -0.2400 | -0.0310 | -0.2090 | [-0.6233, 0.1353] | insufficient |
| extreme | 1d | 25 | 0 | 0 | 0 | 19 | 18 | 0.4000 | 0.4800 | 0.1200 | -0.0800 | -0.0762 | -0.0038 | [-0.4859, 0.3134] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 1 | 0 | 0 | 0 | 1 | 1 | 0.0000 | 1.0000 | 0.0000 | -1.0000 | -0.1587 | -0.8413 | [-0.9838, -0.6859] | insufficient (undefined 3419) |
| value_edge | 15m | 1 | 0 | 0 | 0 | 1 | 1 | 0.0000 | 1.0000 | 0.0000 | -1.0000 | -0.1144 | -0.8856 | [-1.1546, -0.6231] | insufficient (undefined 3419) |
| value_edge | 1h | 1 | 0 | 0 | 0 | 1 | 1 | 1.0000 | 0.0000 | 0.0000 | 1.0000 | 0.1513 | 0.8487 | [0.5368, 1.2077] | insufficient (undefined 3419) |
| value_edge | 4h | 1 | 0 | 0 | 0 | 1 | 1 | 1.0000 | 0.0000 | 0.0000 | 1.0000 | -0.0111 | 1.0111 | [0.5019, 1.4578] | insufficient (undefined 3419) |
| value_edge | 1d | 1 | 0 | 0 | 0 | 1 | 1 | 1.0000 | 0.0000 | 0.0000 | 1.0000 | 0.0480 | 0.9520 | [0.2056, 1.4362] | insufficient (undefined 3419) |
| structural_extreme | 5m | 7 | 0 | 0 | 0 | 7 | 7 | 0.4286 | 0.5714 | 0.0000 | -0.1429 | -0.0159 | -0.1270 | [-0.8517, 0.9269] | insufficient |
| structural_extreme | 15m | 7 | 0 | 0 | 0 | 7 | 7 | 0.5714 | 0.2857 | 0.1429 | 0.2857 | 0.0116 | 0.2741 | [-0.2835, 1.2585] | insufficient |
| structural_extreme | 1h | 7 | 0 | 0 | 0 | 7 | 7 | 0.2857 | 0.7143 | 0.0000 | -0.4286 | -0.0094 | -0.4191 | [-1.0659, 0.1933] | insufficient |
| structural_extreme | 4h | 7 | 0 | 0 | 0 | 7 | 7 | 0.2857 | 0.7143 | 0.0000 | -0.4286 | -0.0060 | -0.4226 | [-1.1766, 0.1905] | insufficient |
| structural_extreme | 1d | 7 | 0 | 0 | 0 | 7 | 7 | 0.2857 | 0.5714 | 0.1429 | -0.2857 | -0.0069 | -0.2788 | [-1.0034, 0.5830] | insufficient |
| neither | 5m | 36 | 0 | 0 | 0 | 30 | 30 | 0.5556 | 0.4444 | 0.0000 | 0.1111 | 0.0037 | 0.1074 | [-0.3093, 0.4779] | insufficient |
| neither | 15m | 36 | 0 | 0 | 0 | 30 | 30 | 0.4444 | 0.5556 | 0.0000 | -0.1111 | 0.0071 | -0.1182 | [-0.4818, 0.2489] | insufficient |
| neither | 1h | 36 | 0 | 0 | 0 | 30 | 30 | 0.3889 | 0.5000 | 0.1111 | -0.1111 | 0.0110 | -0.1221 | [-0.4404, 0.1593] | insufficient |
| neither | 4h | 36 | 0 | 0 | 0 | 30 | 30 | 0.4167 | 0.5000 | 0.0833 | -0.0833 | 0.0446 | -0.1279 | [-0.5320, 0.2363] | insufficient |
| neither | 1d | 36 | 0 | 0 | 0 | 30 | 27 | 0.5278 | 0.3889 | 0.0833 | 0.1389 | -0.0070 | 0.1459 | [-0.2050, 0.4790] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P1-trig trigger.effort_result.5m@1

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 12952 | 0 | 0 | 0 | 10691 | 10517 | 0.4730 | 0.4708 | 0.0562 | 0.0022 | -0.0007 | 0.0028 | [-0.0122, 0.0177] | inconclusive |
| all | 15m | 12952 | 0 | 0 | 0 | 10691 | 8070 | 0.4773 | 0.4719 | 0.0508 | 0.0054 | -0.0011 | 0.0065 | [-0.0119, 0.0256] | inconclusive |
| all | 1h | 12952 | 0 | 0 | 0 | 10691 | 4372 | 0.4684 | 0.4758 | 0.0558 | -0.0073 | -0.0014 | -0.0059 | [-0.0227, 0.0117] | inconclusive |
| all | 4h | 12929 | 2 | 21 | 0 | 10670 | 1693 | 0.4637 | 0.4560 | 0.0804 | 0.0077 | -0.0012 | 0.0090 | [-0.0077, 0.0265] | inconclusive |
| all | 1d | 12552 | 8 | 392 | 0 | 10361 | 335 | 0.4625 | 0.4532 | 0.0844 | 0.0093 | 0.0011 | 0.0082 | [-0.0048, 0.0224] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 3712 | 0 | 0 | 0 | 3083 | 3039 | 0.4747 | 0.4671 | 0.0582 | 0.0075 | -0.0012 | 0.0087 | [-0.0145, 0.0373] | inconclusive |
| low | 15m | 3712 | 0 | 0 | 0 | 3083 | 2341 | 0.4822 | 0.4736 | 0.0442 | 0.0086 | -0.0016 | 0.0102 | [-0.0126, 0.0461] | inconclusive |
| low | 1h | 3712 | 0 | 0 | 0 | 3083 | 1304 | 0.4663 | 0.4666 | 0.0671 | -0.0003 | -0.0033 | 0.0030 | [-0.0292, 0.0473] | inconclusive |
| low | 4h | 3712 | 0 | 0 | 0 | 3083 | 546 | 0.4246 | 0.4176 | 0.1579 | 0.0070 | -0.0053 | 0.0123 | [-0.0129, 0.0390] | inconclusive |
| low | 1d | 3711 | 0 | 1 | 0 | 3082 | 153 | 0.4328 | 0.4241 | 0.1431 | 0.0086 | 0.0043 | 0.0043 | [-0.0165, 0.0298] | insufficient |
| medium | 5m | 4024 | 0 | 0 | 0 | 3308 | 3255 | 0.4756 | 0.4747 | 0.0497 | 0.0010 | -0.0041 | 0.0051 | [-0.0154, 0.0260] | inconclusive |
| medium | 15m | 4024 | 0 | 0 | 0 | 3308 | 2487 | 0.4858 | 0.4672 | 0.0470 | 0.0186 | -0.0093 | 0.0279 | [0.0090, 0.0521] | hypothesis |
| medium | 1h | 4024 | 0 | 0 | 0 | 3308 | 1379 | 0.4784 | 0.4722 | 0.0495 | 0.0062 | -0.0124 | 0.0186 | [-0.0048, 0.0469] | inconclusive |
| medium | 4h | 4024 | 0 | 0 | 0 | 3308 | 616 | 0.4801 | 0.4699 | 0.0500 | 0.0102 | -0.0099 | 0.0201 | [-0.0073, 0.0479] | inconclusive |
| medium | 1d | 3985 | 0 | 39 | 0 | 3277 | 205 | 0.4640 | 0.4718 | 0.0642 | -0.0078 | -0.0120 | 0.0042 | [-0.0222, 0.0281] | insufficient |
| high | 5m | 2600 | 0 | 0 | 0 | 2141 | 2094 | 0.4723 | 0.4746 | 0.0531 | -0.0023 | -0.0058 | 0.0035 | [-0.0259, 0.0381] | inconclusive |
| high | 15m | 2600 | 0 | 0 | 0 | 2141 | 1627 | 0.4746 | 0.4765 | 0.0488 | -0.0019 | -0.0072 | 0.0053 | [-0.0321, 0.0520] | inconclusive |
| high | 1h | 2600 | 0 | 0 | 0 | 2141 | 924 | 0.4608 | 0.4892 | 0.0500 | -0.0285 | -0.0095 | -0.0190 | [-0.0509, 0.0259] | inconclusive |
| high | 4h | 2599 | 1 | 0 | 0 | 2140 | 465 | 0.4933 | 0.4525 | 0.0543 | 0.0408 | -0.0080 | 0.0488 | [0.0097, 0.0898] | hypothesis |
| high | 1d | 2523 | 1 | 76 | 0 | 2080 | 181 | 0.4986 | 0.4435 | 0.0579 | 0.0551 | -0.0073 | 0.0624 | [0.0276, 0.0906] | insufficient |
| extreme | 5m | 2404 | 0 | 0 | 0 | 2008 | 1981 | 0.4676 | 0.4642 | 0.0682 | 0.0033 | -0.0011 | 0.0045 | [-0.0197, 0.0329] | inconclusive |
| extreme | 15m | 2404 | 0 | 0 | 0 | 2008 | 1542 | 0.4601 | 0.4730 | 0.0670 | -0.0129 | -0.0042 | -0.0087 | [-0.0276, 0.0218] | inconclusive |
| extreme | 1h | 2404 | 0 | 0 | 0 | 2008 | 882 | 0.4659 | 0.4788 | 0.0553 | -0.0129 | -0.0117 | -0.0012 | [-0.0382, 0.0543] | inconclusive |
| extreme | 4h | 2403 | 1 | 0 | 0 | 2008 | 407 | 0.4623 | 0.4952 | 0.0424 | -0.0329 | -0.0144 | -0.0185 | [-0.0748, 0.0491] | inconclusive |
| extreme | 1d | 2333 | 7 | 64 | 0 | 1950 | 145 | 0.4681 | 0.4779 | 0.0540 | -0.0099 | 0.0035 | -0.0134 | [-0.0446, 0.0284] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 1380 | 0 | 0 | 0 | 1222 | 1206 | 0.4616 | 0.4746 | 0.0638 | -0.0130 | -0.0048 | -0.0082 | [-0.0343, 0.0180] | inconclusive |
| value_edge | 15m | 1380 | 0 | 0 | 0 | 1222 | 1071 | 0.4717 | 0.4804 | 0.0478 | -0.0087 | -0.0052 | -0.0035 | [-0.0342, 0.0276] | inconclusive |
| value_edge | 1h | 1380 | 0 | 0 | 0 | 1222 | 839 | 0.4638 | 0.4703 | 0.0659 | -0.0065 | -0.0005 | -0.0060 | [-0.0480, 0.0391] | inconclusive |
| value_edge | 4h | 1377 | 0 | 3 | 0 | 1219 | 569 | 0.4198 | 0.4582 | 0.1220 | -0.0385 | -0.0015 | -0.0370 | [-0.0856, 0.0144] | inconclusive |
| value_edge | 1d | 1354 | 2 | 24 | 0 | 1198 | 237 | 0.4261 | 0.4719 | 0.1019 | -0.0458 | 0.0081 | -0.0539 | [-0.1229, 0.0017] | insufficient |
| structural_extreme | 5m | 263 | 0 | 0 | 0 | 247 | 246 | 0.4487 | 0.4297 | 0.1217 | 0.0190 | -0.0060 | 0.0250 | [-0.0549, 0.1287] | insufficient |
| structural_extreme | 15m | 263 | 0 | 0 | 0 | 247 | 229 | 0.4905 | 0.4068 | 0.1027 | 0.0837 | -0.0059 | 0.0896 | [0.0162, 0.1644] | insufficient |
| structural_extreme | 1h | 263 | 0 | 0 | 0 | 247 | 213 | 0.4373 | 0.4335 | 0.1293 | 0.0038 | -0.0022 | 0.0060 | [-0.0556, 0.0685] | insufficient |
| structural_extreme | 4h | 260 | 0 | 3 | 0 | 245 | 179 | 0.3769 | 0.4077 | 0.2154 | -0.0308 | -0.0035 | -0.0273 | [-0.1390, 0.0791] | insufficient |
| structural_extreme | 1d | 256 | 0 | 7 | 0 | 241 | 108 | 0.3516 | 0.3945 | 0.2539 | -0.0430 | -0.0115 | -0.0315 | [-0.1069, 0.0280] | insufficient |
| neither | 5m | 11363 | 0 | 0 | 0 | 9422 | 9279 | 0.4747 | 0.4709 | 0.0544 | 0.0038 | -0.0001 | 0.0039 | [-0.0120, 0.0199] | inconclusive |
| neither | 15m | 11363 | 0 | 0 | 0 | 9422 | 7210 | 0.4780 | 0.4721 | 0.0499 | 0.0060 | -0.0005 | 0.0065 | [-0.0139, 0.0296] | inconclusive |
| neither | 1h | 11363 | 0 | 0 | 0 | 9422 | 4062 | 0.4701 | 0.4768 | 0.0531 | -0.0067 | -0.0010 | -0.0057 | [-0.0247, 0.0139] | inconclusive |
| neither | 4h | 11346 | 2 | 15 | 0 | 9406 | 1625 | 0.4707 | 0.4559 | 0.0734 | 0.0147 | -0.0008 | 0.0155 | [-0.0023, 0.0331] | inconclusive |
| neither | 1d | 10996 | 6 | 361 | 0 | 9118 | 332 | 0.4693 | 0.4513 | 0.0795 | 0.0180 | 0.0006 | 0.0175 | [0.0012, 0.0353] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P1-trig trigger.effort_result.15m@1

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 7496 | 0 | 0 | 0 | 4388 | 4388 | 0.4614 | 0.4680 | 0.0706 | -0.0065 | -0.0007 | -0.0058 | [-0.0258, 0.0180] | inconclusive |
| all | 15m | 7496 | 0 | 0 | 0 | 4388 | 4367 | 0.4720 | 0.4755 | 0.0526 | -0.0035 | -0.0010 | -0.0025 | [-0.0191, 0.0201] | inconclusive |
| all | 1h | 7496 | 0 | 0 | 0 | 4388 | 2930 | 0.4828 | 0.4613 | 0.0559 | 0.0215 | -0.0016 | 0.0231 | [0.0071, 0.0435] | hypothesis |
| all | 4h | 7478 | 1 | 17 | 0 | 4378 | 1382 | 0.4719 | 0.4417 | 0.0864 | 0.0302 | -0.0005 | 0.0307 | [0.0112, 0.0527] | hypothesis |
| all | 1d | 7255 | 3 | 238 | 0 | 4233 | 321 | 0.4619 | 0.4549 | 0.0833 | 0.0070 | -0.0007 | 0.0077 | [-0.0180, 0.0363] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 2174 | 0 | 0 | 0 | 1286 | 1286 | 0.4554 | 0.4669 | 0.0777 | -0.0115 | -0.0030 | -0.0085 | [-0.0502, 0.0313] | inconclusive |
| low | 15m | 2174 | 0 | 0 | 0 | 1286 | 1279 | 0.4825 | 0.4655 | 0.0520 | 0.0170 | -0.0018 | 0.0188 | [-0.0059, 0.0485] | inconclusive |
| low | 1h | 2174 | 0 | 0 | 0 | 1286 | 885 | 0.5005 | 0.4278 | 0.0718 | 0.0727 | -0.0026 | 0.0753 | [0.0376, 0.1273] | hypothesis |
| low | 4h | 2174 | 0 | 0 | 0 | 1286 | 453 | 0.4310 | 0.3942 | 0.1748 | 0.0368 | -0.0003 | 0.0371 | [-0.0170, 0.0929] | inconclusive |
| low | 1d | 2173 | 0 | 1 | 0 | 1285 | 142 | 0.4298 | 0.4234 | 0.1468 | 0.0064 | -0.0029 | 0.0093 | [-0.0328, 0.0474] | insufficient |
| medium | 5m | 2249 | 0 | 0 | 0 | 1328 | 1328 | 0.4606 | 0.4798 | 0.0596 | -0.0191 | -0.0027 | -0.0164 | [-0.0650, 0.0481] | inconclusive |
| medium | 15m | 2249 | 0 | 0 | 0 | 1328 | 1323 | 0.4629 | 0.4949 | 0.0422 | -0.0320 | -0.0076 | -0.0244 | [-0.0727, 0.0507] | inconclusive |
| medium | 1h | 2249 | 0 | 0 | 0 | 1328 | 912 | 0.4767 | 0.4767 | 0.0467 | 0.0000 | -0.0157 | 0.0157 | [-0.0330, 0.0624] | inconclusive |
| medium | 4h | 2249 | 0 | 0 | 0 | 1328 | 470 | 0.4882 | 0.4606 | 0.0511 | 0.0276 | -0.0046 | 0.0322 | [-0.0215, 0.0821] | inconclusive |
| medium | 1d | 2226 | 0 | 23 | 0 | 1313 | 186 | 0.4739 | 0.4730 | 0.0530 | 0.0009 | -0.0104 | 0.0113 | [-0.0280, 0.0503] | insufficient |
| high | 5m | 1516 | 0 | 0 | 0 | 907 | 907 | 0.4670 | 0.4558 | 0.0772 | 0.0112 | -0.0035 | 0.0147 | [-0.0398, 0.0747] | inconclusive |
| high | 15m | 1516 | 0 | 0 | 0 | 907 | 903 | 0.4894 | 0.4584 | 0.0521 | 0.0310 | -0.0066 | 0.0376 | [-0.0193, 0.1056] | inconclusive |
| high | 1h | 1516 | 0 | 0 | 0 | 907 | 627 | 0.4815 | 0.4736 | 0.0449 | 0.0079 | -0.0109 | 0.0188 | [-0.0129, 0.0699] | inconclusive |
| high | 4h | 1516 | 0 | 0 | 0 | 907 | 371 | 0.5059 | 0.4354 | 0.0587 | 0.0706 | -0.0052 | 0.0757 | [0.0331, 0.1218] | insufficient |
| high | 1d | 1478 | 0 | 38 | 0 | 879 | 161 | 0.5054 | 0.4276 | 0.0670 | 0.0778 | -0.0059 | 0.0837 | [0.0017, 0.1423] | insufficient |
| extreme | 5m | 1431 | 0 | 0 | 0 | 845 | 845 | 0.4675 | 0.4619 | 0.0706 | 0.0056 | -0.0007 | 0.0063 | [-0.0388, 0.0591] | inconclusive |
| extreme | 15m | 1431 | 0 | 0 | 0 | 845 | 841 | 0.4598 | 0.4787 | 0.0615 | -0.0189 | -0.0062 | -0.0127 | [-0.0710, 0.0642] | inconclusive |
| extreme | 1h | 1431 | 0 | 0 | 0 | 845 | 592 | 0.4689 | 0.4773 | 0.0538 | -0.0084 | -0.0061 | -0.0023 | [-0.0814, 0.1024] | inconclusive |
| extreme | 4h | 1430 | 1 | 0 | 0 | 844 | 319 | 0.4713 | 0.4909 | 0.0378 | -0.0196 | -0.0011 | -0.0184 | [-0.0808, 0.0703] | insufficient |
| extreme | 1d | 1378 | 3 | 50 | 0 | 813 | 132 | 0.4463 | 0.5044 | 0.0493 | -0.0581 | 0.0068 | -0.0649 | [-0.1119, 0.0024] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 771 | 0 | 0 | 0 | 562 | 562 | 0.4656 | 0.4501 | 0.0843 | 0.0156 | -0.0028 | 0.0184 | [-0.0566, 0.0923] | inconclusive |
| value_edge | 15m | 771 | 0 | 0 | 0 | 562 | 560 | 0.4591 | 0.4786 | 0.0623 | -0.0195 | 0.0004 | -0.0198 | [-0.0828, 0.0529] | inconclusive |
| value_edge | 1h | 771 | 0 | 0 | 0 | 562 | 486 | 0.4760 | 0.4578 | 0.0661 | 0.0182 | 0.0078 | 0.0104 | [-0.0443, 0.0689] | inconclusive |
| value_edge | 4h | 769 | 0 | 2 | 0 | 561 | 369 | 0.4499 | 0.4044 | 0.1456 | 0.0455 | 0.0118 | 0.0337 | [-0.0466, 0.1166] | insufficient |
| value_edge | 1d | 751 | 1 | 19 | 0 | 549 | 191 | 0.4714 | 0.4234 | 0.1052 | 0.0479 | 0.0098 | 0.0381 | [-0.0507, 0.1041] | insufficient |
| structural_extreme | 5m | 97 | 0 | 0 | 0 | 78 | 78 | 0.3814 | 0.4845 | 0.1340 | -0.1031 | -0.0137 | -0.0894 | [-0.3122, 0.1134] | insufficient |
| structural_extreme | 15m | 97 | 0 | 0 | 0 | 78 | 78 | 0.3814 | 0.5155 | 0.1031 | -0.1340 | -0.0372 | -0.0968 | [-0.3135, 0.1280] | insufficient |
| structural_extreme | 1h | 97 | 0 | 0 | 0 | 78 | 76 | 0.4021 | 0.5155 | 0.0825 | -0.1134 | 0.0156 | -0.1290 | [-0.2329, -0.0044] | insufficient |
| structural_extreme | 4h | 97 | 0 | 0 | 0 | 78 | 68 | 0.4124 | 0.3505 | 0.2371 | 0.0619 | 0.0333 | 0.0286 | [-0.0860, 0.1304] | insufficient |
| structural_extreme | 1d | 93 | 0 | 4 | 0 | 75 | 52 | 0.3656 | 0.3978 | 0.2366 | -0.0323 | -0.0427 | 0.0104 | [-0.1423, 0.1664] | insufficient |
| neither | 5m | 6649 | 0 | 0 | 0 | 4000 | 4000 | 0.4617 | 0.4701 | 0.0681 | -0.0084 | -0.0003 | -0.0081 | [-0.0290, 0.0154] | inconclusive |
| neither | 15m | 6649 | 0 | 0 | 0 | 4000 | 3980 | 0.4748 | 0.4745 | 0.0507 | 0.0003 | -0.0006 | 0.0009 | [-0.0198, 0.0249] | inconclusive |
| neither | 1h | 6649 | 0 | 0 | 0 | 4000 | 2733 | 0.4849 | 0.4605 | 0.0546 | 0.0244 | -0.0025 | 0.0269 | [0.0090, 0.0484] | hypothesis |
| neither | 4h | 6633 | 1 | 15 | 0 | 3990 | 1332 | 0.4752 | 0.4467 | 0.0781 | 0.0285 | -0.0021 | 0.0306 | [0.0107, 0.0515] | hypothesis |
| neither | 1d | 6432 | 2 | 215 | 0 | 3853 | 318 | 0.4622 | 0.4588 | 0.0790 | 0.0034 | -0.0013 | 0.0047 | [-0.0235, 0.0387] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P1-trig trigger.effort_result.1h@1

##### primary

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| all | 5m | 3738 | 0 | 0 | 0 | 1305 | 1305 | 0.4436 | 0.4791 | 0.0773 | -0.0356 | 0.0001 | -0.0357 | [-0.0674, -0.0058] | counter |
| all | 15m | 3738 | 0 | 0 | 0 | 1305 | 1305 | 0.4545 | 0.4831 | 0.0623 | -0.0286 | -0.0022 | -0.0264 | [-0.0560, -0.0003] | counter (block-sensitive (28d: inconclusive)) |
| all | 1h | 3738 | 0 | 0 | 0 | 1305 | 1302 | 0.4687 | 0.4690 | 0.0623 | -0.0003 | -0.0015 | 0.0012 | [-0.0413, 0.0476] | inconclusive |
| all | 4h | 3728 | 0 | 10 | 0 | 1301 | 858 | 0.4675 | 0.4370 | 0.0955 | 0.0306 | 0.0011 | 0.0295 | [-0.0096, 0.0720] | inconclusive |
| all | 1d | 3624 | 0 | 114 | 0 | 1267 | 278 | 0.4603 | 0.4523 | 0.0875 | 0.0080 | -0.0026 | 0.0106 | [-0.0166, 0.0429] | insufficient |

##### by regime

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| low | 5m | 1057 | 0 | 0 | 0 | 387 | 387 | 0.4333 | 0.4626 | 0.1041 | -0.0293 | -0.0041 | -0.0252 | [-0.0896, 0.0423] | inconclusive |
| low | 15m | 1057 | 0 | 0 | 0 | 387 | 387 | 0.4494 | 0.4834 | 0.0672 | -0.0341 | -0.0115 | -0.0226 | [-0.0753, 0.0293] | inconclusive |
| low | 1h | 1057 | 0 | 0 | 0 | 387 | 387 | 0.4447 | 0.4607 | 0.0946 | -0.0161 | -0.0095 | -0.0066 | [-0.0675, 0.0627] | inconclusive |
| low | 4h | 1057 | 0 | 0 | 0 | 387 | 271 | 0.3983 | 0.4002 | 0.2015 | -0.0019 | -0.0094 | 0.0075 | [-0.0461, 0.0576] | insufficient |
| low | 1d | 1057 | 0 | 0 | 0 | 387 | 119 | 0.4011 | 0.4380 | 0.1608 | -0.0369 | -0.0094 | -0.0274 | [-0.1019, 0.0452] | insufficient |
| medium | 5m | 1034 | 0 | 0 | 0 | 388 | 388 | 0.4623 | 0.4768 | 0.0609 | -0.0145 | 0.0004 | -0.0149 | [-0.0645, 0.0310] | inconclusive |
| medium | 15m | 1034 | 0 | 0 | 0 | 388 | 388 | 0.4613 | 0.4845 | 0.0542 | -0.0232 | -0.0027 | -0.0205 | [-0.0828, 0.0493] | inconclusive |
| medium | 1h | 1034 | 0 | 0 | 0 | 388 | 387 | 0.4662 | 0.4855 | 0.0484 | -0.0193 | -0.0062 | -0.0132 | [-0.1076, 0.0841] | inconclusive |
| medium | 4h | 1034 | 0 | 0 | 0 | 388 | 277 | 0.4903 | 0.4603 | 0.0493 | 0.0300 | -0.0035 | 0.0335 | [-0.0536, 0.1061] | insufficient |
| medium | 1d | 1017 | 0 | 17 | 0 | 384 | 145 | 0.4671 | 0.4798 | 0.0531 | -0.0128 | -0.0131 | 0.0003 | [-0.1116, 0.0795] | insufficient |
| high | 5m | 781 | 0 | 0 | 0 | 292 | 292 | 0.4430 | 0.5019 | 0.0551 | -0.0589 | -0.0053 | -0.0536 | [-0.1136, 0.0010] | insufficient |
| high | 15m | 781 | 0 | 0 | 0 | 292 | 292 | 0.4609 | 0.4827 | 0.0563 | -0.0218 | -0.0133 | -0.0085 | [-0.0716, 0.0536] | insufficient |
| high | 1h | 781 | 0 | 0 | 0 | 292 | 291 | 0.4968 | 0.4558 | 0.0474 | 0.0410 | -0.0035 | 0.0445 | [-0.0313, 0.1085] | insufficient |
| high | 4h | 781 | 0 | 0 | 0 | 292 | 234 | 0.5595 | 0.3880 | 0.0525 | 0.1716 | 0.0030 | 0.1685 | [0.0790, 0.2371] | insufficient |
| high | 1d | 763 | 0 | 18 | 0 | 286 | 131 | 0.5347 | 0.3971 | 0.0682 | 0.1376 | 0.0097 | 0.1280 | [0.0438, 0.1921] | insufficient |
| extreme | 5m | 809 | 0 | 0 | 0 | 284 | 284 | 0.4363 | 0.4796 | 0.0841 | -0.0433 | 0.0025 | -0.0457 | [-0.1152, 0.0374] | insufficient |
| extreme | 15m | 809 | 0 | 0 | 0 | 284 | 284 | 0.4425 | 0.4845 | 0.0729 | -0.0420 | -0.0056 | -0.0365 | [-0.1040, 0.0310] | insufficient |
| extreme | 1h | 809 | 0 | 0 | 0 | 284 | 284 | 0.4697 | 0.4747 | 0.0556 | -0.0049 | -0.0156 | 0.0106 | [-0.0862, 0.1282] | insufficient |
| extreme | 4h | 809 | 0 | 0 | 0 | 284 | 211 | 0.4314 | 0.5142 | 0.0544 | -0.0828 | -0.0033 | -0.0796 | [-0.1623, 0.0319] | insufficient |
| extreme | 1d | 787 | 0 | 22 | 0 | 275 | 108 | 0.4587 | 0.4892 | 0.0521 | -0.0305 | 0.0136 | -0.0441 | [-0.1269, 0.0553] | insufficient |

##### by location

| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|
| value_edge | 5m | 389 | 0 | 0 | 0 | 218 | 218 | 0.4730 | 0.4139 | 0.1131 | 0.0591 | -0.0036 | 0.0627 | [-0.0521, 0.1660] | insufficient |
| value_edge | 15m | 389 | 0 | 0 | 0 | 218 | 218 | 0.4756 | 0.4550 | 0.0694 | 0.0206 | -0.0129 | 0.0335 | [-0.0721, 0.1211] | insufficient |
| value_edge | 1h | 389 | 0 | 0 | 0 | 218 | 218 | 0.4833 | 0.4319 | 0.0848 | 0.0514 | -0.0085 | 0.0599 | [-0.0623, 0.1731] | insufficient |
| value_edge | 4h | 389 | 0 | 0 | 0 | 218 | 197 | 0.4961 | 0.3702 | 0.1337 | 0.1260 | 0.0188 | 0.1072 | [0.0036, 0.1994] | insufficient |
| value_edge | 1d | 383 | 0 | 6 | 0 | 216 | 127 | 0.4543 | 0.4204 | 0.1253 | 0.0339 | -0.0126 | 0.0465 | [-0.0693, 0.1507] | insufficient |
| structural_extreme | 5m | 30 | 0 | 0 | 0 | 22 | 22 | 0.5000 | 0.3667 | 0.1333 | 0.1333 | 0.0496 | 0.0837 | [-0.2300, 0.3539] | insufficient |
| structural_extreme | 15m | 30 | 0 | 0 | 0 | 22 | 22 | 0.6000 | 0.3000 | 0.1000 | 0.3000 | 0.0106 | 0.2894 | [-0.1485, 0.5789] | insufficient |
| structural_extreme | 1h | 30 | 0 | 0 | 0 | 22 | 22 | 0.5333 | 0.4333 | 0.0333 | 0.1000 | 0.0679 | 0.0321 | [-0.3045, 0.3166] | insufficient |
| structural_extreme | 4h | 30 | 0 | 0 | 0 | 22 | 22 | 0.5000 | 0.3333 | 0.1667 | 0.1667 | 0.0781 | 0.0885 | [-0.2488, 0.3418] | insufficient |
| structural_extreme | 1d | 27 | 0 | 3 | 0 | 20 | 19 | 0.2593 | 0.5185 | 0.2222 | -0.2593 | -0.0206 | -0.2386 | [-0.6073, 0.0759] | insufficient |
| neither | 5m | 3326 | 0 | 0 | 0 | 1220 | 1220 | 0.4396 | 0.4868 | 0.0737 | -0.0472 | -0.0000 | -0.0472 | [-0.0786, -0.0158] | counter |
| neither | 15m | 3326 | 0 | 0 | 0 | 1220 | 1220 | 0.4510 | 0.4874 | 0.0616 | -0.0364 | -0.0015 | -0.0349 | [-0.0668, -0.0085] | counter |
| neither | 1h | 3326 | 0 | 0 | 0 | 1220 | 1217 | 0.4669 | 0.4729 | 0.0601 | -0.0060 | -0.0012 | -0.0048 | [-0.0447, 0.0377] | inconclusive |
| neither | 4h | 3316 | 0 | 10 | 0 | 1216 | 818 | 0.4641 | 0.4451 | 0.0908 | 0.0190 | -0.0016 | 0.0206 | [-0.0178, 0.0651] | inconclusive |
| neither | 1d | 3221 | 0 | 105 | 0 | 1182 | 276 | 0.4632 | 0.4548 | 0.0820 | 0.0084 | -0.0009 | 0.0093 | [-0.0204, 0.0431] | insufficient |
| not_ready | 5m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 15m | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 4h | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |
| not_ready | 1d | 0 | 0 | 0 | 0 | 0 | 0 | – | – | – | – | – | – | – | insufficient (undefined 10000) |

#### P4 SFP cells: extreme_vs_opposite

| trigger | H | value | records |
|---|---|---|--:|
| trigger.sfp.15m@1 | 5m | extreme_first | 61 |
| trigger.sfp.15m@1 | 5m | neither | 148 |
| trigger.sfp.15m@1 | 5m | opposite_first | 2066 |
| trigger.sfp.15m@1 | 5m | undetermined | 1992 |
| trigger.sfp.15m@1 | 15m | extreme_first | 262 |
| trigger.sfp.15m@1 | 15m | neither | 154 |
| trigger.sfp.15m@1 | 15m | opposite_first | 1978 |
| trigger.sfp.15m@1 | 15m | undetermined | 1873 |
| trigger.sfp.15m@1 | 1h | extreme_first | 1066 |
| trigger.sfp.15m@1 | 1h | neither | 196 |
| trigger.sfp.15m@1 | 1h | opposite_first | 1746 |
| trigger.sfp.15m@1 | 1h | undetermined | 1258 |
| trigger.sfp.15m@1 | 1h | unknown | 1 |
| trigger.sfp.15m@1 | 4h | extreme_first | 2253 |
| trigger.sfp.15m@1 | 4h | neither | 145 |
| trigger.sfp.15m@1 | 4h | none | 11 |
| trigger.sfp.15m@1 | 4h | opposite_first | 1386 |
| trigger.sfp.15m@1 | 4h | undetermined | 471 |
| trigger.sfp.15m@1 | 4h | unknown | 1 |
| trigger.sfp.15m@1 | 1d | extreme_first | 3244 |
| trigger.sfp.15m@1 | 1d | neither | 38 |
| trigger.sfp.15m@1 | 1d | none | 160 |
| trigger.sfp.15m@1 | 1d | opposite_first | 791 |
| trigger.sfp.15m@1 | 1d | undetermined | 33 |
| trigger.sfp.15m@1 | 1d | unknown | 1 |
| trigger.sfp.1d@1 | 5m | opposite_first | 23 |
| trigger.sfp.1d@1 | 5m | undetermined | 21 |
| trigger.sfp.1d@1 | 15m | neither | 1 |
| trigger.sfp.1d@1 | 15m | opposite_first | 20 |
| trigger.sfp.1d@1 | 15m | undetermined | 23 |
| trigger.sfp.1d@1 | 1h | neither | 4 |
| trigger.sfp.1d@1 | 1h | opposite_first | 17 |
| trigger.sfp.1d@1 | 1h | undetermined | 23 |
| trigger.sfp.1d@1 | 4h | neither | 3 |
| trigger.sfp.1d@1 | 4h | opposite_first | 18 |
| trigger.sfp.1d@1 | 4h | undetermined | 23 |
| trigger.sfp.1d@1 | 1d | extreme_first | 2 |
| trigger.sfp.1d@1 | 1d | neither | 4 |
| trigger.sfp.1d@1 | 1d | opposite_first | 22 |
| trigger.sfp.1d@1 | 1d | undetermined | 16 |
| trigger.sfp.1h@1 | 5m | extreme_first | 3 |
| trigger.sfp.1h@1 | 5m | neither | 30 |
| trigger.sfp.1h@1 | 5m | opposite_first | 521 |
| trigger.sfp.1h@1 | 5m | undetermined | 509 |
| trigger.sfp.1h@1 | 15m | extreme_first | 12 |
| trigger.sfp.1h@1 | 15m | neither | 28 |
| trigger.sfp.1h@1 | 15m | opposite_first | 511 |
| trigger.sfp.1h@1 | 15m | undetermined | 512 |
| trigger.sfp.1h@1 | 1h | extreme_first | 75 |
| trigger.sfp.1h@1 | 1h | neither | 42 |
| trigger.sfp.1h@1 | 1h | opposite_first | 486 |
| trigger.sfp.1h@1 | 1h | undetermined | 460 |
| trigger.sfp.1h@1 | 4h | extreme_first | 261 |
| trigger.sfp.1h@1 | 4h | neither | 55 |
| trigger.sfp.1h@1 | 4h | none | 2 |
| trigger.sfp.1h@1 | 4h | opposite_first | 446 |
| trigger.sfp.1h@1 | 4h | undetermined | 299 |
| trigger.sfp.1h@1 | 1d | extreme_first | 613 |
| trigger.sfp.1h@1 | 1d | neither | 26 |
| trigger.sfp.1h@1 | 1d | none | 31 |
| trigger.sfp.1h@1 | 1d | opposite_first | 323 |
| trigger.sfp.1h@1 | 1d | undetermined | 70 |
| trigger.sfp.4h@1 | 5m | neither | 6 |
| trigger.sfp.4h@1 | 5m | opposite_first | 113 |
| trigger.sfp.4h@1 | 5m | undetermined | 138 |
| trigger.sfp.4h@1 | 15m | neither | 11 |
| trigger.sfp.4h@1 | 15m | opposite_first | 115 |
| trigger.sfp.4h@1 | 15m | undetermined | 131 |
| trigger.sfp.4h@1 | 1h | extreme_first | 2 |
| trigger.sfp.4h@1 | 1h | neither | 12 |
| trigger.sfp.4h@1 | 1h | opposite_first | 125 |
| trigger.sfp.4h@1 | 1h | undetermined | 118 |
| trigger.sfp.4h@1 | 4h | extreme_first | 9 |
| trigger.sfp.4h@1 | 4h | neither | 14 |
| trigger.sfp.4h@1 | 4h | opposite_first | 121 |
| trigger.sfp.4h@1 | 4h | undetermined | 113 |
| trigger.sfp.4h@1 | 1d | extreme_first | 87 |
| trigger.sfp.4h@1 | 1d | neither | 10 |
| trigger.sfp.4h@1 | 1d | none | 4 |
| trigger.sfp.4h@1 | 1d | opposite_first | 96 |
| trigger.sfp.4h@1 | 1d | undetermined | 57 |
| trigger.sfp.4h@1 | 1d | unknown | 3 |

#### Multiplicity

| family | cells | read (N) | intervals excluding 0 (K) | expected by chance (1 − c)·N |
|---|---|--:|--:|--:|
| P1-buy | primary | 12 | 1 | 0.6 |
| P1-buy | secondary | 62 | 4 | 3.1 |
| P1-sell | primary | 12 | 3 | 0.6 |
| P1-sell | secondary | 65 | 7 | 3.3 |
| P2 | primary | 12 | 8 | 0.6 |
| P2 | secondary | 125 | 58 | 6.3 |
| P3 | primary | 3 | 1 | 0.2 |
| P3 | secondary | 0 | 0 | 0.0 |
| P4 | primary | 11 | 0 | 0.6 |
| P4 | secondary | 27 | 1 | 1.4 |
| P1-trig | primary | 12 | 4 | 0.6 |
| P1-trig | secondary | 55 | 7 | 2.8 |
