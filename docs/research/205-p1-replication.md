# #206 P1 replication on 2020-01 … 2024-09 — report

- Issue: #206. Pre-registered by
  [ADR-064](../adr/064-p1-replication-pre-registration.md); its
  [coverage report](205-p1-replication-coverage.md) (#205) fixed which
  contexts can be read.
- Run: 2026-11-09, `measure_p1_replication`, once (ADR-064 S3 forbids a
  second run after an output has been read; no rerun was needed).
- Code: the test binary is a release build of
  `1bfaf40c2a46d647f960365975d711fbbe0a20be`, the merge commit of #207,
  which is the commit ADR-064 D6 requires, clean (its `test binary code`
  line has no `-dirty`). The coverage report cites `57699b8`, the amended
  commit of the same PR; the Rust tree is identical, only docs differ.
- Queries: [`028-order-flow-interactions.sql`](028-order-flow-interactions.sql),
  SHA-256 `dd32d605081d6c80992655e16f453d143ee4e3ba5b17f4bbe399fcee0e7144a1`;
  [`205-p1-replication.sql`](205-p1-replication.sql), SHA-256
  `5e742fed2fba222bb29f501de22bddb3c5d2a399186c495fd574118df7ec74c3`.
- Dataset: content `e985ea3e9efa9cc9aaf15dbc013fe76eb76711e811a4d3f414e79b591402049f`,
  the dataset of the coverage report, outside the repository.

## Summary

- **Measurable: 46 of 48 contexts** (units ≥ n_min = 385). The two that are
  not: P1-buy w=1h high 1d (354 units) and P1-buy w=1h extreme 1d (288
  units).
- **Readings:** `no effect` 46, `not measurable` 2, `replicated
  (hypothesis)` 0, `replicated (counter)` 0, `insufficient` 0, `not net
  positive` 0. The 46 are 24 at 4h and 22 at 1d.
- **`replicated: 0 of 48`** (Benjamini–Hochberg at q = 0.05, m = 48,
  unread contexts at p = 1). The BH column is `–` in all 48 rows: no
  context is rejected at 8-week blocks.
- **Stop rule S3 applies** (ADR-064 D4): P1 is recorded as not replicated at
  4h / 1d on archive `aggTrades` 2020-01 … 2024-09. Nothing is promoted.
- The tool's `reading` column is copied, not recomputed. The counts above
  are tallies over the printed rows by a read-only script; the reading is
  consistent with D4's rule (no row passes BH, so none can reach
  `replicated`, and each `no effect` row is a measurable context BH did not
  reject).

## Run identity

The lines the tool printed before any table, and what they are compared
with:

| Printed line | Value | Pinned / expected | Match |
|---|---|---|---|
| `test binary code` | `git:1bfaf40c2a46d647f960365975d711fbbe0a20be` | #207's merge commit (D6, *Accept when*) | yes, no `-dirty` |
| `replication.rs frozen decisions sha256` | `ece00cf22aaaa527f2c7f83fe5eb08978267c5599eac203781de5e39ccbd1f7c` | the pin in the pre-registration record | yes |
| `ADR-064 frozen` | `6ed0f6afb3688c99ebc501eb1530e0cd088122fa3e7a1e4a067bc08fd6dcafae` | the pin in the pre-registration record | yes |
| queries `028` | `dd32d605081d6c80992655e16f453d143ee4e3ba5b17f4bbe399fcee0e7144a1` | the pin | yes |
| queries `205` | `5e742fed2fba222bb29f501de22bddb3c5d2a399186c495fd574118df7ec74c3` | the pin | yes |
| dataset `content` | `e985ea3e9efa9cc9aaf15dbc013fe76eb76711e811a4d3f414e79b591402049f` | the coverage report's | yes |
| dataset `data` | `7e92345c663e72e096cbdf2921f60eaa8d9cbae291cd2f1f82bdd64306d27d10` | the coverage report's | yes |
| `engine` | `4c844593779da2e3-1` | ADR-058 D1's key | yes |
| `conditional.sampling` | `@1 2c71fff23e8b0415` | same | yes |
| `conditional.labels` | `@2 6f90bff079e746f2` | same | yes |
| dataset `code` | `git:72371e363248db70b8a2b2d4445291859d7b311e` | the PR's first commit, which built the dataset | yes (expected; the binary's own line above is the one that must equal #207's merge commit) |
| `source` | `archive binance-archive/BTCUSDT streams aggTrades correction fold` | same | yes |
| `replay` | `(1577836800000, 1727740800000)` | the window | yes |
| `records` | `(1577836800000, 1727740800000)` | the window | yes |
| record sets | `record sets equal` | D1 guard | yes |
| twin | `ADR-058 twin mismatches 0` | 0 | yes |

The window guard printed `replay (1577836800000, 1727740800000); records
(1577836800000, 1727740800000)`: both end at `1727740800000` ms, 2024-10-01
00:00 UTC, so no record from 2024-10-01 on is read. The calibration slice
and the hold-out stay unread.

## Commands and inputs

Timestamps are UTC. The checkout is a detached worktree of the commit, so
the binary was built from a clean tree; the target directory is outside it.

```text
2026-11-09T17:32:37Z git -C <mie> worktree add --detach <worktree> 1bfaf40c2a46d647f960365975d711fbbe0a20be
2026-11-09T17:32:37Z git rev-parse HEAD -> 1bfaf40c2a46d647f960365975d711fbbe0a20be
2026-11-09T17:32:37Z git status --porcelain --ignored -> <> (empty)
2026-11-09T17:32:37Z cargo config check: no .cargo in <worktree> or any parent, no ~/.cargo/config*, env CARGO*/RUSTFLAGS/RUSTC empty
2026-11-09T17:32:37Z CARGO_TARGET_DIR=<target dir> systemd-run --user --scope -q -p CPUQuota=400% -p MemoryMax=8G cargo test -j4 --release -p mie-cli --test order_flow_study --no-run
    (release build finished in 27m 06s, a full build in a fresh target dir)
post-build: HEAD=1bfaf40c2a46d647f960365975d711fbbe0a20be, git status --porcelain --ignored -> <> (empty)
2026-11-09T17:59:44Z systemd-run --user --scope -q -p CPUQuota=400% -p MemoryMax=12G env MIE_OUTCOME_DATASET=<dataset> /usr/bin/time -v <test binary> --ignored --exact --nocapture replication::measure_p1_replication > <run dir>/measure.out 2> <run dir>/measure.err
```

`<test binary>` is `<target dir>/release/deps/order_flow_study-<hash>`, the
`Executable` line of the build. `MIE_OUTCOME_DATASET` is set for the run
only. Wall time 1:40.61 (m:ss), peak RSS 7 406 304 kB (about 7.4 GB), exit
status 0, 1 test ran, user time 159.59 s, system time 21.99 s.

Inputs, as the tool printed them, each with its source in
[ADR-064 D7](../adr/064-p1-replication-pre-registration.md):

| Input | Value | Source |
|---|---|---|
| c, μ, n_min | 0.95, 0.10 R, 385 (z = 1.959964) | D3 T1, ⌈(z/μ)²⌉ |
| Fee | 10 bps round trip per label | `docs/direction.md`, the owner's tier |
| q, m | 0.05, 48 | `docs/direction.md` addendum 2026-11-08 |
| T3 | 1 000 000 draws, blocks 8 weeks (4 for sensitivity), seed 205 | D3 T3 |
| T2 | 10 000 resamples, blocks 56 days (28 and 112 for sensitivity), seed 205 | D3 T2 |
| ADR-058 D7 comparison | 10 000 resamples, block 56 days, seed 28 | D4 information |
| Window length | 247.857 weeks | 1 735 days ÷ 7 |

## Not read

S2 applies. Two contexts are below n_min = 385. Only their counts are
listed; they enter BH with p = 1, so m stays 48.

| pair | regime | H | units |
|---|---|---|--:|
| P1-buy w=1h | high | 1d | 354 |
| P1-buy w=1h | extreme | 1d | 288 |

## Results by decision

All 46 measurable contexts read `no effect`; no other reading occurs in
this run. The table copies the printed rows (the 8-week p-value, the BH
decision, the T2 56-day interval, the net expectancy in R and the net R
per year); `E` and `s` are the printed effect and its sign. The `flags`
column lists `null-sensitive` and `block-sensitive` entries, which the
tool appends to the reading in parentheses: no reading carries one, so all
46 are bare `no effect`. Flags do not change a reading in any case. Null
results are listed in full; the complete rows, with `T`, `T_b`, the 4-week
p-value, drawdown and the ADR-058 D7 reading, are in the tool's output
below.

| context | regime | H | units | E | s | p (8w) | BH | T2 interval (56d) | net expectancy (R) | net R/year | flags |
|---|---|---|--:|--:|--:|--:|---|---|--:|--:|---|
| P1-buy w=5m | low | 4h | 2320 | -0.0069 | -1 | 0.609489 | – | [-0.2559, -0.1650] | -0.2110 | -102.6852 | none |
| P1-buy w=5m | low | 1d | 737 | -0.0188 | -1 | 0.705621 | – | [-0.1207, -0.0055] | -0.0640 | -9.8994 | none |
| P1-buy w=5m | medium | 4h | 2134 | 0.0063 | +1 | 0.935871 | – | [-0.2475, -0.1541] | -0.2001 | -89.5649 | none |
| P1-buy w=5m | medium | 1d | 830 | -0.0066 | -1 | 0.741409 | – | [-0.1346, -0.0297] | -0.0810 | -14.1113 | none |
| P1-buy w=5m | high | 4h | 1787 | 0.0260 | +1 | 0.793737 | – | [-0.2510, -0.1500] | -0.2004 | -75.1357 | none |
| P1-buy w=5m | high | 1d | 751 | 0.0297 | +1 | 0.577831 | – | [-0.1675, -0.0124] | -0.0891 | -14.0428 | none |
| P1-buy w=5m | extreme | 4h | 1588 | 0.0150 | +1 | 0.513391 | – | [-0.1931, -0.0911] | -0.1407 | -46.8712 | none |
| P1-buy w=5m | extreme | 1d | 598 | -0.0304 | -1 | 0.556879 | – | [-0.1257, 0.0350] | -0.0467 | -5.8532 | none |
| P1-buy w=15m | low | 4h | 1650 | -0.0177 | -1 | 0.418794 | – | [-0.2879, -0.1728] | -0.2300 | -79.6342 | none |
| P1-buy w=15m | low | 1d | 649 | -0.0197 | -1 | 0.710297 | – | [-0.1606, -0.0505] | -0.1073 | -14.6135 | none |
| P1-buy w=15m | medium | 4h | 1453 | 0.0107 | +1 | 0.928051 | – | [-0.2602, -0.1565] | -0.2098 | -63.9489 | none |
| P1-buy w=15m | medium | 1d | 706 | -0.0291 | -1 | 0.539689 | – | [-0.1303, -0.0018] | -0.0658 | -9.7388 | none |
| P1-buy w=15m | high | 4h | 1202 | 0.0061 | +1 | 0.723649 | – | [-0.2156, -0.0811] | -0.1479 | -37.3036 | none |
| P1-buy w=15m | high | 1d | 633 | 0.0043 | +1 | 0.903673 | – | [-0.1587, 0.0118] | -0.0734 | -9.7533 | none |
| P1-buy w=15m | extreme | 4h | 1044 | -0.0403 | -1 | 0.478916 | – | [-0.2166, -0.1245] | -0.1712 | -37.5030 | none |
| P1-buy w=15m | extreme | 1d | 494 | -0.0256 | -1 | 0.609345 | – | [-0.1518, -0.0182] | -0.0835 | -8.6590 | none |
| P1-buy w=1h | low | 4h | 793 | -0.0305 | -1 | 0.470596 | – | [-0.2973, -0.1345] | -0.2169 | -36.0812 | none |
| P1-buy w=1h | low | 1d | 439 | -0.0977 | -1 | 0.259934 | – | [-0.1848, -0.0367] | -0.1110 | -10.2223 | none |
| P1-buy w=1h | medium | 4h | 673 | 0.0433 | +1 | 0.588983 | – | [-0.2671, -0.1191] | -0.1935 | -27.3186 | none |
| P1-buy w=1h | medium | 1d | 433 | -0.0545 | -1 | 0.681589 | – | [-0.1820, 0.0079] | -0.0880 | -7.9924 | none |
| P1-buy w=1h | high | 4h | 505 | 0.0426 | +1 | 0.463528 | – | [-0.2939, -0.0882] | -0.1917 | -20.3141 | none |
| P1-buy w=1h | extreme | 4h | 412 | 0.0284 | +1 | 0.939255 | – | [-0.2717, -0.0560] | -0.1613 | -13.9451 | none |
| P1-sell w=5m | low | 4h | 2315 | -0.0353 | -1 | 0.292044 | – | [-0.2740, -0.1852] | -0.2289 | -111.1495 | none |
| P1-sell w=5m | low | 1d | 746 | -0.0294 | -1 | 0.337550 | – | [-0.1961, -0.0614] | -0.1259 | -19.6986 | none |
| P1-sell w=5m | medium | 4h | 2214 | 0.0059 | +1 | 0.881305 | – | [-0.2716, -0.1693] | -0.2210 | -102.6454 | none |
| P1-sell w=5m | medium | 1d | 847 | -0.0360 | -1 | 0.484002 | – | [-0.1179, -0.0002] | -0.0594 | -10.5518 | none |
| P1-sell w=5m | high | 4h | 1918 | 0.0075 | +1 | 0.715349 | – | [-0.2259, -0.1200] | -0.1714 | -68.9865 | none |
| P1-sell w=5m | high | 1d | 779 | -0.0039 | -1 | 0.897041 | – | [-0.1161, 0.0209] | -0.0483 | -7.8928 | none |
| P1-sell w=5m | extreme | 4h | 1728 | 0.0004 | +1 | 0.810803 | – | [-0.1803, -0.0908] | -0.1341 | -48.6105 | none |
| P1-sell w=5m | extreme | 1d | 608 | -0.0279 | -1 | 0.732575 | – | [-0.2121, -0.0342] | -0.1232 | -15.7109 | none |
| P1-sell w=15m | low | 4h | 1785 | -0.0416 | -1 | 0.187836 | – | [-0.2762, -0.1567] | -0.2161 | -80.9212 | none |
| P1-sell w=15m | low | 1d | 674 | -0.0419 | -1 | 0.218500 | – | [-0.1708, -0.0510] | -0.1113 | -15.7448 | none |
| P1-sell w=15m | medium | 4h | 1619 | -0.0327 | -1 | 0.572145 | – | [-0.2613, -0.1825] | -0.2231 | -75.7874 | none |
| P1-sell w=15m | medium | 1d | 747 | -0.0429 | -1 | 0.597645 | – | [-0.1087, 0.0144] | -0.0490 | -7.6799 | none |
| P1-sell w=15m | high | 4h | 1377 | -0.0000 | -1 | 0.986279 | – | [-0.2572, -0.1619] | -0.2095 | -60.5264 | none |
| P1-sell w=15m | high | 1d | 667 | -0.0492 | -1 | 0.391364 | – | [-0.1435, 0.0173] | -0.0651 | -9.1064 | none |
| P1-sell w=15m | extreme | 4h | 1247 | -0.0048 | -1 | 0.592401 | – | [-0.2769, -0.1896] | -0.2329 | -60.9373 | none |
| P1-sell w=15m | extreme | 1d | 533 | -0.0449 | -1 | 0.654199 | – | [-0.1849, -0.0238] | -0.1027 | -11.4859 | none |
| P1-sell w=1h | low | 4h | 923 | -0.0397 | -1 | 0.297794 | – | [-0.3038, -0.1525] | -0.2297 | -44.4769 | none |
| P1-sell w=1h | low | 1d | 468 | -0.0932 | -1 | 0.068224 | – | [-0.1405, 0.0178] | -0.0609 | -5.9812 | none |
| P1-sell w=1h | medium | 4h | 802 | -0.0284 | -1 | 0.968991 | – | [-0.2534, -0.1289] | -0.1906 | -32.0629 | none |
| P1-sell w=1h | medium | 1d | 500 | -0.0738 | -1 | 0.535629 | – | [-0.1034, 0.0230] | -0.0415 | -4.3537 | none |
| P1-sell w=1h | high | 4h | 740 | 0.0115 | +1 | 0.673865 | – | [-0.2948, -0.1517] | -0.2234 | -34.6875 | none |
| P1-sell w=1h | high | 1d | 460 | -0.0407 | -1 | 0.499324 | – | [-0.1571, 0.0090] | -0.0749 | -7.2323 | none |
| P1-sell w=1h | extreme | 4h | 705 | 0.0383 | +1 | 0.549425 | – | [-0.2428, -0.0896] | -0.1664 | -24.6121 | none |
| P1-sell w=1h | extreme | 1d | 397 | -0.0320 | -1 | 0.697553 | – | [-0.1847, -0.0016] | -0.0943 | -7.8516 | none |

BH decision list at q = 0.05, m = 48: no context is rejected (`–` in the BH
column of all 48 rows). The smallest 8-week p is 0.068224 (P1-sell w=1h
low 1d) and the smallest 4-week p is 0.124486 (the same context); no
p-value of either null is below 0.05. BH's strictest comparison is p ≤
q/48 = 1.04 · 10⁻³ (ADR-064 D7), so BH rejects nothing at either block
length. Because no context reaches T3, T2 is not decisive for any reading.
For information: the T2 interval's lower bound is above 0 in none of the
46, the whole interval is below 0 in 37, and it spans 0 in the other nine, all at 1d. Net expectancy is
negative in all 46.

## Information, not gates

ADR-064 D4 prints these for every measurable context; A (2 a week) and B
(1 a week) and the 30 R drawdown are judged out of sample (#30, #33), so
they gate nothing here. Over the 46 printed rows:

- units/week at or above A (2): 39 of 46 (23 of 24 at 4h, 16 of 22 at 1d);
  at or above B (1): 46 of 46.
- net R/year above 0: 0 of 46. The least negative is -4.3537 (P1-sell
  w=1h medium 1d), the most negative -111.1495 (P1-sell w=5m low 4h).
- max drawdown below 30 R: 1 of 46 (24.5043 R, P1-sell w=1h medium 1d,
  whose net R/year is negative); the others are 32.4383 R or more.
- ADR-058 D7's 56-day reading of E, for comparison with #28: `inconclusive`
  37, `counter` 8, `hypothesis` 1 (P1-buy w=5m high 4h). None of these
  passed T3 here, and none reads `replicated`.

#28's other P1 observations, including the primary 4h P1-sell
`hypothesis`, are not re-promoted by this result. The two `not measurable`
contexts stay unread.

## Verdict and follow-ups

**S3: 0 of 48 replicated.** P1 is recorded as not replicated at 4h / 1d on
archive `aggTrades` 2020-01 … 2024-09. No later re-run on this window with
changed definitions counts as confirmatory; a better definition is a new
pre-registration on new data. Nothing is promoted and the hold-out stays
unread. This PR files no issue, and the result does not unblock #30 or
#33.

## Deviations from ADR-064

None to the pre-registration: the entry, the frozen text, the queries, the
inputs and the dataset are the pinned ones, and the guard passed. Two
operational notes, neither inside the frozen region:

- The build ran `cargo test -j4 …`; the plan's command had no `-j4`. It
  caps the parallel compile jobs to the 400 % CPU quota and does not
  change the produced code.
- The harness lines of the test binary (`running 1 test`, the
  `has been running for over 60 seconds` notice, `test … ok`, `test
  result`) are left out of the section below, as in the coverage report.

## Results: the tool's output

The output of `measure_p1_replication`, from its first line to its last,
with every heading two levels lower and runs of blank lines collapsed; the
test harness's own lines are left out. No row is dropped and no value is
rounded or re-derived.

### P1 replication (#206, ADR-064)

dataset content e985ea3e9efa9cc9aaf15dbc013fe76eb76711e811a4d3f414e79b591402049f, data 7e92345c663e72e096cbdf2921f60eaa8d9cbae291cd2f1f82bdd64306d27d10, engine 4c844593779da2e3-1, conditional.sampling@1 2c71fff23e8b0415, conditional.labels@2 6f90bff079e746f2, code git:72371e363248db70b8a2b2d4445291859d7b311e
source archive binance-archive/BTCUSDT streams aggTrades correction fold; replay (1577836800000, 1727740800000); records (1577836800000, 1727740800000)
test binary code git:1bfaf40c2a46d647f960365975d711fbbe0a20be
queries sha256 028 dd32d605081d6c80992655e16f453d143ee4e3ba5b17f4bbe399fcee0e7144a1, 205 5e742fed2fba222bb29f501de22bddb3c5d2a399186c495fd574118df7ec74c3; ADR-064 frozen 6ed0f6afb3688c99ebc501eb1530e0cd088122fa3e7a1e4a067bc08fd6dcafae; replication.rs frozen decisions sha256 ece00cf22aaaa527f2c7f83fe5eb08978267c5599eac203781de5e39ccbd1f7c; record sets equal; ADR-058 twin mismatches 0

#### Inputs

c = 0.95, μ = 0.1 R, z = 1.959964, n_min = 385; fee 10 bps round trip per label; q = 0.05 over 48 contexts; T3 1000000 draws, blocks 8 and 4 weeks, seed 205; T2 10000 resamples, blocks 56 / 28 / 112 days, seed 205; ADR-058 D7 10000 resamples, block 56 days, seed 28; 247.857 weeks.

#### Contexts

| pair | w | regime | H | members that ran | dropped | episodes | units | T | T_b | E | s | p (8w) | BH | p (4w) | net expectancy (R) | T2 interval (56d) | units/week | net R/year | max drawdown (R) | E reading (ADR-058 D7, 56d) | reading |
|---|---|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|--:|--:|---|--:|--:|--:|---|---|
| P1-buy | 5m | low | 4h | 13288 | 0 | 7396 | 2320 | -0.0028 | 0.0041 | -0.0069 | -1 | 0.609489 | – | 0.432758 | -0.2110 | [-0.2559, -0.1650] | 9.36 | -102.6852 | 493.2321 | inconclusive | no effect |
| P1-buy | 5m | low | 1d | 13259 | 0 | 7378 | 737 | -0.0064 | 0.0124 | -0.0188 | -1 | 0.705621 | – | 0.523735 | -0.0640 | [-0.1207, -0.0055] | 2.97 | -9.8994 | 62.1275 | inconclusive | no effect |
| P1-buy | 5m | medium | 4h | 10085 | 0 | 5881 | 2134 | 0.0191 | 0.0129 | 0.0063 | +1 | 0.935871 | – | 0.928715 | -0.2001 | [-0.2475, -0.1541] | 8.61 | -89.5649 | 430.5631 | inconclusive | no effect |
| P1-buy | 5m | medium | 1d | 10025 | 0 | 5850 | 830 | -0.0015 | 0.0051 | -0.0066 | -1 | 0.741409 | – | 0.703999 | -0.0810 | [-0.1346, -0.0297] | 3.35 | -14.1113 | 71.7886 | inconclusive | no effect |
| P1-buy | 5m | high | 4h | 7888 | 0 | 4705 | 1787 | 0.0221 | -0.0039 | 0.0260 | +1 | 0.793737 | – | 0.878191 | -0.2004 | [-0.2510, -0.1500] | 7.21 | -75.1357 | 360.5198 | hypothesis | no effect |
| P1-buy | 5m | high | 1d | 7847 | 0 | 4682 | 751 | 0.0149 | -0.0148 | 0.0297 | +1 | 0.577831 | – | 0.796437 | -0.0891 | [-0.1675, -0.0124] | 3.03 | -14.0428 | 75.8577 | inconclusive | no effect |
| P1-buy | 5m | extreme | 4h | 6947 | 0 | 4274 | 1588 | 0.0089 | -0.0061 | 0.0150 | +1 | 0.513391 | – | 0.711527 | -0.1407 | [-0.1931, -0.0911] | 6.41 | -46.8712 | 225.8507 | inconclusive | no effect |
| P1-buy | 5m | extreme | 1d | 6906 | 0 | 4253 | 598 | -0.0517 | -0.0213 | -0.0304 | -1 | 0.556879 | – | 0.489618 | -0.0467 | [-0.1257, 0.0350] | 2.41 | -5.8532 | 37.0578 | inconclusive | no effect |
| P1-buy | 15m | low | 4h | 11209 | 0 | 3202 | 1650 | -0.0150 | 0.0027 | -0.0177 | -1 | 0.418794 | – | 0.475094 | -0.2300 | [-0.2879, -0.1728] | 6.66 | -79.6342 | 380.9034 | inconclusive | no effect |
| P1-buy | 15m | low | 1d | 11174 | 0 | 3195 | 649 | -0.0120 | 0.0077 | -0.0197 | -1 | 0.710297 | – | 0.526083 | -0.1073 | [-0.1606, -0.0505] | 2.62 | -14.6135 | 75.1613 | inconclusive | no effect |
| P1-buy | 15m | medium | 4h | 8251 | 0 | 2541 | 1453 | 0.0238 | 0.0131 | 0.0107 | +1 | 0.928051 | – | 0.817331 | -0.2098 | [-0.2602, -0.1565] | 5.86 | -63.9489 | 310.9513 | inconclusive | no effect |
| P1-buy | 15m | medium | 1d | 8159 | 0 | 2525 | 706 | -0.0222 | 0.0069 | -0.0291 | -1 | 0.539689 | – | 0.618703 | -0.0658 | [-0.1303, -0.0018] | 2.85 | -9.7388 | 56.6381 | inconclusive | no effect |
| P1-buy | 15m | high | 4h | 6447 | 0 | 2013 | 1202 | -0.0002 | -0.0063 | 0.0061 | +1 | 0.723649 | – | 0.881201 | -0.1479 | [-0.2156, -0.0811] | 4.85 | -37.3036 | 179.3782 | inconclusive | no effect |
| P1-buy | 15m | high | 1d | 6393 | 0 | 2002 | 633 | -0.0106 | -0.0149 | 0.0043 | +1 | 0.903673 | – | 0.933781 | -0.0734 | [-0.1587, 0.0118] | 2.55 | -9.7533 | 50.8249 | inconclusive | no effect |
| P1-buy | 15m | extreme | 4h | 5837 | 0 | 1849 | 1044 | -0.0478 | -0.0075 | -0.0403 | -1 | 0.478916 | – | 0.447214 | -0.1712 | [-0.2166, -0.1245] | 4.21 | -37.5030 | 182.4808 | inconclusive | no effect |
| P1-buy | 15m | extreme | 1d | 5794 | 0 | 1840 | 494 | -0.0535 | -0.0279 | -0.0256 | -1 | 0.609345 | – | 0.865171 | -0.0835 | [-0.1518, -0.0182] | 1.99 | -8.6590 | 46.4072 | inconclusive | no effect |
| P1-buy | 1h | low | 4h | 9226 | 0 | 968 | 793 | -0.0335 | -0.0030 | -0.0305 | -1 | 0.470596 | – | 0.270910 | -0.2169 | [-0.2973, -0.1345] | 3.20 | -36.0812 | 174.8378 | inconclusive | no effect |
| P1-buy | 1h | low | 1d | 9217 | 0 | 967 | 439 | -0.0854 | 0.0123 | -0.0977 | -1 | 0.259934 | – | 0.267820 | -0.1110 | [-0.1848, -0.0367] | 1.77 | -10.2223 | 54.9507 | counter | no effect |
| P1-buy | 1h | medium | 4h | 7128 | 0 | 778 | 673 | 0.0665 | 0.0232 | 0.0433 | +1 | 0.588983 | – | 0.758031 | -0.1935 | [-0.2671, -0.1191] | 2.72 | -27.3186 | 133.8564 | inconclusive | no effect |
| P1-buy | 1h | medium | 1d | 7021 | 0 | 774 | 433 | -0.0400 | 0.0144 | -0.0545 | -1 | 0.681589 | – | 0.655559 | -0.0880 | [-0.1820, 0.0079] | 1.75 | -7.9924 | 55.3538 | inconclusive | no effect |
| P1-buy | 1h | high | 4h | 5319 | 0 | 573 | 505 | 0.0389 | -0.0037 | 0.0426 | +1 | 0.463528 | – | 0.990457 | -0.1917 | [-0.2939, -0.0882] | 2.04 | -20.3141 | 97.5594 | inconclusive | no effect |
| P1-buy | 1h | high | 1d | 5262 | 0 | 570 | 354 | – | – | – | – | – | – | – | – | – | – | – | – | – | not measurable |
| P1-buy | 1h | extreme | 4h | 4368 | 0 | 496 | 412 | 0.0137 | -0.0146 | 0.0284 | +1 | 0.939255 | – | 0.947729 | -0.1613 | [-0.2717, -0.0560] | 1.66 | -13.9451 | 69.3256 | inconclusive | no effect |
| P1-buy | 1h | extreme | 1d | 4360 | 0 | 493 | 288 | – | – | – | – | – | – | – | – | – | – | – | – | – | not measurable |
| P1-sell | 5m | low | 4h | 14631 | 0 | 8006 | 2315 | -0.0409 | -0.0056 | -0.0353 | -1 | 0.292044 | – | 0.759635 | -0.2289 | [-0.2740, -0.1852] | 9.34 | -111.1495 | 530.6121 | counter | no effect |
| P1-sell | 5m | low | 1d | 14600 | 0 | 7988 | 746 | -0.0436 | -0.0141 | -0.0294 | -1 | 0.337550 | – | 0.650083 | -0.1259 | [-0.1961, -0.0614] | 3.01 | -19.6986 | 98.5679 | counter | no effect |
| P1-sell | 5m | medium | 4h | 11423 | 0 | 6484 | 2214 | -0.0089 | -0.0148 | 0.0059 | +1 | 0.881305 | – | 0.850517 | -0.2210 | [-0.2716, -0.1693] | 8.93 | -102.6454 | 491.8029 | inconclusive | no effect |
| P1-sell | 5m | medium | 1d | 11375 | 0 | 6453 | 847 | -0.0375 | -0.0015 | -0.0360 | -1 | 0.484002 | – | 0.648059 | -0.0594 | [-0.1179, -0.0002] | 3.42 | -10.5518 | 55.9558 | counter | no effect |
| P1-sell | 5m | high | 4h | 9455 | 0 | 5414 | 1918 | 0.0056 | -0.0019 | 0.0075 | +1 | 0.715349 | – | 0.731225 | -0.1714 | [-0.2259, -0.1200] | 7.74 | -68.9865 | 334.2595 | inconclusive | no effect |
| P1-sell | 5m | high | 1d | 9405 | 0 | 5383 | 779 | 0.0052 | 0.0092 | -0.0039 | -1 | 0.897041 | – | 0.812113 | -0.0483 | [-0.1161, 0.0209] | 3.14 | -7.8928 | 44.2548 | inconclusive | no effect |
| P1-sell | 5m | extreme | 4h | 9069 | 0 | 5319 | 1728 | 0.0043 | 0.0039 | 0.0004 | +1 | 0.810803 | – | 0.799079 | -0.1341 | [-0.1803, -0.0908] | 6.97 | -48.6105 | 236.6626 | inconclusive | no effect |
| P1-sell | 5m | extreme | 1d | 9028 | 0 | 5297 | 608 | -0.0081 | 0.0198 | -0.0279 | -1 | 0.732575 | – | 0.894487 | -0.1232 | [-0.2121, -0.0342] | 2.45 | -15.7109 | 77.6514 | inconclusive | no effect |
| P1-sell | 15m | low | 4h | 13297 | 0 | 3652 | 1785 | -0.0487 | -0.0071 | -0.0416 | -1 | 0.187836 | – | 0.386694 | -0.2161 | [-0.2762, -0.1567] | 7.20 | -80.9212 | 387.9667 | counter | no effect |
| P1-sell | 15m | low | 1d | 13270 | 0 | 3644 | 674 | -0.0589 | -0.0169 | -0.0419 | -1 | 0.218500 | – | 0.417674 | -0.1113 | [-0.1708, -0.0510] | 2.72 | -15.7448 | 80.7822 | counter | no effect |
| P1-sell | 15m | medium | 4h | 10434 | 0 | 2936 | 1619 | -0.0493 | -0.0166 | -0.0327 | -1 | 0.572145 | – | 0.636029 | -0.2231 | [-0.2613, -0.1825] | 6.53 | -75.7874 | 364.1718 | inconclusive | no effect |
| P1-sell | 15m | medium | 1d | 10384 | 0 | 2920 | 747 | -0.0423 | 0.0007 | -0.0429 | -1 | 0.597645 | – | 0.868941 | -0.0490 | [-0.1087, 0.0144] | 3.01 | -7.6799 | 39.2615 | inconclusive | no effect |
| P1-sell | 15m | high | 4h | 8780 | 0 | 2518 | 1377 | 0.0032 | 0.0032 | -0.0000 | -1 | 0.986279 | – | 0.646515 | -0.2095 | [-0.2572, -0.1619] | 5.56 | -60.5264 | 289.3501 | inconclusive | no effect |
| P1-sell | 15m | high | 1d | 8716 | 0 | 2502 | 667 | -0.0398 | 0.0094 | -0.0492 | -1 | 0.391364 | – | 0.518013 | -0.0651 | [-0.1435, 0.0173] | 2.69 | -9.1064 | 52.3746 | counter | no effect |
| P1-sell | 15m | extreme | 4h | 8629 | 0 | 2453 | 1247 | 0.0009 | 0.0057 | -0.0048 | -1 | 0.592401 | – | 0.548933 | -0.2329 | [-0.2769, -0.1896] | 5.03 | -60.9373 | 294.1604 | inconclusive | no effect |
| P1-sell | 15m | extreme | 1d | 8567 | 0 | 2436 | 533 | -0.0228 | 0.0221 | -0.0449 | -1 | 0.654199 | – | 0.840231 | -0.1027 | [-0.1849, -0.0238] | 2.15 | -11.4859 | 64.6442 | inconclusive | no effect |
| P1-sell | 1h | low | 4h | 12396 | 0 | 1171 | 923 | -0.0441 | -0.0044 | -0.0397 | -1 | 0.297794 | – | 0.437694 | -0.2297 | [-0.3038, -0.1525] | 3.72 | -44.4769 | 212.6233 | inconclusive | no effect |
| P1-sell | 1h | low | 1d | 12342 | 0 | 1167 | 468 | -0.1138 | -0.0206 | -0.0932 | -1 | 0.068224 | – | 0.124486 | -0.0609 | [-0.1405, 0.0178] | 1.89 | -5.9812 | 32.4383 | counter | no effect |
| P1-sell | 1h | medium | 4h | 9857 | 0 | 970 | 802 | -0.0451 | -0.0168 | -0.0284 | -1 | 0.968991 | – | 0.886863 | -0.1906 | [-0.2534, -0.1289] | 3.24 | -32.0629 | 158.2042 | inconclusive | no effect |
| P1-sell | 1h | medium | 1d | 9814 | 0 | 964 | 500 | -0.0710 | 0.0028 | -0.0738 | -1 | 0.535629 | – | 0.900951 | -0.0415 | [-0.1034, 0.0230] | 2.02 | -4.3537 | 24.5043 | inconclusive | no effect |
| P1-sell | 1h | high | 4h | 8417 | 0 | 891 | 740 | 0.0132 | 0.0017 | 0.0115 | +1 | 0.673865 | – | 0.747707 | -0.2234 | [-0.2948, -0.1517] | 2.99 | -34.6875 | 166.7757 | inconclusive | no effect |
| P1-sell | 1h | high | 1d | 8380 | 0 | 886 | 460 | -0.0414 | -0.0007 | -0.0407 | -1 | 0.499324 | – | 0.838185 | -0.0749 | [-0.1571, 0.0090] | 1.86 | -7.2323 | 45.4725 | inconclusive | no effect |
| P1-sell | 1h | extreme | 4h | 10228 | 0 | 878 | 705 | 0.0440 | 0.0057 | 0.0383 | +1 | 0.549425 | – | 0.316872 | -0.1664 | [-0.2428, -0.0896] | 2.84 | -24.6121 | 126.4648 | inconclusive | no effect |
| P1-sell | 1h | extreme | 1d | 10181 | 0 | 870 | 397 | -0.0086 | 0.0234 | -0.0320 | -1 | 0.697553 | – | 0.927829 | -0.0943 | [-0.1847, -0.0016] | 1.60 | -7.8516 | 48.8480 | inconclusive | no effect |

#### Summary

- measurable: 46 of 48; replicated: 0 of 48 (BH at q = 0.05, m = 48, unread contexts at p = 1).

