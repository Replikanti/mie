# #205 P1 replication — coverage

- Issue: #205. Pre-registered by [ADR-064](../adr/064-p1-replication-pre-registration.md)
  (D5; the run and its inputs are in its *Pre-registration record*).
- Run: 2026-11-09, `coverage_p1_replication`, which reads no outcome. This
  is the rerun after the review amendment and its recheck fix (ADR-064
  *Pre-registration record*); the runs from `72371e3` and `d9df826`
  printed the same counts.
- Code: the test binary is a release build of
  `57699b842cbc291899b3f33e084f3f2241e2c834`, the amended commit, clean
  (its `test binary code` line).
  The dataset was built from `72371e363248db70b8a2b2d4445291859d7b311e`,
  the PR's first commit (its `code` line); the amendment changed no code
  the build runs.
- Queries: [`028-order-flow-interactions.sql`](028-order-flow-interactions.sql),
  SHA-256 `dd32d605081d6c80992655e16f453d143ee4e3ba5b17f4bbe399fcee0e7144a1`;
  [`205-p1-replication.sql`](205-p1-replication.sql), SHA-256
  `5e742fed2fba222bb29f501de22bddb3c5d2a399186c495fd574118df7ec74c3`.
- Frozen decisions of `replication.rs`: SHA-256
  `ece00cf22aaaa527f2c7f83fe5eb08978267c5599eac203781de5e39ccbd1f7c`.

## Dataset

Records = replay = 2020-01-01 … 2024-09-30 (`1577836800000..1727740800000`
ms), `aggTrades` only, built once from the replication's own archive store:

| Manifest line | Value |
|---|---|
| `source` | `archive binance-archive/BTCUSDT streams aggTrades correction fold` |
| `content` | `e985ea3e9efa9cc9aaf15dbc013fe76eb76711e811a4d3f414e79b591402049f` |
| `data` | `7e92345c663e72e096cbdf2921f60eaa8d9cbae291cd2f1f82bdd64306d27d10` |
| `engine` | `4c844593779da2e3-1` |
| `sampling` | `conditional.sampling@1 2c71fff23e8b0415` |
| `labeling` | `conditional.labels@2 6f90bff079e746f2` |
| `events` | 2 327 594 373, 0 rejections |
| `code` | `git:72371e363248db70b8a2b2d4445291859d7b311e` |

| File | Rows | SHA-256 |
|------|-----:|---------|
| `contexts.parquet` | 2 638 166 | `205a82462872954569738254ea9e0c9b053b1966f20a3fc0e2cac5f2ec802795` |
| `outcomes.parquet` | 13 190 830 | `d0b88950bc709459e95436283177e0ace7a9c661d587657b0c07a3ac18eb62a7` |
| `passages.parquet` | 75 907 | `effca270243585914bd86dc8b894142536f8f6141633e3c09079a5e1ac4974f9` |
| `effort.parquet` | 7 914 498 | `eeeda049d5bb2b3d9d326c9edc92c84a713c026b36af4f971642d78c26cb637a` |

`mie conditional verify`: PASS.

## Command

From a release build of `57699b8` (`cargo test --release -p mie-cli --test
order_flow_study --no-run`), under a 12 GB memory and 400 % CPU cap:

```sh
MIE_OUTCOME_DATASET=<dataset> <test binary> --ignored --exact --nocapture \
    replication::coverage_p1_replication
```

Wall time 0:56.55 (m:ss), peak RSS 2.5 GB; 1 test ran.

## Summary

- **46 of 48 contexts are measurable** (units ≥ n_min = 385): all 24 at 4h
  and 22 of 24 at 1d.
- **Not measurable:** P1-buy w=1h high 1d (354 units) and P1-buy w=1h
  extreme 1d (288 units). ADR-064 S2 applies: #206 prints only their
  counts, and they enter Benjamini–Hochberg with p = 1, so m stays 48
  (ADR-064 D3).
- No member is dropped from E for an empty stratum in any context.
- Coverage twin mismatches 0.

## Results: the tool's output

The output of `coverage_p1_replication`, from its first line to its last,
with every heading two levels lower and runs of blank lines collapsed; the
test harness's own lines (`running 1 test`, `test … ok`, `test result`)
are left out. No row is dropped and no value is rounded or re-derived.

### P1 replication coverage (#205, ADR-064)

dataset content e985ea3e9efa9cc9aaf15dbc013fe76eb76711e811a4d3f414e79b591402049f, data 7e92345c663e72e096cbdf2921f60eaa8d9cbae291cd2f1f82bdd64306d27d10, engine 4c844593779da2e3-1, conditional.sampling@1 2c71fff23e8b0415, conditional.labels@2 6f90bff079e746f2, code git:72371e363248db70b8a2b2d4445291859d7b311e
source archive binance-archive/BTCUSDT streams aggTrades correction fold; replay (1577836800000, 1727740800000); records (1577836800000, 1727740800000)
test binary code git:57699b842cbc291899b3f33e084f3f2241e2c834

queries docs/research/028-order-flow-interactions.sql sha256 dd32d605081d6c80992655e16f453d143ee4e3ba5b17f4bbe399fcee0e7144a1
queries docs/research/205-p1-replication.sql sha256 5e742fed2fba222bb29f501de22bddb3c5d2a399186c495fd574118df7ec74c3
ADR-064 frozen text sha256 6ed0f6afb3688c99ebc501eb1530e0cd088122fa3e7a1e4a067bc08fd6dcafae
replication.rs frozen decisions sha256 ece00cf22aaaa527f2c7f83fe5eb08978267c5599eac203781de5e39ccbd1f7c

pins: contexts 2638166, baseline 2498200, trigger 139966, effort rows 7914498; races that ran 4h 2634304, 1d 2613978

#### Inputs

c = 0.95, μ = 0.1 R, z = 1.959964, n_min = 385; fee 10 bps round trip per label; window 1577836800000 … 1727740800000 ms, 1735 days, 247.857 weeks. No outcome is read.

#### Horizons

| H | baseline races that ran | baselines with barriers | median half-width (bps) | fee at the median (R) | disjoint windows in the window |
|---|--:|--:|--:|--:|--:|
| 4h | 2494463 | 2494601 | 54.08 | 0.1849 | 10410 |
| 1d | 2475083 | 2476601 | 144.98 | 0.0690 | 1735 |

#### Exclusions before any race is read

| universe | window | records | effort not ready | incomplete coverage |
|---|---|--:|--:|--:|
| baseline | 5m | 2498200 | 1445 | 35 |
| baseline | 15m | 2498200 | 1455 | 106 |
| baseline | 1h | 2498200 | 1500 | 401 |

| pair | window | P1 rows | regime not ready (in no context) |
|---|---|--:|--:|
| P1-buy | 5m | 38421 | 213 |
| P1-buy | 15m | 31960 | 216 |
| P1-buy | 1h | 26138 | 97 |
| P1-sell | 5m | 44820 | 231 |
| P1-sell | 15m | 41371 | 210 |
| P1-sell | 1h | 41126 | 226 |

#### Contexts

| pair | w | regime | H | members that ran | dropped | episodes | units | units/week | measurable | median fee (R) |
|---|---|---|---|--:|--:|--:|--:|--:|---|--:|
| P1-buy | 5m | low | 4h | 13288 | 0 | 7396 | 2320 | 9.36 | yes | 0.1999 |
| P1-buy | 5m | low | 1d | 13259 | 0 | 7378 | 737 | 2.97 | yes | 0.0680 |
| P1-buy | 5m | medium | 4h | 10085 | 0 | 5881 | 2134 | 8.61 | yes | 0.1971 |
| P1-buy | 5m | medium | 1d | 10025 | 0 | 5850 | 830 | 3.35 | yes | 0.0704 |
| P1-buy | 5m | high | 4h | 7888 | 0 | 4705 | 1787 | 7.21 | yes | 0.1818 |
| P1-buy | 5m | high | 1d | 7847 | 0 | 4682 | 751 | 3.03 | yes | 0.0717 |
| P1-buy | 5m | extreme | 4h | 6947 | 0 | 4274 | 1588 | 6.41 | yes | 0.1676 |
| P1-buy | 5m | extreme | 1d | 6906 | 0 | 4253 | 598 | 2.41 | yes | 0.0713 |
| P1-buy | 15m | low | 4h | 11209 | 0 | 3202 | 1650 | 6.66 | yes | 0.2021 |
| P1-buy | 15m | low | 1d | 11174 | 0 | 3195 | 649 | 2.62 | yes | 0.0676 |
| P1-buy | 15m | medium | 4h | 8251 | 0 | 2541 | 1453 | 5.86 | yes | 0.2021 |
| P1-buy | 15m | medium | 1d | 8159 | 0 | 2525 | 706 | 2.85 | yes | 0.0704 |
| P1-buy | 15m | high | 4h | 6447 | 0 | 2013 | 1202 | 4.85 | yes | 0.1785 |
| P1-buy | 15m | high | 1d | 6393 | 0 | 2002 | 633 | 2.55 | yes | 0.0701 |
| P1-buy | 15m | extreme | 4h | 5837 | 0 | 1849 | 1044 | 4.21 | yes | 0.1668 |
| P1-buy | 15m | extreme | 1d | 5794 | 0 | 1840 | 494 | 1.99 | yes | 0.0697 |
| P1-buy | 1h | low | 4h | 9226 | 0 | 968 | 793 | 3.20 | yes | 0.2032 |
| P1-buy | 1h | low | 1d | 9217 | 0 | 967 | 439 | 1.77 | yes | 0.0678 |
| P1-buy | 1h | medium | 4h | 7128 | 0 | 778 | 673 | 2.72 | yes | 0.2024 |
| P1-buy | 1h | medium | 1d | 7021 | 0 | 774 | 433 | 1.75 | yes | 0.0705 |
| P1-buy | 1h | high | 4h | 5319 | 0 | 573 | 505 | 2.04 | yes | 0.1814 |
| P1-buy | 1h | high | 1d | 5262 | 0 | 570 | 354 | 1.43 | no | 0.0723 |
| P1-buy | 1h | extreme | 4h | 4368 | 0 | 496 | 412 | 1.66 | yes | 0.1656 |
| P1-buy | 1h | extreme | 1d | 4360 | 0 | 493 | 288 | 1.16 | no | 0.0689 |
| P1-sell | 5m | low | 4h | 14631 | 0 | 8006 | 2315 | 9.34 | yes | 0.2013 |
| P1-sell | 5m | low | 1d | 14600 | 0 | 7988 | 746 | 3.01 | yes | 0.0679 |
| P1-sell | 5m | medium | 4h | 11423 | 0 | 6484 | 2214 | 8.93 | yes | 0.2001 |
| P1-sell | 5m | medium | 1d | 11375 | 0 | 6453 | 847 | 3.42 | yes | 0.0708 |
| P1-sell | 5m | high | 4h | 9455 | 0 | 5414 | 1918 | 7.74 | yes | 0.1814 |
| P1-sell | 5m | high | 1d | 9405 | 0 | 5383 | 779 | 3.14 | yes | 0.0719 |
| P1-sell | 5m | extreme | 4h | 9069 | 0 | 5319 | 1728 | 6.97 | yes | 0.1677 |
| P1-sell | 5m | extreme | 1d | 9028 | 0 | 5297 | 608 | 2.45 | yes | 0.0714 |
| P1-sell | 15m | low | 4h | 13297 | 0 | 3652 | 1785 | 7.20 | yes | 0.2035 |
| P1-sell | 15m | low | 1d | 13270 | 0 | 3644 | 674 | 2.72 | yes | 0.0685 |
| P1-sell | 15m | medium | 4h | 10434 | 0 | 2936 | 1619 | 6.53 | yes | 0.2013 |
| P1-sell | 15m | medium | 1d | 10384 | 0 | 2920 | 747 | 3.01 | yes | 0.0710 |
| P1-sell | 15m | high | 4h | 8780 | 0 | 2518 | 1377 | 5.56 | yes | 0.1835 |
| P1-sell | 15m | high | 1d | 8716 | 0 | 2502 | 667 | 2.69 | yes | 0.0718 |
| P1-sell | 15m | extreme | 4h | 8629 | 0 | 2453 | 1247 | 5.03 | yes | 0.1703 |
| P1-sell | 15m | extreme | 1d | 8567 | 0 | 2436 | 533 | 2.15 | yes | 0.0716 |
| P1-sell | 1h | low | 4h | 12396 | 0 | 1171 | 923 | 3.72 | yes | 0.2059 |
| P1-sell | 1h | low | 1d | 12342 | 0 | 1167 | 468 | 1.89 | yes | 0.0690 |
| P1-sell | 1h | medium | 4h | 9857 | 0 | 970 | 802 | 3.24 | yes | 0.2011 |
| P1-sell | 1h | medium | 1d | 9814 | 0 | 964 | 500 | 2.02 | yes | 0.0711 |
| P1-sell | 1h | high | 4h | 8417 | 0 | 891 | 740 | 2.99 | yes | 0.1825 |
| P1-sell | 1h | high | 1d | 8380 | 0 | 886 | 460 | 1.86 | yes | 0.0727 |
| P1-sell | 1h | extreme | 4h | 10228 | 0 | 878 | 705 | 2.84 | yes | 0.1712 |
| P1-sell | 1h | extreme | 1d | 10181 | 0 | 870 | 397 | 1.60 | yes | 0.0725 |

#### Summary

- 4h: 24 of 24 contexts measurable (units ≥ 385).
- 1d: 22 of 24 contexts measurable (units ≥ 385).
- All: 46 of 48; the rest are `not measurable` and are not read (ADR-064 S2).

coverage twin mismatches 0
