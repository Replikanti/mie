# Test fixtures

Public Binance USDⓈ-M market data for BTCUSDT; no keys, tokens or account
data.

| File | Provenance |
|---|---|
| `btcusdt-1h-2024-07-08.csv` | BTCUSDT perpetual 1h klines, 2024-07-01T00:00Z to 2024-08-31T23:00Z (1 488 consecutive bars), from the Binance public data archive; the first five columns, verbatim |
| `atr-regime-reference.tsv` | Output of `tools/reference/atr_regime_reference.py` on the CSV above: the reference for `tests/atr_reference.rs` (ADR-033) |
| `btcusdt-profile-histograms.tsv` | Dense 10 USDT volume histograms of 12 UTC days (the 15th of each month, 2025-10 to 2026-09), `<day> <TAB> <bin lower edge> <TAB> <volume>`, 2 980 rows: the input of `tests/profile_reference.rs` (ADR-036, rule A5) |
| `profile-reference.tsv` | `<day> <TAB> <levels>`, the `VolumeProfile` `Display` fields from `start=` to `lvn=` per day: the reference for `tests/profile_reference.rs` |

## Klines

Source archives, verified against their `.CHECKSUM` files:

| Archive | SHA-256 |
|---|---|
| `https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-07.zip` | `920aacb84b2745ee8f31806a753cdc5b2d672e6814d9c50c0bf524ad45eb5611` |
| `https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-08.zip` | `8d8465eb4c8a4fd05ebc0126e4c14a217edb2039441546bf095229e975268de6` |

Extraction: unzip both archives, then keep the header line and the columns
`open_time,open,high,low,close`:

```sh
{ echo open_time,open,high,low,close
  tail -n +2 BTCUSDT-1h-2024-07.csv
  tail -n +2 BTCUSDT-1h-2024-08.csv
} | cut -d, -f1-5 > btcusdt-1h-2024-07-08.csv
```

The window contains the 2024-08-05 volatility shock, so all four regime
labels occur.

## Reference

Generated with Python 3.14.7 (standard library only), from the repository
root:

```sh
python3 tools/reference/atr_regime_reference.py \
  crates/mie-domain/tests/fixtures/btcusdt-1h-2024-07-08.csv \
  > crates/mie-domain/tests/fixtures/atr-regime-reference.tsv
```

The generator checks that the bars are consecutive, that every pair of ATR
values the percentile compares differs by at least 14 units of 1e-8 (twice
the fixed-point error bound of ADR-033 D2, so rounding cannot flip a rank;
the smallest difference is in the TSV header) and that every label occurs.

## Volume profile

Both files are the output of `tools/reference/volume_profile_reference.py`
(an independent transcription of ADR-036 decisions 1 and 4–6, standard
library only) on the BTCUSDT perpetual daily aggTrades archives of
`https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/`.
Each day D reads the zips of D−1, D and D+1, keeps the rows whose
`transact_time` falls in D and drops repeated rows as `ArchiveReplay`
does. The generator checks every zip's sha256 against the one the archive
backfill (#12, ADR-034) recorded in its import ledger:

| Archive | SHA-256 |
|---|---|
| `BTCUSDT-aggTrades-2025-10-14.zip` | `d528e40c49850d0e1e37d5ca3369dc0cd3d72935090f4a3c180816cad0409c98` |
| `BTCUSDT-aggTrades-2025-10-15.zip` | `f4e9156abf5cfa27f95da2dcecdf09a2c81fd9ad9d8790061f0b4cacfd5ff46d` |
| `BTCUSDT-aggTrades-2025-10-16.zip` | `dd7902fbb7eadf68b719498ab9ba80834dffc142bbd70b91d5038001ec8d1559` |
| `BTCUSDT-aggTrades-2025-11-14.zip` | `fd1e7e07833ac3f1fd759d61880f1219115e9e333a467912c0272c039f6458b2` |
| `BTCUSDT-aggTrades-2025-11-15.zip` | `9f061971e082994a4b4b5ca4bc4f328c3bbb42522b85461cc495769b4032209b` |
| `BTCUSDT-aggTrades-2025-11-16.zip` | `290a95cb47dc37f7e3be27b341d92129b5c70c95d432d8045435400891689010` |
| `BTCUSDT-aggTrades-2025-12-14.zip` | `fa4d1dbf985682e086060a8f6d7535dc7c25d957b716a21ae6fd9aecf3d3f3a9` |
| `BTCUSDT-aggTrades-2025-12-15.zip` | `5f0d96a12f5406b66d531be5a09c766c47144a1bc44ec030493cb5aacfed3b2f` |
| `BTCUSDT-aggTrades-2025-12-16.zip` | `a2bb4cd164e071b0bfa6224872d88721590b74d7387ea5b826e1c8b44cd9d41c` |
| `BTCUSDT-aggTrades-2026-01-14.zip` | `bfaaae59b290421aae3bf9cd2c82ce0f77d6dc007198cb48e4864759225e091c` |
| `BTCUSDT-aggTrades-2026-01-15.zip` | `04b1759dc32f8e623e65b449fd79fd109d273b97a4a51c1824cfa25386aae192` |
| `BTCUSDT-aggTrades-2026-01-16.zip` | `4bd7a0bbdecfecef1b1744090fe04357ca22a28855ac407b6737165cc50232f6` |
| `BTCUSDT-aggTrades-2026-02-14.zip` | `7a10d136e1bfb6781f3f9f542869d790edb6a77366fcf6d12637f8565a884392` |
| `BTCUSDT-aggTrades-2026-02-15.zip` | `6627fa8b51a0d976dfe5c6bb064596cf0270a4314ea963f04af8f68ae72e8592` |
| `BTCUSDT-aggTrades-2026-02-16.zip` | `585d1df2c6477a7572d023c65c4e8a72aafec07d17f78f40bfa01dd1178455d4` |
| `BTCUSDT-aggTrades-2026-03-14.zip` | `9886c685718b3fb34d086c24b3a02cfc3d48040dd7ae3584793fc3c939c0cd4c` |
| `BTCUSDT-aggTrades-2026-03-15.zip` | `ff2acc4a3e70b735b159d4da93d636cf7797300a9772b96c3c0c2472a8f348e3` |
| `BTCUSDT-aggTrades-2026-03-16.zip` | `74c8c170ca8e88b57f2d96c2bbdf6d09e499b6e2ef079f3ebfd97c1657fe31b3` |
| `BTCUSDT-aggTrades-2026-04-14.zip` | `acd7d6aea8f74e0b7a3714a4921ba1054aabbf85edc821ad274419bf70ec47fd` |
| `BTCUSDT-aggTrades-2026-04-15.zip` | `d0e6dca6394393350746553d56d571004f133c1c421f1e3cdd1c8d23b429004d` |
| `BTCUSDT-aggTrades-2026-04-16.zip` | `0b275a521bc484c5d8c726b3c1bdbc34c0d7c6c649a3fcfce04c290ffcfc0921` |
| `BTCUSDT-aggTrades-2026-05-14.zip` | `048de3d73cbbb5108c06a875a7eeeabcb1b03b5a6328279508f3af682bdd063e` |
| `BTCUSDT-aggTrades-2026-05-15.zip` | `a7b3035eb14ef9f8bca58523ca75ef7161538d232dade5b2d86c12e482a0ecbe` |
| `BTCUSDT-aggTrades-2026-05-16.zip` | `cd0ee95140f81c07a0a0c099b8921de0ea3890f9f149b5f8f125c606c4d9da94` |
| `BTCUSDT-aggTrades-2026-06-14.zip` | `afcb1fbd4240c0d4f03e98e58817f73857da78f9260887431016c30a4513cdea` |
| `BTCUSDT-aggTrades-2026-06-15.zip` | `8fd6768f10c80a11e54279d4584b1dbe468a3f8a42bad4083728f606ca12dd9a` |
| `BTCUSDT-aggTrades-2026-06-16.zip` | `37f5349d10c74af64369dbf6d1be6eff739c264ffda8f32c5cc8578af3d7a2dd` |
| `BTCUSDT-aggTrades-2026-07-14.zip` | `9a149e365bf86ba6056e18d30f386206b78662ce60e332421525d9780eccf7ce` |
| `BTCUSDT-aggTrades-2026-07-15.zip` | `c87fba7741fe9dcee0c89edb8b98c4a5bfe228aff08c36a3005366ad155844f7` |
| `BTCUSDT-aggTrades-2026-07-16.zip` | `5f14e39d87f02b74b239587702431bc87460ca1eb4e18f7f030f482a275d36ca` |
| `BTCUSDT-aggTrades-2026-08-14.zip` | `426d9fe8c5b73b136e014236bf4b330fa2eee26d9b0d790e4acf631fd104810c` |
| `BTCUSDT-aggTrades-2026-08-15.zip` | `dec683aebfbde90dfba9255ebd36dcb970a6441a9e7e7785b5ebad3c0b70c7a9` |
| `BTCUSDT-aggTrades-2026-08-16.zip` | `31bace3652e9219e108a9af3719ef9d10ad68fcea3374b9ba5b1d96e50eec4c9` |
| `BTCUSDT-aggTrades-2026-09-14.zip` | `2deabfa8cf285f72b7e7b4fc2a36e1eaa0413fd8da982ea0b94bd7a8bd947529` |
| `BTCUSDT-aggTrades-2026-09-15.zip` | `4c0a707148a13d50e39364b5339840adcaaefb351a9fa24d7bf77166aae366ad` |
| `BTCUSDT-aggTrades-2026-09-16.zip` | `7913ca54ac75492ba4e812d0ff02ac74f1d44b8fbdcd557efb314504ddbac25c` |

Generated with Python 3.14.7, from the repository root, with the zips in
`<archive dir>` and the backfill's import ledger in `<ledger dir>`:

```sh
days="2025-10-15 2025-11-15 2025-12-15 2026-01-15 2026-02-15 2026-03-15
      2026-04-15 2026-05-15 2026-06-15 2026-07-15 2026-08-15 2026-09-15"
out=<output dir>
for day in $days; do
  python3 tools/reference/volume_profile_reference.py \
    "$day" <archive dir> <ledger dir> > "$out/$day.tsv"
done
for day in $days; do grep -P '\t' "$out/$day.tsv"; done \
  > crates/mie-domain/tests/fixtures/btcusdt-profile-histograms.tsv
for day in $days; do
  printf '%s\t%s\n' "$day" "$(tail -n 1 "$out/$day.tsv")"
done > crates/mie-domain/tests/fixtures/profile-reference.tsv
```

Each day's histogram rows are the dense range from its lowest to its
highest bin with volume; on these 12 days no bin inside the range is
empty. The levels line of every day equals the first 13 fields of the
engine's `profile.volume.prior_day@1` line for that day from a full replay
of the backfill (#79, rule A5). 2026-08-15 is a quiet Saturday (29 bins)
and has no LVN.
