# Test fixtures

Public Binance USDⓈ-M market data for BTCUSDT; no keys, tokens or account
data.

| File | Provenance |
|---|---|
| `btcusdt-1h-2024-07-08.csv` | BTCUSDT perpetual 1h klines, 2024-07-01T00:00Z to 2024-08-31T23:00Z (1 488 consecutive bars), from the Binance public data archive; the first five columns, verbatim |
| `atr-regime-reference.tsv` | Output of `tools/reference/atr_regime_reference.py` on the CSV above: the reference for `tests/atr_reference.rs` (ADR-033) |

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
