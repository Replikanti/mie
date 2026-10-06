#!/usr/bin/env python3
"""Reference values for `volatility.atr.1h@1` and `volatility.regime.1h@1`.

An independent, exact transcription of the Pine Script v5 definitions the
features follow (ADR-033), used to generate the golden fixture of
`crates/mie-domain/tests/atr_reference.rs`. Python 3 standard library only;
CI does not run it.

Definitions (Pine v5), over consecutive 1h bars:

- `ta.tr(true)`: `high - low` on the first bar (no previous close), else
  `max(high - low, abs(high - close[1]), abs(low - close[1]))`.
- `ta.rma(src, 14)`: `ta.sma(src, 14)` on the first bar where it exists (the
  14th), then `(src + 13 * rma[1]) / 14`.
- `ta.atr(14)` = `ta.rma(ta.tr(true), 14)`.
- `ta.percentrank(atr, 200)`: `100 * #{i in 1..200 : atr[i] <= atr} / 200`,
  `na` until 200 previous values exist.
- Labels (ADR-017 bands, upper-closed for fractional values): LOW `[0, 25]`,
  MEDIUM `(25, 50]`, HIGH `(50, 75]`, EXTREME `(75, 100]`.

All arithmetic is exact (`fractions.Fraction`). Output: one TSV row per bar,
`open_time_ms <TAB> atr <TAB> percentile <TAB> label`, where `atr` is the
exact value rounded half to even to 12 decimals and `-` marks a value that is
still warming up.

Usage: atr_regime_reference.py <klines.csv>   (writes the TSV to stdout)

The input is a CSV with the header `open_time,open,high,low,close`.
"""

import csv
import sys
from fractions import Fraction

HOUR_MS = 3_600_000
ATR_LENGTH = 14
LOOKBACK = 200
# The Rust ATR is fixed point at 1e-8 and stays within 7 units of the exact
# value (ADR-033 D2). A compared pair closer than twice that could rank
# differently there, so the fixture must not contain one.
MIN_PAIR_GAP = Fraction(14, 10**8)
ATR_DECIMALS = 12


def read_bars(path):
    with open(path, newline="") as handle:
        rows = list(csv.reader(handle))
    if rows[0] != ["open_time", "open", "high", "low", "close"]:
        sys.exit(f"unexpected header: {rows[0]}")
    bars = []
    for row in rows[1:]:
        open_time = int(row[0])
        high, low, close = (Fraction(value) for value in row[2:5])
        bars.append((open_time, high, low, close))
    for previous, current in zip(bars, bars[1:]):
        if current[0] - previous[0] != HOUR_MS:
            sys.exit(f"bars are not consecutive 1h bars at {current[0]}")
    return bars


def true_ranges(bars):
    previous_close = None
    for _, high, low, close in bars:
        if previous_close is None:
            yield high - low
        else:
            yield max(high - low, abs(high - previous_close), abs(low - previous_close))
        previous_close = close


def atr_series(bars):
    atr = []
    window = []
    for tr in true_ranges(bars):
        if atr and atr[-1] is not None:
            atr.append((tr + (ATR_LENGTH - 1) * atr[-1]) / ATR_LENGTH)
            continue
        window.append(tr)
        atr.append(sum(window) / ATR_LENGTH if len(window) == ATR_LENGTH else None)
    return atr


def percent_ranks(atr):
    ranks = []
    min_gap = None
    for t, current in enumerate(atr):
        previous = atr[t - LOOKBACK : t] if t >= LOOKBACK else []
        if current is None or len(previous) < LOOKBACK or None in previous:
            ranks.append(None)
            continue
        for value in previous:
            gap = abs(value - current)
            if min_gap is None or gap < min_gap:
                min_gap = gap
        at_or_below = sum(1 for value in previous if value <= current)
        ranks.append(Fraction(100 * at_or_below, LOOKBACK))
    return ranks, min_gap


def label(percentile):
    if percentile <= 25:
        return "LOW"
    if percentile <= 50:
        return "MEDIUM"
    if percentile <= 75:
        return "HIGH"
    return "EXTREME"


def fixed(value, decimals):
    """`value` rounded half to even to `decimals` places, as a decimal string."""
    scaled = round(value * 10**decimals)  # Fraction.__round__ is half-even.
    sign = "-" if scaled < 0 else ""
    whole, part = divmod(abs(scaled), 10**decimals)
    return f"{sign}{whole}.{part:0{decimals}d}"


def percentile_text(percentile):
    # k / 2 for k in 0..=200: one decimal is exact.
    return fixed(percentile, 1)


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: atr_regime_reference.py <klines.csv>")
    path = sys.argv[1]
    bars = read_bars(path)
    atr = atr_series(bars)
    ranks, min_gap = percent_ranks(atr)
    if min_gap is None or min_gap < MIN_PAIR_GAP:
        sys.exit(f"near tie: two compared ATR values differ by {min_gap} < {MIN_PAIR_GAP}")
    labels = {label(rank) for rank in ranks if rank is not None}
    if labels != {"LOW", "MEDIUM", "HIGH", "EXTREME"}:
        sys.exit(f"not every label occurs: {sorted(labels)}")

    out = sys.stdout
    out.write("# Reference for volatility.atr.1h@1 and volatility.regime.1h@1 (ADR-033)\n")
    out.write(f"# input: {path.rsplit('/', 1)[-1]} ({len(bars)} consecutive 1h bars)\n")
    out.write("# atr: Pine v5 ta.atr(14) = ta.rma(ta.tr(true), 14), exact, rounded half to even to 12 decimals\n")
    out.write("# percentile: Pine v5 ta.percentrank(atr, 200); label: ADR-017 bands, upper-closed\n")
    out.write(f"# smallest compared ATR difference: {fixed(min_gap, ATR_DECIMALS)}\n")
    out.write("# open_time_ms\tatr\tpercentile\tlabel\n")
    for (open_time, *_), value, rank in zip(bars, atr, ranks):
        atr_text = "-" if value is None else fixed(value, ATR_DECIMALS)
        if rank is None:
            out.write(f"{open_time}\t{atr_text}\t-\t-\n")
        else:
            out.write(f"{open_time}\t{atr_text}\t{percentile_text(rank)}\t{label(rank)}\n")


if __name__ == "__main__":
    main()
