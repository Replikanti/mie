#!/usr/bin/env python3
"""Reference values for `profile.volume.prior_day@1` on one real UTC day.

An independent, exact transcription of ADR-036 decisions 1 and 4-6, written
from the ADR text and not from `crates/mie-domain/src/profile.rs`, used for
rule A5 of #79 and for the fixtures of `profile_reference.rs`. Python 3
standard library only; CI does not run it.

Definitions (ADR-036), with b = 10 USDT and all values exact integers at
1e-8 (ADR-027):

- Bins (decision 1): a trade with a positive quantity adds its exact
  quantity to bin `k = floor(price / b)`, which covers `[k*b, (k+1)*b)`. The
  profile range is `[min k, max k]` over bins with volume; zero-volume bins
  inside it belong to the dense histogram.
- POC (decision 4): the bin with the most volume; a tie goes to the bin
  closest to the range centre (smallest `|2k - (min + max)|`), then to the
  lower bin. Reported at the bin midpoint.
- Value area (decision 5): start at the POC; compare the next bin above with
  the next bin below and add the larger, both when they are equal, the other
  side once one side is exhausted; stop once `va_volume * 100 >= total * 70`.
  VAL is the lower edge of the lowest value-area bin, VAH the upper edge of
  the highest.
- Nodes (decision 6): smooth with the kernel 1-2-3-2-1, bins outside the
  range counting as 0. A peak (HVN) is a maximal run of equal smoothed
  values whose neighbours on both sides are strictly lower, a virtual 0
  standing beyond the range. A valley (LVN) is a maximal run whose
  neighbours on both sides are strictly higher and inside the range. A run
  `[a, b]` sits at bin `a + (b - a) // 2`. Peak prominence: height minus the
  larger of its two bases, a base being the lowest value from the peak
  outward up to the first strictly higher bin, or 0 when no higher bin lies
  on that side. Valley prominence: the smaller of its two tops minus its
  depth, a top being the highest value outward up to the first strictly
  lower bin or the range edge. A node is kept when
  `prominence * 100 >= max * 10`; it reports its midpoint and
  `floor(1000 * prominence / max)` permille.

Input: the archive zips of D-1, D and D+1 (`BTCUSDT-aggTrades-<day>.zip` in
`<archive dir>`) and the import ledger directory of the backfill (#12). Each
zip's sha256 must equal the one its ledger recorded. Rows are kept when
their `transact_time` falls in D; as `ArchiveReplay` does, rows are taken in
`(transact_time, agg_trade_id)` order and a row whose id is at or below the
last kept id is dropped, so a row in two files counts once.

Output on stdout: one TSV row per bin of the dense 10 USDT histogram,
`<day> <TAB> <bin lower edge> <TAB> <volume>`, then one line with the
`VolumeProfile` `Display` fields from `start=` to `lvn=` (prices and
quantities with 8 decimals), which must equal the first 13 fields of the
engine's `prior_day` line for D.

Usage: volume_profile_reference.py <YYYY-MM-DD> <archive dir> <ledger dir>
"""

import csv
import datetime
import hashlib
import io
import os
import sys
import zipfile
from decimal import Decimal

SCALE = 10**8
DAY_MS = 86_400_000
BIN = 10 * SCALE
VALUE_AREA_PCT = 70
PROMINENCE_PCT = 10
KERNEL = (1, 2, 3, 2, 1)
SYMBOL = "BTCUSDT"
HEADER = [
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
]


def fixed(text):
    """`text` as an exact integer at 1e-8; more precision is an error."""
    scaled = Decimal(text) * SCALE
    if scaled != scaled.to_integral_value():
        sys.exit(f"more than 8 decimals: {text}")
    return int(scaled)


def show(units):
    """An integer at 1e-8 with 8 decimals, as the engine prints it."""
    sign = "-" if units < 0 else ""
    whole, fraction = divmod(abs(units), SCALE)
    return f"{sign}{whole}.{fraction:08d}"


def ledger_sha256(ledger_dir, name):
    path = os.path.join(ledger_dir, "aggTrades", f"{name}.import")
    with open(path) as handle:
        for line in handle:
            key, _, value = line.rstrip("\n").partition(" ")
            if key == "sha256":
                return value
    sys.exit(f"no sha256 in {path}")


def read_zip(path):
    """The rows of one archive zip as `(time, id, price, qty)` integers."""
    with zipfile.ZipFile(path) as archive:
        members = archive.namelist()
        if len(members) != 1:
            sys.exit(f"{path}: expected one member, found {members}")
        with archive.open(members[0]) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="ascii", newline=""))
            rows = []
            for index, row in enumerate(reader):
                if index == 0 and not row[0].isdigit():
                    if row != HEADER:
                        sys.exit(f"{path}: unexpected header {row}")
                    continue
                if len(row) != len(HEADER):
                    sys.exit(f"{path}: row {index} has {len(row)} columns")
                rows.append((int(row[5]), int(row[0]), fixed(row[1]), fixed(row[2])))
    return rows


def day_trades(day, archive_dir, ledger_dir):
    """The deduplicated `(price, qty)` of every trade of `day`."""
    start = (day - datetime.date(1970, 1, 1)).days * DAY_MS
    rows = []
    for offset in (-1, 0, 1):
        name = f"{SYMBOL}-aggTrades-{day + datetime.timedelta(days=offset)}.zip"
        path = os.path.join(archive_dir, name)
        with open(path, "rb") as handle:
            digest = hashlib.sha256(handle.read()).hexdigest()
        if digest != ledger_sha256(ledger_dir, name):
            sys.exit(f"{name}: sha256 {digest} differs from the import ledger")
        rows.extend(read_zip(path))
    rows.sort()
    trades = []
    last_id = None
    for time, agg_id, price, qty in rows:
        if time >= start + DAY_MS:
            break
        if last_id is not None and agg_id <= last_id:
            continue
        last_id = agg_id
        if time >= start:
            trades.append((price, qty))
    return start, trades


def histogram(trades):
    """`(first bin, dense volumes)` of the trades (decision 1)."""
    volume = {}
    for price, qty in trades:
        if qty > 0:
            k = price // BIN
            volume[k] = volume.get(k, 0) + qty
    if not volume:
        sys.exit("the day has no volume")
    low, high = min(volume), max(volume)
    return low, [volume.get(k, 0) for k in range(low, high + 1)]


def point_of_control(v):
    n = len(v)
    most = max(v)
    candidates = [i for i in range(n) if v[i] == most]
    # Closest to the centre, then the lower bin.
    return min(candidates, key=lambda i: (abs(2 * i - (n - 1)), i))


def value_area(v, poc):
    total = sum(v)
    lo = hi = poc
    inside = v[poc]
    while inside * 100 < total * VALUE_AREA_PCT:
        has_below, has_above = lo > 0, hi < len(v) - 1
        below = v[lo - 1] if has_below else None
        above = v[hi + 1] if has_above else None
        if has_below and has_above and below == above:
            lo, hi = lo - 1, hi + 1
            inside += below + above
        elif has_below and (not has_above or below > above):
            lo -= 1
            inside += below
        else:
            hi += 1
            inside += above
    return lo, hi, inside


def smoothed(v):
    reach = len(KERNEL) // 2
    n = len(v)
    return [
        sum(w * v[k + j - reach] for j, w in enumerate(KERNEL) if 0 <= k + j - reach < n)
        for k in range(n)
    ]


def runs(s):
    """Maximal runs `[a, b]` of equal values."""
    a = 0
    while a < len(s):
        b = a
        while b + 1 < len(s) and s[b + 1] == s[a]:
            b += 1
        yield a, b
        a = b + 1


def base(outward, height):
    """Lowest value outward up to the first strictly higher one; 0 if none."""
    lowest = height
    for value in outward:
        if value > height:
            return lowest
        lowest = min(lowest, value)
    return 0


def top(outward, depth):
    """Highest value outward up to the first strictly lower one or the edge."""
    highest = depth
    for value in outward:
        if value < depth:
            return highest
        highest = max(highest, value)
    return highest


def nodes(s):
    """`(peaks, valleys)` as `(index, prominence)`, by index."""
    peaks, valleys = [], []
    n = len(s)
    for a, b in runs(s):
        level = s[a]
        left = s[a - 1] if a > 0 else None
        right = s[b + 1] if b + 1 < n else None
        at = a + (b - a) // 2
        left_out = s[a - 1 :: -1] if a > 0 else []
        right_out = s[b + 1 :]
        if (left if left is not None else 0) < level and (right if right is not None else 0) < level:
            peaks.append((at, level - max(base(left_out, level), base(right_out, level))))
        if left is not None and right is not None and left > level and right > level:
            valleys.append((at, min(top(left_out, level), top(right_out, level)) - level))
    return peaks, valleys


def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__.rsplit("Usage: ", 1)[1].strip())
    day = datetime.date.fromisoformat(sys.argv[1])
    start, trades = day_trades(day, sys.argv[2], sys.argv[3])
    low, v = histogram(trades)
    for i, volume in enumerate(v):
        print(f"{day}\t{show((low + i) * BIN)}\t{show(volume)}")

    poc = point_of_control(v)
    lo, hi, inside = value_area(v, poc)
    s = smoothed(v)
    biggest = max(s)

    def kept(candidates):
        return [
            f"{show((low + i) * BIN + BIN // 2)}:{1000 * p // biggest}"
            for i, p in candidates
            if biggest > 0 and p * 100 >= biggest * PROMINENCE_PCT
        ] or ["-"]

    peaks, valleys = nodes(s)
    fields = [
        f"start={start}ms",
        f"end={start + DAY_MS}ms",
        "sessions=1",
        f"vol={show(sum(v))}",
        f"low={show(low * BIN)}",
        f"high={show((low + len(v)) * BIN)}",
        f"poc={show((low + poc) * BIN + BIN // 2)}",
        f"poc_vol={show(v[poc])}",
        f"val={show((low + lo) * BIN)}",
        f"vah={show((low + hi + 1) * BIN)}",
        f"va_vol={show(inside)}",
        "hvn=" + ",".join(kept(peaks)),
        "lvn=" + ",".join(kept(valleys)),
    ]
    print(" ".join(fields))


if __name__ == "__main__":
    main()
