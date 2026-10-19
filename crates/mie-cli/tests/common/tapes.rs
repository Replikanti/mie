//! The seeded synthetic tapes of the measurement tools' CI twins: about
//! three weeks of trades with open interest and a trades gap, and about 34
//! hours of a book with trades. Written for `trigger_measure.rs` (ADR-053)
//! and shared with `context_measure.rs` (ADR-054).

use mie_domain::event::{
    Aggressor, BookSnapshot, BookUpdate, FeedGap, GapReason, Level, MarketEvent, OpenInterest,
    Stream, Trade,
};
use mie_domain::num::{Price, Qty, SCALE};
use mie_domain::time::EventTime;

const DAY_MS: i64 = 86_400_000;
const HOUR_MS: i64 = 3_600_000;
const MINUTE_MS: i64 = 60_000;

/// The open-interest grid (ADR-042 D2).
const GRID_MS: i64 = 300_000;

/// A seeded generator, so the tapes need no `rand`.
pub struct Lcg(pub u64);

impl Lcg {
    pub fn below(&mut self, bound: u64) -> i64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        i64::try_from((self.0 >> 11) % bound).unwrap()
    }
}

/// 2026-01-01 00:00 UTC.
pub const TAPE_START: i64 = 1_767_225_600_000;

/// Days on the synthetic tape.
pub const TAPE_DAYS: i64 = 21;

/// The open-interest polls of the synthetic tape, in time order: two around
/// each 5-minute boundary, `r` seconds before it and `10 − r` after it
/// (`r` ∈ 1…9), as a 10 s live poll lands, so each boundary closes at the
/// poll after it. No polls for six hours of day 12: the boundaries there
/// have no point, so rejections around them have no start or no end.
pub fn oi_polls(lcg: &mut Lcg, end: i64) -> Vec<MarketEvent> {
    let mut polls = Vec::new();
    let hole = (TAPE_START + 12 * DAY_MS + HOUR_MS)..(TAPE_START + 12 * DAY_MS + 7 * HOUR_MS);
    let mut oi: i64 = 80_000 * SCALE;
    let mut boundary = TAPE_START + GRID_MS;
    while boundary < end {
        oi += (lcg.below(2_001) - 1_000) * 10_000_000;
        let before = 1_000 * (1 + lcg.below(9));
        if !hole.contains(&boundary) {
            for (time, units) in [
                (boundary - before, oi),
                (
                    boundary + 10_000 - before,
                    oi + lcg.below(1_001) * 1_000_000,
                ),
            ] {
                polls.push(MarketEvent::OpenInterest(OpenInterest {
                    time: EventTime::from_millis(time),
                    open_interest: Qty::from_units(units),
                    resolution_ms: 10_000,
                }));
            }
        }
        boundary += GRID_MS;
    }
    polls
}

/// About three weeks of trades around 62 000 USDT, 10 s to 60 s apart: a
/// walk with a trend of up to ±4 USDT per trade whose sign and size cycle
/// hour by hour on noise of ±12 USDT, so price leaves value and comes back
/// and auction states, swings, sweeps and candidates of every kind occur;
/// heavy-tailed sizes; the open interest of [`oi_polls`]; and a trades gap
/// on day 9.
pub fn synthetic_tape() -> Vec<MarketEvent> {
    let mut lcg = Lcg(0x6d69_6500_0024_0003);
    let end = TAPE_START + TAPE_DAYS * DAY_MS;
    let mut events = oi_polls(&mut lcg, end);
    let mut time = TAPE_START + 123_456;
    let mut cents: i64 = 62_000 * 100;
    let mut trade_id = 0;
    let gap_start = TAPE_START + 9 * DAY_MS + 10 * HOUR_MS;
    let gap_end = gap_start + 45 * MINUTE_MS;
    let mut gapped = false;
    while time < end {
        time += 10_000 + lcg.below(50_000);
        if !gapped && time >= gap_start {
            gapped = true;
            events.push(MarketEvent::FeedGap(FeedGap {
                stream: Stream::Trades,
                start: EventTime::from_millis(gap_start),
                end: EventTime::from_millis(gap_end),
                reason: GapReason::Disconnected,
            }));
            time = gap_end + 1 + lcg.below(30_000);
        }
        let trend = ((time - TAPE_START) / HOUR_MS * 7 % 9) - 4;
        cents += trend * 100 + lcg.below(2_401) - 1_200;
        trade_id += 1;
        events.push(MarketEvent::Trade(Trade {
            time: EventTime::from_millis(time),
            trade_id,
            price: Price::from_units(cents * (SCALE / 100)),
            qty: Qty::from_units(
                (1 + lcg.below(100_000_000)) * if lcg.below(50) == 0 { 40 } else { 1 },
            ),
            aggressor: if lcg.below(2) == 0 {
                Aggressor::Buy
            } else {
                Aggressor::Sell
            },
        }));
    }
    events.sort();
    events
}

/// About 34 hours of a book 0.1 USDT wide around 62 000 USDT, twelve levels
/// a side 3 USDT apart, up to five trades a minute that lift the best ask
/// or hit the best bid, and one book update a minute, at second 50, with
/// each best level's fills and a refill of 0 to 2 times them; a level below
/// 20 BTC is topped up to 40. Absorption needs a day of effort-vs-result
/// warm-up before it is evaluated, so the tape is longer than a day.
pub fn book_tape() -> Vec<MarketEvent> {
    let mut lcg = Lcg(0x6d69_6500_0024_0006);
    let minutes = 34 * 60;
    let bid = 62_000 * SCALE;
    let ask = bid + SCALE / 10;
    let step = 3 * SCALE;
    let level = |price: i64, qty: i64| Level {
        price: Price::from_units(price),
        qty: Qty::from_units(qty),
    };
    let levels = |best: i64, sign: i64| -> Vec<Level> {
        (0..12)
            .map(|k| level(best + sign * k * step, 40 * SCALE))
            .collect()
    };
    let mut last_id = 5_000;
    let mut events = vec![MarketEvent::BookSnapshot(BookSnapshot {
        time: EventTime::from_millis(TAPE_START),
        last_update_id: last_id,
        bids: levels(bid, -1),
        asks: levels(ask, 1),
    })];
    let mut resting = [40 * SCALE; 2];
    let mut trade_id = 0;
    for minute in 0..minutes {
        let open = TAPE_START + minute * MINUTE_MS;
        let mut filled = [0; 2];
        for slot in 0..1 + lcg.below(5) {
            let buy = lcg.below(2) == 0;
            let side = usize::from(buy);
            let mut size = (1 + lcg.below(200)) * SCALE / 100;
            if lcg.below(8) == 0 {
                size *= 8;
            }
            let size = size.min(resting[side] - filled[side] - SCALE);
            if size <= 0 {
                continue;
            }
            filled[side] += size;
            trade_id += 1;
            events.push(MarketEvent::Trade(Trade {
                time: EventTime::from_millis(open + 1_000 + slot * 9_000 + lcg.below(8_000)),
                trade_id,
                price: Price::from_units(if buy { ask } else { bid }),
                qty: Qty::from_units(size),
                aggressor: if buy { Aggressor::Buy } else { Aggressor::Sell },
            }));
        }
        let mut changes = [Vec::new(), Vec::new()];
        for (side, price) in [bid, ask].into_iter().enumerate() {
            let refill = filled[side] * lcg.below(5) / 2;
            let mut next = resting[side] - filled[side] + refill;
            if next < 20 * SCALE {
                next = 40 * SCALE;
            }
            if next != resting[side] {
                changes[side].push(level(price, next));
            }
            resting[side] = next;
        }
        let (first, previous) = if minute == 0 {
            (last_id - 5, 0)
        } else {
            (last_id + 1, last_id)
        };
        last_id += 5;
        let [bids, asks] = changes;
        events.push(MarketEvent::BookUpdate(BookUpdate {
            time: EventTime::from_millis(open + 50_000),
            first_update_id: first,
            last_update_id: last_id,
            prev_update_id: previous,
            bids,
            asks,
        }));
    }
    events.push(MarketEvent::Trade(Trade {
        time: EventTime::from_millis(TAPE_START + minutes * MINUTE_MS + 1_000),
        trade_id: trade_id + 1,
        price: Price::from_units(bid),
        qty: Qty::from_units(SCALE),
        aggressor: Aggressor::Sell,
    }));
    events.sort();
    events
}
