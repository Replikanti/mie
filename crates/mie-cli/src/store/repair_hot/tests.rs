//! The hot repair's pure decisions (#217): X2 with the plan's numbers, the
//! fork guard X2a over built journals, and X4 over in-memory facts. The
//! names follow the plan's edge cases.

use super::*;
use crate::store::archive_checks::tests::{NAME, S1, entry, part, sha, supersede};

/// A supersede record of the fixture's archive file naming `entries`
/// (`(name, bytes)`).
fn record_naming(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut lines: Vec<String> = entries
        .iter()
        .map(|(name, bytes)| {
            let named = SupersededEntry::of((*name).to_owned(), bytes);
            format!("entry {} {}\n", named.name, named.sha256)
        })
        .collect();
    lines.sort();
    lines.dedup();
    let text = String::from_utf8(supersede()).unwrap();
    text.replace(
        "quarantine -\n",
        &format!("{}quarantine -\n", lines.concat()),
    )
    .into_bytes()
}

// ------------------------------------------------------------- helpers

/// One journal line of exactly `len` bytes (its `\n` included): a record
/// of `run` with `fields` (`"k":v,` each) and a `pad` of `*`.
pub(super) fn line(run: &str, fields: &str, len: usize) -> Vec<u8> {
    let head = format!("{{\"type\":\"stats\",\"run_id\":\"{run}\",\"at_ms\":1,{fields}\"pad\":\"");
    let tail = "\"}\n";
    assert!(
        head.len() + tail.len() <= len,
        "{len} is too short for {head}"
    );
    let mut bytes = head.into_bytes();
    bytes.resize(len - tail.len(), b'*');
    bytes.extend_from_slice(tail.as_bytes());
    assert_eq!(bytes.len(), len);
    bytes
}

/// A journal of 100-byte records of `run`, `n` bytes long (a multiple of
/// 100).
pub(super) fn records(run: &str, n: usize) -> Vec<u8> {
    (0..n / 100).flat_map(|_| line(run, "", 100)).collect()
}

/// The fork guard's layout: a common prefix of 600 bytes, then the copies'
/// lines `[600, 700)` and `[700, 1000)` of run `A`, the first with
/// `first_fields`.
fn layout(first_fields: &str) -> Vec<u8> {
    let mut bytes = records("A", 600);
    bytes.extend(line("A", first_fields, 100));
    bytes.extend(line("A", "", 300));
    assert_eq!(bytes.len(), 1000);
    bytes
}

/// The first byte where `a` and `b` differ over their common length.
fn first_diff(a: &[u8], b: &[u8]) -> Option<u64> {
    (0..a.len().min(b.len()))
        .find(|&i| a[i] != b[i])
        .map(|i| i as u64)
}

/// Whether R7 refuses to rebuild `copy` from `hot` with `copies` (all of
/// them, `copy` among them) as the witnesses (ADR-065 D8), in memory.
fn r7_refuses(hot: Option<&[u8]>, copy: &[u8], copies: &[&[u8]]) -> bool {
    let Some(hot) = hot else {
        return !copy.is_empty();
    };
    let d = match first_diff(copy, hot) {
        Some(d) => d,
        None if copy.len() <= hot.len() => return false,
        None => hot.len() as u64,
    };
    replica::first_contested(hot, copies, line_start_in(hot, d)).is_some()
}

/// X1's facts over in-memory journals, as the run reads them: hot (`None`
/// when absent) and the copies in configuration order, with `k` copy
/// targets configured; X2a runs where it decides.
fn facts_of(hot: Option<&[u8]>, copies: &[&[u8]], k: usize) -> JournalFacts {
    let hot_bytes = hot.unwrap_or(&[]);
    let hot_len = hot_bytes.len() as u64;
    let witnesses: Vec<WitnessFacts> = copies
        .iter()
        .enumerate()
        .map(|(i, copy)| WitnessFacts {
            id: format!("cold-{}", (b'a' + i as u8) as char),
            len: copy.len() as u64,
            vs_hot: first_diff(copy, hot_bytes),
            vs_others: copies.iter().map(|other| first_diff(copy, other)).collect(),
            r7_refuses: r7_refuses(hot, copy, copies),
        })
        .collect();
    let mut facts = JournalFacts {
        hot_len,
        witnesses,
        chosen: None,
        bounds: None,
        last_line_start: 0,
        cut: false,
        fork: false,
        copy_targets: k,
        behind: None,
    };
    if !facts.copies_agree() {
        facts.behind = behind_of(hot, copies);
        return facts;
    }
    if copies.is_empty() {
        return facts;
    }
    let longest = copies.iter().map(|c| c.len()).max().unwrap_or(0);
    let c = copies.iter().position(|c| c.len() == longest).unwrap();
    facts.chosen = Some(c);
    let c_bytes = copies[c];
    let common = hot_bytes.len().min(c_bytes.len());
    let d = (0..common).find(|&i| hot_bytes[i] != c_bytes[i]);
    let e = (0..common).rev().find(|&i| hot_bytes[i] != c_bytes[i]);
    facts.bounds = d.zip(e).map(|(d, e)| (d as u64, e as u64));
    facts.last_line_start = last_line_start(c_bytes);
    facts.cut = c_bytes.last().is_some_and(|&byte| byte != b'\n');
    if let Some((d, e)) = facts.bounds
        && c_bytes.len() == hot_bytes.len()
        && e < facts.second_len()
    {
        facts.fork = fork_of(hot_bytes, c_bytes, d, e);
    }
    facts
}

/// [`BehindFacts`] over in-memory journals, as the run reads them where the
/// copies differ: `None` without a copy longer than hot or where hot holds
/// no strict majority at some byte below L_h. The tail is the longest copy
/// with no difference with hot below L_h (the first on a tie); X2a runs
/// over each differing copy's region.
fn behind_of(hot: Option<&[u8]>, copies: &[&[u8]]) -> Option<BehindFacts> {
    let hot = hot?;
    if copies.iter().all(|c| c.len() <= hot.len()) {
        return None;
    }
    let below: Vec<&[u8]> = copies
        .iter()
        .map(|c| &c[..c.len().min(hot.len())])
        .collect();
    if replica::first_contested(hot, &below, 0).is_some() {
        return None;
    }
    let disagree = replica::first_disagreement(copies, hot.len() as u64);
    let tail = copies
        .iter()
        .copied()
        .filter(|c| first_diff(c, hot).is_none())
        .rev()
        .max_by_key(|c| c.len())
        .and_then(|c| c.get(hot.len()..))
        .unwrap_or(&[]);
    let fork = copies.iter().enumerate().find_map(|(w, c)| {
        let d = first_diff(c, hot)?;
        let common = c.len().min(hot.len());
        let e = (0..common).rev().find(|&i| hot[i] != c[i])? as u64;
        fork_of(hot, c, d, e).then_some((w, d))
    });
    Some(behind_facts(hot.len() as u64, tail, disagree, fork))
}

/// X2a over in-memory journals, hot's lines read one at a time from s.
fn fork_of(hot: &[u8], c: &[u8], d: u64, e: u64) -> bool {
    let s = line_start_in(c, d) as usize;
    let mut rest = &hot[s..];
    fork_shaped(&ForkFacts { c, d, e }, &mut || {
        if rest.is_empty() {
            return Ok(None);
        }
        let end = rest
            .iter()
            .position(|b| *b == b'\n')
            .map_or(rest.len(), |i| i + 1);
        let (line, tail) = rest.split_at(end);
        rest = tail;
        Ok(Some(line.to_vec()))
    })
}

fn decide(hot: Option<&[u8]>, copies: &[&[u8]]) -> JournalDecision {
    decide_journal(&facts_of(hot, copies, copies.len().max(2)))
}

/// `bytes` with each `(offset, byte)` written.
fn with(bytes: &[u8], writes: &[(usize, u8)]) -> Vec<u8> {
    let mut bytes = bytes.to_vec();
    for &(at, byte) in writes {
        bytes[at] = byte;
    }
    bytes
}

/// `bytes` with one bit of each offset flipped.
fn flipped(bytes: &[u8], at: &[usize]) -> Vec<u8> {
    let mut bytes = bytes.to_vec();
    for &i in at {
        bytes[i] ^= 0x01;
    }
    bytes
}

/// Facts of hot against copies that are prefixes of one another, by
/// numbers alone (X2's bounds).
fn numbers(hot_len: u64, lens: &[u64], bounds: Option<(u64, u64)>, lls: u64) -> JournalFacts {
    let longest = lens.iter().copied().max().unwrap_or(0);
    JournalFacts {
        hot_len,
        witnesses: lens
            .iter()
            .enumerate()
            .map(|(i, &len)| WitnessFacts {
                id: format!("cold-{i}"),
                len,
                vs_hot: bounds.map(|(d, _)| d).filter(|d| *d < len.min(hot_len)),
                vs_others: vec![None; lens.len()],
                r7_refuses: false,
            })
            .collect(),
        chosen: lens.iter().position(|&len| len == longest),
        bounds,
        last_line_start: lls,
        cut: false,
        fork: false,
        copy_targets: lens.len().max(2),
        behind: None,
    }
}

// ----------------------------------------------------------------- X2

/// No witness under the configured name: *agree*, nothing to write.
#[test]
fn x2_no_copy_under_the_configured_name() {
    assert_eq!(
        decide(Some(&records("A", 300)), &[]),
        JournalDecision::Agree
    );
    assert_eq!(decide(None, &[]), JournalDecision::Agree);
}

/// Hot absent with copies of 300 bytes, and hot a prefix of copies of
/// 1 000 and 500 bytes: *behind*, one witness enough.
#[test]
fn x2_behind_extends_with_one_witness() {
    let copy = records("A", 300);
    assert_eq!(decide(None, &[&copy, &copy]), JournalDecision::Behind);
    assert_eq!(decide(None, &[&copy]), JournalDecision::Behind);
    let long = records("A", 1000);
    let hot = &long[..400];
    assert_eq!(
        decide(Some(hot), &[&long, &long[..500]]),
        JournalDecision::Behind
    );
    // Agree: hot holds every copy's bytes and more.
    assert_eq!(
        decide(Some(&long), &[&long[..500], &long[..400]]),
        JournalDecision::Agree
    );
}

/// The bounds of *damaged* (k = 2): two witnesses at hot's full length at
/// every differing byte; e = 499 against copies of 1 000 and 500 is
/// *damaged*, e = 500 *one-witness*; d = 0 with e = 999 is *damaged*
/// (byte 0's flip leaves the first line no record, so X2a finds no fork);
/// and with equal lengths the order of the copies does not matter.
#[test]
fn x2_damaged_needs_two_witnesses_at_hot_length() {
    let c = records("A", 1000);
    assert_eq!(
        decide(Some(&flipped(&c, &[10])), &[&c, &c]),
        JournalDecision::Damaged
    );
    assert_eq!(
        decide(Some(&flipped(&c, &[10, 499])), &[&c, &c[..500]]),
        JournalDecision::Damaged
    );
    assert_eq!(
        decide(Some(&flipped(&c, &[10, 500])), &[&c, &c[..500]]),
        JournalDecision::OneWitness
    );
    assert_eq!(
        decide(Some(&flipped(&c, &[10, 500])), &[&c[..500], &c]),
        JournalDecision::OneWitness
    );
    let ends = flipped(&c, &[0, 999]);
    let facts = facts_of(Some(&ends), &[&c, &c], 2);
    assert_eq!(facts.bounds, Some((0, 999)));
    assert!(!facts.fork);
    assert_eq!(decide_journal(&facts), JournalDecision::Damaged);
    // By numbers: hot 1 000, copies 1 000 and 500.
    let n = |e| decide_journal(&numbers(1000, &[1000, 500], Some((10, e)), 900));
    assert_eq!(n(499), JournalDecision::Damaged);
    assert_eq!(n(500), JournalDecision::OneWitness);
    assert_eq!(
        decide_journal(&numbers(1000, &[1000, 1000], Some((10, 10)), 900)),
        JournalDecision::Damaged
    );
}

/// Hot 1 200 bytes against copies of 1 000 with lls(C) = 900: a difference
/// ending at 899 is *hot-ahead*, one ending at 900 *diverged*.
#[test]
fn x2_hot_ahead_names_replicate() {
    let n = |e| decide_journal(&numbers(1200, &[1000, 1000], Some((10, e)), 900));
    assert_eq!(n(899), JournalDecision::HotAhead);
    assert_eq!(n(900), JournalDecision::Diverged);
    // Built: hot extends the copies by 200 bytes and is flipped at 10.
    let hot = records("A", 1200);
    let copy = &hot[..1000];
    assert_eq!(last_line_start(copy), 900);
    assert_eq!(
        decide(Some(&flipped(&hot, &[10])), &[copy, copy]),
        JournalDecision::HotAhead
    );
    assert_eq!(
        decide(Some(&flipped(&hot, &[10, 950])), &[copy, copy]),
        JournalDecision::Diverged
    );
}

/// *diverged*: hot longer with the difference in the copies' last line,
/// and copies longer than hot that hot contradicts (hot 800, copies 1 000,
/// d = 700).
#[test]
fn x2_diverged_last_line_and_longer_copies() {
    let c = records("A", 1000);
    let hot = flipped(&c[..800], &[700]);
    assert_eq!(decide(Some(&hot), &[&c, &c]), JournalDecision::Diverged);
    assert_eq!(
        decide_journal(&numbers(800, &[1000, 1000], Some((700, 700)), 900)),
        JournalDecision::Diverged
    );
    assert_eq!(
        decide_journal(&numbers(1200, &[1000, 1000], Some((910, 950)), 900)),
        JournalDecision::Diverged
    );
}

/// Hot unreadable: a difference at every byte (d = 0, e = L_h − 1), the
/// guard does not apply; *damaged* with two copies of hot's length,
/// *one-witness* with copies of 1 000 and 600.
#[test]
fn x2_unreadable_hot_needs_two_full_witnesses() {
    let mut facts = numbers(1000, &[1000, 1000], Some((0, 999)), 900);
    facts.fork = false;
    assert_eq!(decide_journal(&facts), JournalDecision::Damaged);
    let facts = numbers(1000, &[1000, 600], Some((0, 999)), 900);
    assert_eq!(decide_journal(&facts), JournalDecision::OneWitness);
    assert!(JournalDecision::OneWitness.keeps_hot());
}

/// The copies differ (k = 2 unless stated): *copy-damaged* when hot agrees
/// with a copy that every differing copy contradicts inside hot's length
/// (in either configuration order), otherwise *two-fault*; with k = 3 the
/// two-fault stop names the consistent copy targets.
#[test]
fn x2_copies_differ_splits_copy_damaged_and_two_fault() {
    let a = records("A", 1000);
    let b = flipped(&a, &[300]);
    assert_eq!(decide(Some(&a), &[&a, &b]), JournalDecision::CopyDamaged);
    // The tie: hot equals the second copy, the first is damaged.
    assert_eq!(decide(Some(&a), &[&b, &a]), JournalDecision::CopyDamaged);
    // Hot also differs from cold-a at 700.
    let hot = flipped(&a, &[700]);
    assert_eq!(
        decide(Some(&hot), &[&a, &b]),
        JournalDecision::TwoFault { group: None }
    );
    // Hot 800, a prefix of cold-a; cold-b differs from cold-a at 850,
    // inside the first line past hot's end: nothing past L_h is agreed as
    // a line, so one copy stands against the other there.
    let late = flipped(&a, &[850]);
    assert_eq!(
        decide(Some(&a[..800]), &[&a, &late]),
        JournalDecision::TwoFault { group: None }
    );
    // k = 3: cold-c equals cold-a, cold-b differs at 300, hot at 700.
    assert_eq!(
        decide_journal(&facts_of(Some(&hot), &[&a, &b, &a], 3)),
        JournalDecision::TwoFault {
            group: Some(vec![0, 2])
        }
    );
}

/// *copy-damaged* is exactly R7's rule, D8 (review of #226, findings 1
/// and 2): hot intact with cold-a damaged at x = 110 (the second line)
/// and cold-b at 10 has two of three journals for hot at every byte, so
/// R7 heals both, in either order: *copy-damaged*. Hot damaged at x with
/// cold-b damaged at 10 and cold-a only the first line long: at x only hot
/// and cold-b hold a byte (one against one), so R7 refuses cold-b, and the
/// state is *two-fault*.
#[test]
fn x2_copy_damaged_is_r7s_majority_rule() {
    let a = records("A", 1000);
    let x = 110;
    let (cold_a, cold_b) = (flipped(&a, &[x]), flipped(&a, &[10]));
    assert_eq!(
        decide(Some(&a), &[&cold_a, &cold_b]),
        JournalDecision::CopyDamaged
    );
    assert_eq!(
        decide(Some(&a), &[&cold_b, &cold_a]),
        JournalDecision::CopyDamaged
    );
    let hot = flipped(&a, &[x]);
    let facts = facts_of(Some(&hot), &[&a[..100], &cold_b], 2);
    assert!(!facts.witnesses[0].r7_refuses, "a byte prefix of hot");
    assert!(facts.witnesses[1].r7_refuses);
    assert_eq!(
        decide_journal(&facts),
        JournalDecision::TwoFault { group: None }
    );
    // A copy longer than hot holds bytes hot lacks: R7 refuses it.
    let facts = facts_of(Some(&a[..800]), &[&a, &flipped(&a, &[900])], 2);
    assert!(facts.witnesses.iter().all(|w| w.r7_refuses));
}

/// Hot *behind* with the copies differing (ADR-065 D2, review of #226
/// round 2, finding 2; round 3, finding 1). Hot lost its last 200 bytes;
/// cold-a has a bit flipped at 10, cold-b is the true journal. Hot holds
/// two of three at 10, and past L_h both copies agree:
/// *behind-copy-damaged* to cold-b's end, never a stop that offers
/// `--keep-hot-journal` (which would drop the 200 bytes both copies hold).
/// The bytes past L_h come only from copies with no difference below it:
/// with cold-b cut at 900, the bytes past it are flipped cold-a's alone,
/// so it is *two-fault*. Copies that disagree past L_h (at 950, or at 850
/// inside the first line) are *two-fault*. Hot contradicted below L_h (its
/// own flip at 300, where both copies agree against it) or a tie there
/// (only hot and cold-a hold byte 300) is no *behind*: *two-fault*.
#[test]
fn x2_behind_with_a_copy_fault_takes_the_agreed_tail() {
    let good = records("A", 1000);
    let hot = &good[..800];
    let cold_a = flipped(&good, &[10]);
    let decided = |hot: &[u8], copies: &[&[u8]]| {
        let facts = facts_of(Some(hot), copies, 2);
        (decide_journal(&facts), facts.behind)
    };
    let two_fault = JournalDecision::TwoFault { group: None };
    assert_eq!(
        decided(hot, &[&cold_a, &good]),
        (
            JournalDecision::BehindCopyDamaged,
            Some(BehindFacts {
                reach: 1000,
                cut: false,
                disagree: None,
                fork: None
            })
        )
    );
    assert_eq!(
        decided(hot, &[&good, &cold_a]).0,
        JournalDecision::BehindCopyDamaged
    );
    // The flipped copy may be shorter than the consistent one.
    assert_eq!(
        decided(hot, &[&cold_a[..900], &good]).0,
        JournalDecision::BehindCopyDamaged
    );
    assert_eq!(
        decided(hot, &[&cold_a, &good[..900]]),
        (
            two_fault.clone(),
            Some(BehindFacts {
                reach: 900,
                cut: false,
                disagree: None,
                fork: None
            })
        )
    );
    // A copy that ends inside a line: the extension gets its `\n`.
    let mut cut = good.clone();
    cut.extend_from_slice(b"{\"type\":");
    assert_eq!(
        decided(hot, &[&cold_a, &cut]),
        (
            JournalDecision::BehindCopyDamaged,
            Some(BehindFacts {
                reach: 1008,
                cut: true,
                disagree: None,
                fork: None
            })
        )
    );
    assert_eq!(
        decided(hot, &[&cold_a, &flipped(&good, &[950])]),
        (
            two_fault.clone(),
            Some(BehindFacts {
                reach: 1000,
                cut: false,
                disagree: Some(950),
                fork: None
            })
        )
    );
    assert_eq!(
        decided(hot, &[&cold_a, &flipped(&good, &[850])]).0,
        two_fault
    );
    let rotten = flipped(&good[..800], &[300]);
    assert_eq!(
        decided(&rotten, &[&cold_a, &good]),
        (two_fault.clone(), None)
    );
    assert_eq!(
        decided(&rotten, &[&flipped(&good[..400], &[10]), &good[..200]]).1,
        None,
        "no copy longer than hot"
    );
    let tie = [&flipped(&good, &[10])[..], &good[..250]];
    assert_eq!(decided(&rotten, &tie), (two_fault, None));
    assert!(!JournalDecision::BehindCopyDamaged.keeps_hot());
}

/// A fork is never *behind-copy-damaged* (ADR-065 D2, D3; review of #226
/// round 3, finding 1). P is 600 bytes of run A. (1) The reviewer's shape:
/// hot lost O (300 bytes of run A) at a power loss after cold-b copied it,
/// then wrote N (100 bytes of run B), which a pass copied to cold-a: hot
/// and cold-a outvote cold-b below L_h, but only cold-b, which differs,
/// holds bytes past L_h: *two-fault*, and its stop names the refusal. (2)
/// N is 300 bytes and a later line M (100 bytes) reached only cold-a: the
/// bytes past L_h are cold-a's, consistent with hot, and cold-b (P + 100
/// bytes of run A) ends below L_h; X2a calls cold-b's region a fork, so
/// it is *two-fault* as well. Without the fork verdict it would be
/// *behind-copy-damaged*, which appends M and leaves O to the repair.
#[test]
fn x2_a_fork_is_never_behind_copy_damaged() {
    let p = records("A", 600);
    let joined = |parts: &[&[u8]]| parts.concat();
    let o = line("A", "", 300);
    let n = line("B", "", 100);
    let hot = joined(&[&p, &n]);
    let cold_b = joined(&[&p, &o]);
    let facts = facts_of(Some(&hot), &[&hot, &cold_b], 2);
    let behind = facts.behind.expect("behind");
    assert_eq!(behind.reach, 700);
    assert!(behind.fork.is_some_and(|(w, _)| w == 1), "{behind:?}");
    assert_eq!(
        decide_journal(&facts),
        JournalDecision::TwoFault { group: None }
    );
    let clause = behind_clause(&facts);
    assert!(
        clause.contains("the copy on cold-b differs from hot's journal at byte")
            && clause.contains("holds bytes past byte 700"),
        "{clause}"
    );

    let n = line("B", "", 300);
    let m = line("B", "", 100);
    let hot = joined(&[&p, &n]);
    let cold_a = joined(&[&p, &n, &m]);
    let cold_b = joined(&[&p, &line("A", "", 100)]);
    let mut facts = facts_of(Some(&hot), &[&cold_a, &cold_b], 2);
    let behind = facts.behind.expect("behind");
    assert_eq!((behind.reach, behind.disagree), (1000, None));
    let (w, d) = behind.fork.expect("a fork");
    assert_eq!((w, d), (1, first_diff(&cold_b, &hot).unwrap()));
    assert_eq!(
        decide_journal(&facts),
        JournalDecision::TwoFault { group: None }
    );
    assert!(
        behind_clause(&facts).contains("in the shape of a fork"),
        "{}",
        behind_clause(&facts)
    );
    facts.behind = Some(BehindFacts {
        fork: None,
        ..behind
    });
    assert_eq!(decide_journal(&facts), JournalDecision::BehindCopyDamaged);
}

/// A finished extension whose `\n` did not land leaves hot equal to C,
/// which ends inside a line (review of #226, finding 5): *behind*, so the
/// re-run writes the `\n`. Equal and whole is *agree*.
#[test]
fn x2_hot_equal_to_a_cut_copy_is_behind() {
    let mut cut = records("A", 300);
    cut.extend_from_slice(b"{\"type\":\"stats\"");
    assert_eq!(decide(Some(&cut), &[&cut, &cut]), JournalDecision::Behind);
    assert_eq!(decide(Some(&cut), &[&cut[..300]]), JournalDecision::Agree);
    let whole = records("A", 300);
    assert_eq!(decide(Some(&whole), &[&whole]), JournalDecision::Agree);
}

/// Which states `--keep-hot-journal` may act on (X3).
#[test]
fn x3_the_states_the_flag_acts_on() {
    for decision in [
        JournalDecision::Agree,
        JournalDecision::Behind,
        JournalDecision::BehindCopyDamaged,
        JournalDecision::Damaged,
        JournalDecision::HotAhead,
    ] {
        assert!(!decision.keeps_hot(), "{decision:?}");
    }
    for decision in [
        JournalDecision::Diverged,
        JournalDecision::OneWitness,
        JournalDecision::CopyDamaged,
        JournalDecision::TwoFault { group: None },
    ] {
        assert!(decision.keeps_hot(), "{decision:?}");
    }
}

// ---------------------------------------------------------------- X2a

/// X2a over the layout: hot's own bytes after `prefix` of the copies' bytes
/// up to `from`, the copies' bytes after `to`.
fn fork_case(c: &[u8], middle: &[u8], from: usize, to: usize) -> JournalDecision {
    let mut hot = c[..from].to_vec();
    hot.extend_from_slice(middle);
    hot.extend_from_slice(&c[to..]);
    assert_eq!(hot.len(), c.len());
    decide(Some(&hot), &[c, c])
}

/// Equal-length forks are *diverged*: other `\n` positions, a glued line
/// after a torn one with another run's record, a torn last record, an
/// unknown type and a `\r\n` line.
#[test]
fn x2a_an_equal_length_fork_is_diverged() {
    let c = layout("");
    // Hot's lines after 600 end at 749 and 999, records of run B.
    let mut middle = line("B", "", 150);
    middle.extend(line("B", "", 250));
    assert_eq!(fork_case(&c, &middle, 600, 1000), JournalDecision::Diverged);

    // Glued: the copies' bytes 600–649, then a 50-byte record of run B
    // ending at 699, then the copies' bytes to the end. One `\n` at 699 in
    // both, so only the glued branch finds it.
    let glued = line("B", "", 50);
    let mut hot = c[..650].to_vec();
    hot.extend(&glued);
    hot.extend_from_slice(&c[700..]);
    let facts = facts_of(Some(&hot), &[&c, &c], 2);
    let (d, e) = facts.bounds.unwrap();
    assert!(d >= 650 && e < 699, "{d} {e}");
    assert_eq!(decide_journal(&facts), JournalDecision::Diverged);

    // A record of run B ending at 849, then a torn record to the end.
    let mut middle = line("B", "", 250);
    let mut torn = b"{\"at_ms\":1,\"pad\":\"".to_vec();
    torn.resize(150, b'*');
    middle.extend(&torn);
    assert_eq!(fork_case(&c, &middle, 600, 1000), JournalDecision::Diverged);

    // A type the table does not know, and a line ending in `\r\n`.
    let unknown = {
        let mut bytes = b"{\"type\":\"zzzzz\",\"run_id\":\"B\",\"at_ms\":1,\"pad\":\"".to_vec();
        bytes.resize(146, b'*');
        bytes.extend_from_slice(b"\"}\r\n");
        bytes
    };
    assert_eq!(
        record_run_id(&unknown[..unknown.len() - 1]).as_deref(),
        Some("B")
    );
    let mut middle = unknown;
    middle.extend(line("B", "", 250));
    assert_eq!(fork_case(&c, &middle, 600, 1000), JournalDecision::Diverged);
}

/// A torn line inside the region, not only at its start (review of #226,
/// finding 4): run B's record over 600–749, run B killed 50 bytes into
/// its next line, then run C's record glued onto it, to 999. Every `\n`
/// position differs from the copies', and the later glued line is
/// recognised as one: *diverged*. Its torn part must be a torn record:
/// two lines merged by a `\n` turned into 0x0B in a later line stay
/// *damaged*; turned into a blank, the first record parses with the blank
/// as trailing whitespace, so the merged line reads as glued and the
/// guard stops (two faults read as a fork, ADR-065 D3).
#[test]
fn x2a_a_torn_line_glued_inside_the_region_is_a_fork() {
    let c = layout("");
    let mut middle = line("B", "", 150);
    let mut torn = line("B", "", 100);
    torn.truncate(50);
    middle.extend(torn);
    middle.extend(line("C", "", 200));
    assert_eq!(fork_case(&c, &middle, 600, 1000), JournalDecision::Diverged);

    let c = records("A", 1000);
    assert_eq!((c[637], c[799]), (b'1', b'\n'));
    let merged = with(&c, &[(637, b'0'), (799, 0x0B)]);
    assert_eq!(decide(Some(&merged), &[&c, &c]), JournalDecision::Damaged);
    let blank = with(&c, &[(637, b'0'), (799, b' ')]);
    assert_eq!(decide(Some(&blank), &[&c, &c]), JournalDecision::Diverged);
}

/// A substitution that keeps every `\n` position and valid JSON (a digit
/// flip inside `"records":12`) is *damaged*; so is a nested object's
/// closing `}` turned into a blank, which puts the root's members inside
/// it: the suffix from the inner `{` is a record of C's own run.
#[test]
fn x2a_a_valid_json_bit_flip_is_damaged() {
    let c = layout("\"records\":12,");
    assert_eq!(c[650], b'2');
    let hot = with(&c, &[(650, b'2' ^ 0x01)]);
    let facts = facts_of(Some(&hot), &[&c, &c], 2);
    assert_eq!(facts.bounds, Some((650, 650)));
    assert!(record_run_id(&hot[600..699]).is_some());
    assert_eq!(decide_journal(&facts), JournalDecision::Damaged);

    // The synthetic line, padded to 100 bytes, at 600.
    let mut nested =
        b"{\"a\":{\"x\":1},\"at_ms\":5,\"run_id\":\"A\",\"type\":\"t\",\"pad\":\"".to_vec();
    nested.resize(97, b'*');
    nested.extend_from_slice(b"\"}\n");
    let mut c = records("A", 600);
    c.extend(&nested);
    c.extend(line("A", "", 300));
    assert_eq!(c[611], b'}');
    let hot = with(&c, &[(611, b' ')]);
    assert!(record_run_id(&hot[600..699]).is_none());
    assert_eq!(record_run_id(&hot[605..699]).as_deref(), Some("A"));
    assert_eq!(decide(Some(&hot), &[&c, &c]), JournalDecision::Damaged);
}

/// Zero-filled regions are *damaged*: 64 zero bytes from 680 across the
/// `\n` at 699, zero bytes over exactly 600–999, zero bytes from 950 to
/// the end, and U+200B in place of a line's first 3 bytes.
#[test]
fn x2a_a_zero_filled_region_is_damaged() {
    let c = layout("");
    let zeros = |from: usize, to: usize| {
        let mut hot = c.clone();
        hot[from..to].fill(0);
        hot
    };
    for hot in [zeros(680, 744), zeros(600, 1000), zeros(950, 1000)] {
        assert_eq!(decide(Some(&hot), &[&c, &c]), JournalDecision::Damaged);
    }
    let hot = with(&c, &[(700, 0xE2), (701, 0x80), (702, 0x8B)]);
    assert_eq!(decide(Some(&hot), &[&c, &c]), JournalDecision::Damaged);
}

/// A flipped `\n` is *damaged*: the `\n` at 699 turned into 0x0B (two
/// lines merge), and a `*` at 650 turned into `\n` (one line splits).
#[test]
fn x2a_newline_flips_are_damaged() {
    let c = layout("");
    assert_eq!(c[699], b'\n');
    let merged = with(&c, &[(699, 0x0B)]);
    assert_eq!(decide(Some(&merged), &[&c, &c]), JournalDecision::Damaged);
    assert_eq!(c[650], b'*');
    let split = with(&c, &[(650, b'*' ^ 0x20)]);
    assert_eq!(split[650], b'\n');
    assert_eq!(decide(Some(&split), &[&c, &c]), JournalDecision::Damaged);
}

/// A read error inside the guard counts as a fork: it errs toward
/// stopping.
#[test]
fn x2a_a_read_error_is_a_fork() {
    let c = layout("");
    assert!(fork_shaped(
        &ForkFacts {
            c: &c,
            d: 650,
            e: 650
        },
        &mut || Err("Input/output error (os error 5)".to_owned())
    ));
}

/// lls(C): the start of the last complete line, as the pass compares it.
#[test]
fn x2_last_line_start() {
    assert_eq!(last_line_start(b""), 0);
    assert_eq!(last_line_start(b"{}\n"), 0);
    assert_eq!(last_line_start(b"{}\n{}\n"), 3);
    assert_eq!(last_line_start(b"{}\n{}\n{"), 3);
    assert_eq!(last_line_start(b"{\"a\""), 0);
}

// ----------------------------------------------------------------- X4

fn copy(root: &str, state: FileState, bytes: &[u8]) -> CopyFacts {
    CopyFacts {
        root: root.to_owned(),
        path: PathBuf::from(format!("{root}/{NAME}")),
        state,
        bytes: Some(bytes.to_vec()),
    }
}

fn name_facts(hot: Option<CopyFacts>, targets: Vec<CopyFacts>) -> BTreeMap<String, NameFacts> {
    BTreeMap::from([(
        NAME.to_owned(),
        NameFacts {
            kind: NameKind::Current,
            hot,
            targets,
            closure: "CLOSURE".to_owned(),
        },
    )])
}

fn decision(names: &BTreeMap<String, NameFacts>) -> NameDecision {
    plan_hot_import_ledger(names).into_values().next().unwrap()
}

/// Hot's good copy is kept, a differing target copy named for R8; no file
/// anywhere plans nothing.
#[test]
fn x4_good_hot_copy_is_kept() {
    assert!(plan_hot_import_ledger(&BTreeMap::new()).is_empty());
    let names = name_facts(
        Some(copy(HOT_ID, FileState::Good, b"E")),
        vec![
            copy("cold-a", FileState::Good, b"E"),
            copy("cold-b", FileState::DoesNotParse("x".into()), b"rotten\n"),
        ],
    );
    assert_eq!(
        decision(&names),
        NameDecision::Keep {
            differing: vec!["cold-b".to_owned()]
        }
    );
}

/// Hot lacks the name, one good copy on cold-b only: written, created.
#[test]
fn x4_absent_on_hot_is_written() {
    let names = name_facts(None, vec![copy("cold-b", FileState::Good, b"E")]);
    assert_eq!(
        decision(&names),
        NameDecision::Write {
            from: 0,
            replace: false
        }
    );
}

/// Hot rotten, cold-a good, cold-b rotten: cold-a's bytes replace hot's.
#[test]
fn x4_bad_hot_copy_one_good_variant_is_written() {
    let rotten = || FileState::DoesNotParse("not UTF-8".into());
    let names = name_facts(
        Some(copy(HOT_ID, rotten(), b"rotten\n")),
        vec![
            copy("cold-a", FileState::Good, b"E"),
            copy("cold-b", rotten(), b"rotten\n"),
        ],
    );
    assert_eq!(
        decision(&names),
        NameDecision::Write {
            from: 0,
            replace: true
        }
    );
    // An unreadable hot copy is replaced the same way.
    let mut unreadable = copy(HOT_ID, FileState::Unreadable("denied".into()), b"");
    unreadable.bytes = None;
    let names = name_facts(
        Some(unreadable),
        vec![
            copy("cold-a", FileState::Good, b"E"),
            copy("cold-b", FileState::Good, b"E"),
        ],
    );
    assert_eq!(
        decision(&names),
        NameDecision::Write {
            from: 0,
            replace: true
        }
    );
}

/// Two good variants, or none: a stop naming every copy with its state;
/// with none, the closure too.
#[test]
fn x4_two_good_variants_or_none_stop() {
    let rotten = || FileState::DoesNotParse("not UTF-8".into());
    let names = name_facts(
        Some(copy(HOT_ID, rotten(), b"rotten\n")),
        vec![
            copy("cold-a", FileState::Good, b"E rows 4"),
            copy("cold-b", FileState::Good, b"E rows 5"),
        ],
    );
    let NameDecision::Stop(why) = decision(&names) else {
        panic!("not a stop");
    };
    for needle in [
        "hot (",
        "does not parse",
        "cold-a (",
        "cold-b (",
        "two good byte variants",
    ] {
        assert!(why.contains(needle), "{needle}: {why}");
    }
    assert!(!why.contains("CLOSURE"), "{why}");
    let names = name_facts(
        Some(copy(HOT_ID, rotten(), b"rotten\n")),
        vec![
            copy("cold-a", FileState::OtherSha(vec!["p".into()]), b"E"),
            copy("cold-b", FileState::Unreadable("denied".into()), b""),
        ],
    );
    let NameDecision::Stop(why) = decision(&names) else {
        panic!("not a stop");
    };
    for needle in [
        "lists p with another SHA-256",
        "unreadable (denied)",
        "CLOSURE",
    ] {
        assert!(why.contains(needle), "{needle}: {why}");
    }
}

/// A superseded entry is good when a supersede record of its archive file
/// names its SHA-256 for its name: one named variant passes, others do not.
#[test]
fn x4_superseded_entry_test_uses_the_records_sha() {
    let e0 = entry(&[(&part("aggTrades", "2026-10-01", 0), &sha("a"))]);
    let other = entry(&[(&part("aggTrades", "2026-10-01", 0), &sha("b"))]);
    let one = SupersedeRecord::parse(std::str::from_utf8(&record_naming(&[(NAME, &e0)])).unwrap())
        .unwrap();
    let none = |_: &[(String, String)]| Vec::new();
    let state = |bytes: &[u8], records: &[&SupersedeRecord]| {
        file_state(NAME, NameKind::Superseded, bytes, records, &none)
    };
    assert_eq!(state(&e0, &[&one]), FileState::Good);
    assert_eq!(state(&other, &[&one]), FileState::Unnamed);
    assert_eq!(state(&e0, &[]), FileState::Unnamed);
    assert!(matches!(
        state(b"rotten\n", &[&one]),
        FileState::DoesNotParse(_)
    ));
    // Both variants named by one record: both good, so the plan stops.
    let both = SupersedeRecord::parse(
        std::str::from_utf8(&record_naming(&[(NAME, &e0), (NAME, &other)])).unwrap(),
    )
    .unwrap();
    assert_eq!(both.entries.len(), 2);
    for bytes in [&e0, &other] {
        assert_eq!(state(bytes, &[&both]), FileState::Good);
    }
    let mut names = name_facts(
        Some(copy(
            HOT_ID,
            FileState::DoesNotParse("x".into()),
            b"rotten\n",
        )),
        vec![
            copy("cold-a", FileState::Good, &e0),
            copy("cold-b", FileState::Good, &other),
        ],
    );
    names.values_mut().next().unwrap().kind = NameKind::Superseded;
    assert!(matches!(decision(&names), NameDecision::Stop(_)));
    // One named variant among the targets is written.
    let mut names = name_facts(
        Some(copy(
            HOT_ID,
            FileState::DoesNotParse("x".into()),
            b"rotten\n",
        )),
        vec![
            copy("cold-a", FileState::Unnamed, &other),
            copy("cold-b", FileState::Good, &e0),
        ],
    );
    names.values_mut().next().unwrap().kind = NameKind::Superseded;
    assert_eq!(
        decision(&names),
        NameDecision::Write {
            from: 1,
            replace: true
        }
    );
}

/// A current entry needs R8's test; a supersede record must parse and name
/// its own file (G3).
#[test]
fn x4_supersede_record_binding() {
    let own = supersede();
    assert_eq!(
        file_state(S1, NameKind::Record, &own, &[], &|_| Vec::new()),
        FileState::Good
    );
    let foreign = String::from_utf8(own)
        .unwrap()
        .replace("2026-10-01", "2026-10-02")
        .into_bytes();
    assert!(matches!(
        file_state(S1, NameKind::Record, &foreign, &[], &|_| Vec::new()),
        FileState::DoesNotParse(_)
    ));
    let e0 = entry(&[(&part("aggTrades", "2026-10-01", 0), &sha("a"))]);
    assert_eq!(
        file_state(NAME, NameKind::Current, &e0, &[], &|_| Vec::new()),
        FileState::Good
    );
    assert_eq!(
        file_state(NAME, NameKind::Current, &e0, &[], &|files| {
            files.iter().map(|(path, _)| path.clone()).collect()
        }),
        FileState::OtherSha(vec![part("aggTrades", "2026-10-01", 0)])
    );
}

/// The generation bounds of G1: 4294967295 is a valid name, 4294967296 a
/// malformed `.import` name the readers fail closed on.
#[test]
fn x4_generation_bounds() {
    let file = "BTCUSDT-aggTrades-2026-10-01.zip";
    assert!(LedgerName::parse(&format!("{file}.4294967295.import")).is_ok());
    assert!(!fails_closed(&format!(
        "aggTrades/{file}.4294967295.import"
    )));
    assert!(fails_closed(&format!("aggTrades/{file}.4294967296.import")));
}

/// Which malformed names on hot are moved aside: the `.import` names G1
/// rejects; the names both readers skip stay.
#[test]
fn x4_malformed_import_names_moved_aside_others_left() {
    const NAME: &str = "BTCUSDT-aggTrades-2026-10-01.zip";
    for moved in [
        format!("{NAME}.01.import"),
        format!("{NAME}.0.import"),
        format!("{NAME}.\u{FF11}.import"),
        format!("{NAME}.1\u{200B}.import"),
        format!("{NAME}.1 .import"),
    ] {
        assert!(fails_closed(&format!("aggTrades/{moved}")), "{moved:?}");
    }
    for left in [
        format!(".{NAME}.import"),
        format!(".{NAME}.import.tmp"),
        "notes.txt".to_owned(),
        format!("{NAME}.import "),
        "PENDING".to_owned(),
    ] {
        assert!(!fails_closed(&format!("aggTrades/{left}")), "{left:?}");
    }
    assert!(!fails_closed("bad segment/"));
}

/// The closure by hand of a name with no good copy anywhere (the plan's
/// residual 3).
#[test]
fn x4_no_good_copy_names_its_closure() {
    let dir = Path::new("/ledger");
    let name = NAME.to_owned();
    let archive = "data/futures/um/daily/aggTrades/BTCUSDT/X.zip";
    // The current entry of generation 0, and one above a re-admit's record.
    for record in [None, Some(Some(false))] {
        let text = closure(&name, NameKind::Current, dir, Some(archive), record, false);
        for needle in [
            "/ledger.damaged-<at_ms>/aggTrades/",
            "<target>/.mie/damaged/<at_ms>/.mie/import-ledger/",
            "mie store rebuild --replace-hot-ledger --record-unlisted",
            &format!("mie store resolve --file {archive}"),
        ] {
            assert!(text.contains(needle), "{needle}: {text}");
        }
    }
    let text = closure(&name, NameKind::Current, dir, None, None, false);
    assert!(
        text.contains("which `mie store resolve` proposes"),
        "{text}"
    );
    // A superseded entry: the moves alone.
    let text = closure(&name, NameKind::Superseded, dir, Some(archive), None, false);
    assert!(text.contains("move "), "{text}");
    assert!(!text.contains("mie store rebuild"), "{text}");
    assert!(text.contains("Nothing needs to be recreated"), "{text}");
    // A supersede record, an entry after a replace's record, and one whose
    // record has no good copy: no move.
    for (kind, record) in [
        (NameKind::Record, None),
        (NameKind::Current, Some(Some(true))),
        (NameKind::Current, Some(None)),
    ] {
        let text = closure(&name, kind, dir, Some(archive), record, false);
        assert!(text.contains("No mie command recreates it"), "{text}");
        assert!(text.contains("backup kept outside mie"), "{text}");
        assert!(!text.contains("move "), "{text}");
    }
    // PENDING names the archive file: the importer's resume, not resolve.
    let text = closure(&name, NameKind::Current, dir, Some(archive), None, true);
    assert!(text.contains("run the importer"), "{text}");
    assert!(!text.contains("mie store resolve --file"), "{text}");
}

/// `--scope` is required and has two values.
#[test]
fn x0_scope_parse() {
    assert_eq!(Scope::parse(Some("journal")), Ok(Scope::Journal));
    assert_eq!(Scope::parse(Some("import-ledger")), Ok(Scope::ImportLedger));
    assert!(Scope::parse(Some("both")).unwrap_err().contains("neither"));
    assert!(Scope::parse(None).unwrap_err().contains("required"));
}
