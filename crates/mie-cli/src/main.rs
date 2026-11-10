//! The `mie` command line.

use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::catalog::parse_day;
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_binance::transport::{
    Clock, DownloadTimeouts, NetTimeouts, SystemClock, TungsteniteConnector, UreqDownload, UreqHttp,
};
use mie_app::kline_check::KlineVerdict;
use mie_cli::alerts;
use mie_cli::archive::{self, ArchiveTransports, ImportRequest};
use mie_cli::conditional::{self, BuildRequest};
use mie_cli::config::{ArchiveConfig, IngestConfig};
use mie_cli::equivalence::{self, EquivalenceRequest};
use mie_cli::experiment::{self, ExperimentPipeline, ExperimentRequest};
use mie_cli::ingest::{self, Transports};
use mie_cli::replay::{self, ReplayRequest, ReplaySource};
use mie_cli::report;
use mie_cli::store;
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const USAGE: &str = "\
mie — BTC Market Intelligence Engine

USAGE:
    mie ingest --config <path>
        Capture the Binance USDⓈ-M live feed into the raw store and feed the
        core. Runs until SIGINT or SIGTERM, then seals every open file and
        exits 0. Exits 1 when the capture fails — a failed write (for
        example a full disk) stops it loudly — or the store cannot be
        opened. On a tiered store (paths.cold_roots) a replication pass
        runs after every seal pass on its own thread, and partitions before
        the hot window leave hot once every copy target holds a verified
        copy of every file in them and a ledger copy that records it; an
        append to a removed partition first copies it back from a copy
        target. The journal gets replication, removal, recall and
        copy_ledger_truncated lines. See crates/mie-cli/ingest.example.toml.

    mie capture-report --config <path> --from <ms> --to <ms>
                       [--max-late-fraction <0..1>]
        Verify the capture runs started in [from, to) (UTC epoch ms):
        trade-id continuity, disconnects as gaps, clean shutdown, journal
        totals, and per-stream LateEvent fraction (default limit 0: any
        late event fails). Prints PASS or FAIL; exits 0 on PASS, 1 on FAIL.

    mie archive-import --config <path> --from <YYYY-MM-DD> --to <YYYY-MM-DD>
                       [--streams <a,b,...>] [--dry-run]
        Import the Binance public data archive (USDⓈ-M BTCUSDT) for the
        inclusive UTC day range into the raw store under the source
        binance-archive; monthly files cover every month the range
        overlaps. Every file is checksum-verified; re-runs skip imported
        files, an interrupted file is resumed. --streams replaces the
        configured streams (aggTrades, klines, klines_1m, ..., fundingRate,
        metrics, bookDepth, trades). --dry-run fetches checksums only and
        reports published / missing / imported / changed files. SIGINT or
        SIGTERM stops at the next file boundary. Exits 1 when a file failed
        or changed upstream. On a tiered store an import into a partition
        removed from hot first copies it back from a copy target and prints
        a recall line. See crates/mie-cli/archive.example.toml.

    mie archive-verify --config <path> --from <YYYY-MM-DD> --to <YYYY-MM-DD>
        Re-read every imported file of the range (hash-verified), normalize
        every record, check ledger <-> store consistency, list trade-id
        breaks and holes over 60 s, print the dataset version per stream.
        A configured stream without an imported file for a period of the
        range fails. Prints PASS or FAIL; exits 0 on PASS, 1 on FAIL.

    mie archive-kline-check --config <path> --from <YYYY-MM-DD> --to <YYYY-MM-DD>
                            [--trade-source aggTrades|trades]
                            [--trade-correction fold|none]
        Build bars from archive trades (default aggTrades) through the
        domain and compare every complete bar with the archive klines
        (ADR-031). aggTrades are folded with the trades archive by default
        (ADR-050; --trade-correction none delivers them as published and
        reproduces results from before ADR-050); the flag is refused with
        --trade-source trades. Prints the fold summary and a coverage line
        (window bars compared, bars
        skipped as incomplete, klines without a bar) before the verdict
        (ADR-045): PASS (exit 0) when every bar of the window was compared
        and matched and at most one bar per timeframe (the partial starts)
        was skipped as incomplete; FAIL (exit 1) on a mismatch or when
        nothing was compared; INCONCLUSIVE (exit 3) when no bar disagrees
        but missing days or feed gaps left bars of the window uncompared.
        trades ids are sparse by design, so only missing days are gaps
        under --trade-source trades.

    mie replay --config <path> --from <ms|YYYY-MM-DD> --to <ms|YYYY-MM-DD>
               [--source live|archive|history] [--streams <a,b,...>]
               [--trade-correction fold|none]
        Replay [from, to) of the raw store through the core (a date is a
        UTC day: --from its start, --to its end, inclusive). live (default)
        reads an ingest config and recomputes every capture run that
        touches the window from its journal; archive reads an archive
        config and merges the backfill (--streams replaces the configured
        streams, which default to all but trades and bookDepth). An archive
        replay folds the trades the aggTrades archive omits back in from
        imported trades files (ADR-050, --trade-correction fold, the
        default); none delivers aggTrades as published and reproduces data
        versions and hashes from before ADR-050. history reads an ingest
        config with a [history] section and replays the history chain
        (ADR-052): the archive backfill of its archive config from the
        origin up to the cutover (default streams, folded), then the
        journaled runs that started at or after the cutover, always from
        the origin, restricted to the window; --streams and
        --trade-correction do not apply, and a run the cutover falls inside
        fails the replay. An uncrossable hole restarts a fresh engine
        (ADR-052 D4): each restart and the effective origin are printed.
        Prints the dataset version, runs or streams (one line per segment
        for history), the fold summary with one line per corrected day and
        per corrected minute, delivered events and gaps, trailing gaps,
        missing days, domain rejections, the final state and the
        event-stream hash; the same window prints the same bytes. Exits 0
        on PASS; 1 when the replay failed, the domain rejected an event or
        the window holds no event.

    mie equivalence --config <path> --from <ms|YYYY-MM-DD> --to <ms|YYYY-MM-DD>
        Live/replay equivalence (ADR-041): for every capture run of the
        ingest config that started in [from, to) (UTC wall clock of its
        run_start; a date is a UTC day, as for mie replay), recompute the
        run alone from the raw store into a fresh engine and compare its
        state checkpoints (ordinal, event-stream hash, Market State hash)
        with the ones the run journaled. A warm-started run (ADR-052 D9)
        is compared on one pass over the history chain of the config's
        [history]: the chain through its predecessor must reproduce the
        prefix event-stream hash and the handoff state hash its warm_start
        journaled (else DIVERGED (warm prefix), with the journaled and the
        current prefix dataset version printed), then the run's own events
        continue in the same engine and their checkpoints are compared.
        Prints one line per run — EQUIVALENT, DIVERGED with the first
        divergent checkpoint, DIVERGED (warm prefix), EVENTS EQUIVALENT /
        STATE NOT COMPARABLE (another feature set: only the delivered
        events are compared), or NOT COMPARABLE (crashed, no checkpoints,
        no event delivered, replay error, another history chain) — then
        PASS or FAIL; the same request prints the same bytes. Exits 0 only
        when at least one run was compared and every run is EQUIVALENT.

    mie experiment validate <spec>
        Validate an experiment spec (spec text v1, ADR-040). Prints the
        experiment id and the canonical spec, then the second stage: the
        spec's rules resolved under the backtest's rule registry v1
        (ADR-060 D2), either `backtest: resolves under rule registry v1`
        or one `backtest: <error>` line per resolution error; exits 0
        either way, since a spec that parses is valid. An invalid spec
        prints every error, one per line, and exits 1. See
        crates/mie-cli/experiment.example.spec.

    mie experiment run <spec> --config <path> --results <dir>
                       [--source live|archive|history] [--streams <a,b,...>]
                       [--trade-correction fold|none]
                       [--pipeline backtest|slippage-calibration]
        Run the spec's sample period through the core (source options as
        mie replay) and record the result in the append-only result store
        under <dir>. live and archive record research.replay_summary@1.
        history takes an ingest config with a [history] section and
        records research.replay_summary@2 (ADR-052 D10): the history chain
        from the origin up to the sample's start drives the engine first,
        the sample is then replayed as @1 replays it, and the result adds
        the prefix's provenance and the state hashes at the sample's start
        and end. Its key ends in the engine's feature-set version and
        state-hash encoding, so another build records next to the old
        result. --streams and --trade-correction apply to archive only.
        --pipeline backtest (with --source history only) records
        research.backtest@1 instead (ADR-060 D13): the spec's rules are
        resolved, the hypothesis is simulated on the same chain drive, and
        the result holds @2's provenance lines, the counts, one trade line
        per taken decision and the metrics; its key ends in the engine like
        @2's. A simulation only: no order is placed (ADR-010).
        --pipeline slippage-calibration (with --source live only) records
        research.slippage_calibration@1 (ADR-061 D7): every accepted trade
        of the sample gives a buy and a sell at its print, priced on the
        order book of that moment by the spread model and the depth walk
        at 17 sizes from 0.001 to 120 BTC; the result holds @1's
        provenance lines, the trade count and per model the priced fills,
        the unpriced ones by cause, the sum and the mean of the per-fill
        slippage. The spec's features must contain book.l2@1; its rule
        lines are not read. Its key has no engine, like @1's.
        Prints `recorded <key>` for a new result or `reproduced <key>` when
        the stored result was reproduced exactly; exits 0. A data-version
        mismatch, an empty sample, a sample before the history origin, a
        provider failure or a result that differs from the stored one
        prints `FAIL: <reason>` and exits 1; so do, for the backtest and
        before anything is opened, a spec that does not resolve (every
        resolution error), funding.recorded@1 on a sample the chain
        delivers no settlements for (its archive does not replay
        fundingRate, or the sample ends after the cutover), and a source
        other than history; for the calibration, a spec without book.l2@1
        and a source other than live. Stored results are never
        overwritten.

    mie conditional build --config <path> --from <ms|YYYY-MM-DD> --to <ms|YYYY-MM-DD>
                          --out <dir> [--source live|archive]
                          [--streams <a,b,...>] [--trade-correction fold|none]
                          [--replay-from <ms|YYYY-MM-DD>] [--replay-to <ms|YYYY-MM-DD>]
        Build the conditional outcome dataset (ADR-057, format 2 of
        ADR-058): replay the source (as mie replay; history is refused),
        take the context records of [from, to) and label them under the
        current labeling, and write contexts.parquet, outcomes.parquet,
        passages.parquet and effort.parquet plus dataset.manifest into
        <dir>, which must not exist. The replay
        window defaults to [from, to); --replay-from and --replay-to widen
        it, so earlier events warm the engine up and later ones give the
        last records their lookahead (labels the data ends before are
        censored end_of_data). The manifest names the source, both windows,
        the data version, engine, sampling and labeling, the events and
        rejections, each file's rows and SHA-256, the content hash of all
        that, and the code version (outside the hash): the same inputs give
        the same files and content hash. Prints the manifest, the record and
        label counts and PASS (exit 0), or FAIL: <reason> (exit 1), in which
        case <dir> holds no manifest and is no dataset.

    mie conditional verify <dir>
        Verify an outcome dataset: the manifest's content hash, each file's
        SHA-256, rows, format and schema. Prints the manifest and PASS
        (exit 0), or FAIL: <reason> (exit 1). A format-1 dataset fails:
        rebuild it.

    mie conditional query <dir> <view> [--n-min <n>]
        Verify an outcome dataset as verify does, then print one of its
        DuckDB views (ADR-057 D7) as TSV, ordered by the view's key columns
        (exit 0): contexts, outcomes, passages, effort (the tables),
        labeled (one row per record and horizon with the excursions in
        barrier offsets, the time to the race, level_within and
        extreme_vs_opposite) or
        outcome_summary (distributions per horizon and per every
        combination of trigger, regime and location, plus trigger
        families), which requires --n-min, the minimum clear-sample count
        it flags (meets_min_sample = clear >= n). NULL prints as NULL,
        times as epoch milliseconds, prices exactly. A failure prints
        FAIL: <reason> (exit 1). No query downloads anything.

    mie conditional views
        Print the SQL of the views, for any DuckDB client of the pinned
        version: SET VARIABLE mie_dataset = '<dir>'; then run it.

    mie alerts replay --config <path> --from <ms|YYYY-MM-DD> --to <ms|YYYY-MM-DD>
                      --out <file> [--source live|archive|history]
                      [--streams <a,b,...>] [--trade-correction fold|none]
                      [--replay-from <ms|YYYY-MM-DD>]
        Alert dry-run (ADR-059): replay the source (as mie replay) through
        the alert decision path live ingestion uses, and write the alert
        log of [from, to) to <file>, which must not exist: JSON Lines with
        a header (source, windows, data version, engine, alert policy and
        its fingerprint), one message line per message (event time, event
        key, the alerts' canonical lines, the rendered text) and a summary
        (events, rejections, messages, alerts per kind, messages per UTC
        day p50/p95/max). --replay-from starts the replay earlier, so the
        engine and the monitor warm up without a message. The same inputs
        give the same bytes. Prints the provenance and the counts, then
        PASS (exit 0), or FAIL: <reason> (exit 1) when the replay failed
        or the window holds no event; nothing is written then. Nothing is
        sent anywhere.

    mie alerts report --config <path> --from <ms|YYYY-MM-DD> --to <ms|YYYY-MM-DD>
                      [--source live|archive|history] [--streams <a,b,...>]
                      [--trade-correction fold|none]
        Replay [from, to) and print the market report as of its end, in
        the text a report message carries: last price, volatility regime,
        UTC-day VWAP, prior-day auction state, the monitored prior-day
        levels with their zones and the active monitors. PASS (exit 0), or
        FAIL: <reason> (exit 1) when the window holds no event.

    mie store init --config <path>
        Initialize a tiered raw store (ADR-062 D7): write hot's root marker
        (.mie/root, id hot) and an empty copy ledger (.mie/copy-ledger)
        under the ingest config's raw_root, once. Needs paths.cold_roots.
        Refused when raw_root/.mie exists, and when a configured cold root
        already carries a marker or a copy-ledger copy: that hot was
        initialized before and is lost, which the hot restore repairs,
        never a new init. Prints what it wrote and what it found on each
        cold root and exits 0, or prints FAIL: <reason> and exits 1. See
        crates/mie-cli/ingest.example.toml.

    mie store init-root --config <path> --root <path> --id <id>
        Write the root marker of a freshly provisioned cold root
        (ADR-062 D4), once. <path> must be one of the ingest config's
        paths.cold_roots, spelled as there, and an existing, empty
        directory; it is never created. The id is 1 to 64 characters from
        A-Z, a-z, 0-9, '_' and '-', not hot, and no other configured root
        may carry it. Run mie store init first. The root must be a mount
        point: refused when it is on the same device as raw_root or as its
        parent directory, since an unmounted mount point is an empty
        directory on its parent's filesystem. Prints what it wrote and exits 0, or
        prints FAIL: <reason> and exits 1.

    mie store replicate --config <path> [--archive-config <path>]
        Run one replication pass of a tiered raw store (ADR-062 D11): copy
        to every copy target of paths.cold_roots what it lacks, in this
        order: every sealed file (verified by its SHA-256, the manifest
        published last), then the import-ledger entries, then the tail of
        the copy ledger, then the tail of the journal, each verified after
        the copy. A copy target without its root marker is skipped and
        named, and nothing is written into it. Refused, before anything is
        written, when paths.cold_roots is empty, when hot has no marker,
        when hot's copy ledger is missing or unreadable, when another pass
        runs, when two roots carry one id, and when hot and the copy
        targets that carry a marker do not all report distinct devices.
        An archive part is copied only once the import entry that lists it
        exists on hot, and its entry only after its parts; this needs the
        archive configuration (--archive-config, else [history]
        archive_config), which must name the same raw_root, symbol and
        cold_roots. Without one, archive parts and entries are not
        copied. A copy target's marker is re-read before every write to
        it, and a target's journal copy stops before the first line
        that names a live file missing on that target. Nothing is ever
        removed: mie ingest removes from hot. Prints what it copied and every copy-ledger truncation,
        then PASS and exits 0 when every file has a copy on every copy
        target, or one FAIL: <reason> line per file, entry, target or copy
        left short and exits 1.

    mie store adopt --config <path> [--archive-config <path>] --from <path>
                    [--journal <path>] [--import-ledger <dir>]
        Adopt an existing single-root store (ADR-062 D10) into the
        initialized tiered store of the ingest config, without passing its
        data through hot: every sealed file of --from is copied straight to
        every copy target (verified, manifest published last) and recorded
        in the copy ledger; every partition before the first date of the
        hot window (today - 7, UTC, derived from the seed lookback, D8) is
        recorded as removed from hot, and the later ones are copied to hot
        too. --journal copies the store's journal to the configured one,
        which must be absent or a byte prefix of it; --import-ledger copies
        the store's import entries to hot's import ledger, named by the
        archive configuration (--archive-config, else [history]
        archive_config). Then one replication pass runs. --from is only
        read, never written. A path the store already holds with identical
        bytes is skipped, with other bytes it is refused (Integrity), and
        a new path is adopted, so a re-run resumes an interrupted adoption
        and a later run can adopt another store. Refused, before anything is
        written, when paths.cold_roots is empty, hot has no marker or no
        readable copy ledger, a copy target has no marker, roots share a
        device or an id, --from is not a closed single-root store outside
        hot and every cold root (a writer holds it, it has .mie/, seal
        debris or a pending import), an archive part has no import entry
        listing it, or another pass or a writer of hot runs. Prints one line
        per adopted
        partition, a total, the pass's lines, then PASS and exits 0, or
        one FAIL: <reason> line per problem and exits 1; it stops at the
        first failure.

    mie store verify --config <path> [--archive-config <path>]
                     --period-hours <P>
        Verify every copy of a tiered raw store (ADR-062 D11): on hot,
        every sealed file against the copy ledger's manifest; on every copy
        target that carries its root marker, every recorded copy (present,
        manifest identical to the ledger's, size and SHA-256), the copies
        each file lacks, the import-ledger entries (byte for byte, with the
        archive configuration: --archive-config, else [history]
        archive_config), the copy-ledger copy (its hash chain, naming the
        first failing record, and a byte prefix of hot's) and the journal
        copy (a byte prefix of hot's, naming the first differing byte); on
        a read-only cold root, the copies it holds. Every finding on a copy
        target is appended to hot's copy ledger, then one verify record per
        target; nothing on any root is written and nothing is repaired. A
        copy target without its marker is named as not verified, with the
        count of its copies whose last good verification is older than P
        hours. P is required and has no default; it must be a whole number
        of hours of at least 1. Runs beside mie ingest; refused when hot has
        no marker or no readable copy ledger, paths.cold_roots is empty,
        another verify or repair runs, two roots carry one id or share a
        device. Prints one summary line per root, then PASS and exits 0, or
        one FAIL: <finding> line per finding naming the file, the root and
        the finding, and exits 1.

    mie store repair --config <path> [--archive-config <path>] [--record-lost]
        Repair the copies of a tiered raw store (ADR-062 D11) that mie store
        verify recorded as damaged or missing, and fill the copy targets
        that lack files which left hot (a target added or replaced). It
        never runs on its own: run it after reading what verify found,
        since several bad copies at once point at a shared cause. Each copy
        is checked again first; a good one is only recorded again. A bad
        copy is moved aside into <root>/.mie/damaged/<time>/, never deleted,
        and replaced from the first good copy (hot, then the cold roots in
        order), verified by its SHA-256 and recorded. Copy-ledger and
        journal copies are rebuilt from hot where that is safe (a damaged
        unterminated tail is cut off); a copy ledger whose complete records
        verify but differ from hot's, a journal copy no other copy agrees
        with, and an import entry hot cannot vouch for are refused and
        named. A file with no good copy left is reported lost and nothing
        is written, but only when every root that records a copy of it
        still carries its marker and every root was read without error;
        otherwise it is not repairable in this run, naming those roots.
        --record-lost then appends its lost record, after which every
        replay that covers it fails, so pass it only after a run without it
        reported the file. Replication pauses while the repair runs; it
        ends with one replication pass. Refused like mie store verify, and
        when a replication pass runs. Prints what it repaired and the
        pass's lines, then PASS and exits 0, or one FAIL: <reason> line per
        copy left unrepaired, file lost or not repairable, or refused, and
        exits 1.

    mie store rebuild --config <path> [--archive-config <path>]
                      [--record-lost] [--record-unlisted] [--replace-hot-ledger]
        Rebuild hot's copy ledger of a tiered raw store (ADR-062 D7) after
        it was lost or damaged, while mie ingest keeps capturing. It starts
        from the longest copy-ledger copy on a copy target whose hash chain
        verifies (or empty, and says so), lists every manifest on every
        root, checks each cold copy's data against it, and cross-checks
        hot's journal and the import ledger (the archive configuration:
        --archive-config, else [history] archive_config): it records every
        verified copy, every partition with cold copies and no directory
        on hot as removed, and the archive holds it re-derives. A file a
        record names that no root holds is reported lost; --record-lost
        records it, after which every replay that covers it fails, so pass
        it only after a run without it reported the file. An archive part
        no import entry lists is held only with --record-unlisted, after
        which the importer refuses until the hold is resolved; a run
        without it names the parts, the import ledger's directory, how many
        entries it holds and how many entries the copy targets hold that
        hot lacks (an empty or unmounted directory reads as no entries),
        and writes nothing. A missing import-ledger directory, or a link to
        a missing one, stops the rebuild while archive data exists. A
        readable hot ledger is replaced only with --replace-hot-ledger. The
        old ledger, and every ledger copy that is no prefix of the new one,
        is moved into .mie/damaged/<time>/, never deleted. Every root, read-only
        roots included, must carry its marker. The rebuild reads every copy
        on every root once, the cost of one verification; replication, hot
        removal and imports pause meanwhile, and hot removal resumes only
        after the next mie store verify. Prints the start, the records
        derived, every lost file and hold, then the ledger written (`wrote
        …`) or `nothing written`; only that line says whether anything was
        written. Then PASS and exits 0, or one FAIL: <reason> line per
        problem and exits 1.

    mie --help
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match dispatch(&args) {
        Ok(code) => code,
        Err(message) => {
            eprintln!("mie: {message}");
            eprintln!("Run `mie --help` for usage.");
            ExitCode::from(2)
        }
    }
}

fn dispatch(args: &[String]) -> Result<ExitCode, String> {
    match args.first().map(String::as_str) {
        None | Some("--help" | "-h" | "help") => {
            print!("{USAGE}");
            Ok(ExitCode::SUCCESS)
        }
        Some("ingest") => {
            let options = Options::parse(&args[1..], &["--config"], &[])?;
            let config =
                IngestConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
            run_ingest(&config)
        }
        Some("capture-report") => {
            let options = Options::parse(
                &args[1..],
                &["--config", "--from", "--to", "--max-late-fraction"],
                &[],
            )?;
            let config =
                IngestConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
            let (from, to) = (options.millis("--from")?, options.millis("--to")?);
            if from >= to {
                return Err(format!("--from {from} must be below --to {to}"));
            }
            let max_late = options.fraction("--max-late-fraction", 0.0)?;
            let pass = report::run(&config, from, to, max_late, &mut std::io::stdout().lock())?;
            Ok(if pass {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
        Some("archive-import") => {
            let options = Options::parse(
                &args[1..],
                &["--config", "--from", "--to", "--streams"],
                &["--dry-run"],
            )?;
            let config =
                ArchiveConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
            let (from_day, to_day) = options.days()?;
            let streams = match options.optional("--streams") {
                Some(list) => {
                    let names: Vec<String> = list.split(',').map(str::to_owned).collect();
                    Some(
                        ArchiveConfig::expand(&names, &config.archive.kline_intervals)
                            .map_err(|e| e.to_string())?,
                    )
                }
                None => None,
            };
            let request = ImportRequest {
                from_day,
                to_day,
                streams,
                dry_run: options.flag("--dry-run"),
            };
            run_archive_import(&config, &request)
        }
        Some("archive-verify") => {
            let options = Options::parse(&args[1..], &["--config", "--from", "--to"], &[])?;
            let config =
                ArchiveConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
            let (from_day, to_day) = options.days()?;
            let pass = archive::verify(&config, from_day, to_day, &mut std::io::stdout().lock())?;
            Ok(exit(pass))
        }
        Some("archive-kline-check") => {
            let options = Options::parse(
                &args[1..],
                &[
                    "--config",
                    "--from",
                    "--to",
                    "--trade-source",
                    "--trade-correction",
                ],
                &[],
            )?;
            let trade_source = match options.optional("--trade-source").unwrap_or("aggTrades") {
                "aggTrades" => ArchiveStream::AggTrades,
                "trades" => ArchiveStream::Trades,
                other => {
                    return Err(format!(
                        "--trade-source {other:?} is neither aggTrades nor trades"
                    ));
                }
            };
            let correction = match trade_source {
                ArchiveStream::Trades => {
                    if options.optional("--trade-correction").is_some() {
                        return Err("--trade-correction applies to --trade-source aggTrades \
                                    only: trades has nothing to correct"
                            .to_owned());
                    }
                    TradeCorrection::None
                }
                _ => options.correction()?,
            };
            let config =
                ArchiveConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
            let (from_day, to_day) = options.days()?;
            let verdict = archive::kline_check(
                &config,
                from_day,
                to_day,
                trade_source,
                correction,
                &mut std::io::stdout().lock(),
            )?;
            Ok(ExitCode::from(verdict_code(verdict)))
        }
        Some("replay") => {
            let options = Options::parse(
                &args[1..],
                &[
                    "--config",
                    "--from",
                    "--to",
                    "--source",
                    "--streams",
                    "--trade-correction",
                ],
                &[],
            )?;
            let config_path = options.path("--config")?;
            let from = replay::parse_bound("--from", options.value("--from")?, false)?;
            let to = replay::parse_bound("--to", options.value("--to")?, true)?;
            if from >= to {
                return Err(format!("--from {from} must be below --to {to}"));
            }
            let source = options.replay_source(&config_path)?;
            let request = ReplayRequest {
                source,
                window: ReplayWindow {
                    start: EventTime::from_millis(from),
                    end: EventTime::from_millis(to),
                },
            };
            let outcome = replay::run(&request, &mut std::io::stdout().lock())?;
            Ok(exit(outcome.pass))
        }
        Some("equivalence") => {
            let options = Options::parse(&args[1..], &["--config", "--from", "--to"], &[])?;
            let config =
                IngestConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
            let from_ms = replay::parse_bound("--from", options.value("--from")?, false)?;
            let to_ms = replay::parse_bound("--to", options.value("--to")?, true)?;
            if from_ms >= to_ms {
                return Err(format!("--from {from_ms} must be below --to {to_ms}"));
            }
            let request = EquivalenceRequest {
                config,
                from_ms,
                to_ms,
            };
            let outcome = equivalence::run(&request, &mut std::io::stdout().lock())?;
            Ok(exit(outcome.pass))
        }
        Some("experiment") => match (args.get(1).map(String::as_str), args.get(2)) {
            (Some("validate"), Some(spec)) => {
                Options::parse(&args[3..], &[], &[])?;
                let valid =
                    experiment::validate(&PathBuf::from(spec), &mut std::io::stdout().lock())?;
                Ok(exit(valid))
            }
            (Some("run"), Some(spec)) => {
                let options = Options::parse(
                    &args[3..],
                    &[
                        "--config",
                        "--source",
                        "--streams",
                        "--results",
                        "--trade-correction",
                        "--pipeline",
                    ],
                    &[],
                )?;
                let pipeline = match options.optional("--pipeline") {
                    None => ExperimentPipeline::Summary,
                    Some("backtest") => ExperimentPipeline::Backtest,
                    Some("slippage-calibration") => ExperimentPipeline::SlippageCalibration,
                    Some(other) => {
                        return Err(format!(
                            "--pipeline {other:?} is neither backtest nor slippage-calibration \
                             (omit it for the replay summary)"
                        ));
                    }
                };
                let request = ExperimentRequest {
                    spec: PathBuf::from(spec),
                    source: options.replay_source(&options.path("--config")?)?,
                    results: options.path("--results")?,
                    pipeline,
                };
                let pass = experiment::run(&request, &mut std::io::stdout().lock())?;
                Ok(exit(pass))
            }
            _ => Err(
                "expected `mie experiment validate <spec>` or `mie experiment run <spec> …`"
                    .to_owned(),
            ),
        },
        Some("conditional") => match (args.get(1).map(String::as_str), args.get(2)) {
            (Some("build"), _) => {
                let options = Options::parse(
                    &args[2..],
                    &[
                        "--config",
                        "--from",
                        "--to",
                        "--out",
                        "--source",
                        "--streams",
                        "--trade-correction",
                        "--replay-from",
                        "--replay-to",
                    ],
                    &[],
                )?;
                let bound = |flag: &str, is_end: bool, default: Option<i64>| match (
                    options.optional(flag),
                    default,
                ) {
                    (Some(value), _) => replay::parse_bound(flag, value, is_end),
                    (None, Some(default)) => Ok(default),
                    (None, None) => Err(format!("{flag} is required")),
                };
                let from = bound("--from", false, None)?;
                let to = bound("--to", true, None)?;
                if from >= to {
                    return Err(format!("--from {from} must be below --to {to}"));
                }
                let replay_from = bound("--replay-from", false, Some(from))?;
                let replay_to = bound("--replay-to", true, Some(to))?;
                if replay_from > from || replay_to < to {
                    return Err(format!(
                        "the replay window [{replay_from}, {replay_to}) must contain \
                         [{from}, {to})"
                    ));
                }
                let request = BuildRequest {
                    source: options.replay_source(&options.path("--config")?)?,
                    records: ReplayWindow {
                        start: EventTime::from_millis(from),
                        end: EventTime::from_millis(to),
                    },
                    replay: ReplayWindow {
                        start: EventTime::from_millis(replay_from),
                        end: EventTime::from_millis(replay_to),
                    },
                    out: options.path("--out")?,
                };
                let pass = conditional::build(&request, &mut std::io::stdout().lock())?;
                Ok(exit(pass))
            }
            (Some("verify"), Some(dir)) => {
                Options::parse(&args[3..], &[], &[])?;
                let pass = conditional::verify(&PathBuf::from(dir), &mut std::io::stdout().lock())?;
                Ok(exit(pass))
            }
            (Some("query"), Some(dir)) => {
                let view = args
                    .get(3)
                    .ok_or("expected `mie conditional query <dir> <view> [--n-min <n>]`")?;
                let options = Options::parse(&args[4..], &["--n-min"], &[])?;
                let n_min = options
                    .optional("--n-min")
                    .map(|value| {
                        value
                            .parse::<u64>()
                            .map_err(|_| format!("--n-min {value:?} is not a count"))
                    })
                    .transpose()?;
                let mut out = std::io::BufWriter::new(std::io::stdout().lock());
                let pass = conditional::query(&PathBuf::from(dir), view, n_min, &mut out)?;
                Ok(exit(pass))
            }
            (Some("views"), _) => {
                Options::parse(&args[2..], &[], &[])?;
                conditional::views(&mut std::io::stdout().lock())?;
                Ok(ExitCode::SUCCESS)
            }
            _ => Err(
                "expected `mie conditional build …`, `mie conditional verify <dir>`, \
                 `mie conditional query <dir> <view>` or `mie conditional views`"
                    .to_owned(),
            ),
        },
        Some("alerts") => match args.get(1).map(String::as_str) {
            Some("replay") => {
                let options = Options::parse(
                    &args[2..],
                    &[
                        "--config",
                        "--from",
                        "--to",
                        "--out",
                        "--source",
                        "--streams",
                        "--trade-correction",
                        "--replay-from",
                    ],
                    &[],
                )?;
                let from = replay::parse_bound("--from", options.value("--from")?, false)?;
                let to = replay::parse_bound("--to", options.value("--to")?, true)?;
                if from >= to {
                    return Err(format!("--from {from} must be below --to {to}"));
                }
                let replay_from = match options.optional("--replay-from") {
                    Some(value) => replay::parse_bound("--replay-from", value, false)?,
                    None => from,
                };
                if replay_from > from {
                    return Err(format!(
                        "--replay-from {replay_from} must not be after --from {from}"
                    ));
                }
                let request = alerts::ReplayRequest {
                    source: options.replay_source(&options.path("--config")?)?,
                    replay: ReplayWindow {
                        start: EventTime::from_millis(replay_from),
                        end: EventTime::from_millis(to),
                    },
                    emit_from: EventTime::from_millis(from),
                    out: options.path("--out")?,
                };
                let pass = alerts::replay(&request, &mut std::io::stdout().lock())?;
                Ok(exit(pass))
            }
            Some("report") => {
                let options = Options::parse(
                    &args[2..],
                    &[
                        "--config",
                        "--from",
                        "--to",
                        "--source",
                        "--streams",
                        "--trade-correction",
                    ],
                    &[],
                )?;
                let from = replay::parse_bound("--from", options.value("--from")?, false)?;
                let to = replay::parse_bound("--to", options.value("--to")?, true)?;
                if from >= to {
                    return Err(format!("--from {from} must be below --to {to}"));
                }
                let request = alerts::ReportRequest {
                    source: options.replay_source(&options.path("--config")?)?,
                    window: ReplayWindow {
                        start: EventTime::from_millis(from),
                        end: EventTime::from_millis(to),
                    },
                };
                let pass = alerts::report(&request, &mut std::io::stdout().lock())?;
                Ok(exit(pass))
            }
            _ => Err("expected `mie alerts replay …` or `mie alerts report …`".to_owned()),
        },
        Some("store") => match args.get(1).map(String::as_str) {
            Some("init") => {
                let options = Options::parse(&args[2..], &["--config"], &[])?;
                let config =
                    IngestConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
                let pass = store::init(&config, &mut std::io::stdout().lock())?;
                Ok(exit(pass))
            }
            Some("init-root") => {
                let options = Options::parse(&args[2..], &["--config", "--root", "--id"], &[])?;
                let config =
                    IngestConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
                let root = options.path("--root")?;
                let id = options.value("--id")?;
                let probe = store::device_probe()?;
                let pass = store::init_root(
                    &config,
                    &root,
                    id,
                    probe.as_ref(),
                    &mut std::io::stdout().lock(),
                )?;
                Ok(exit(pass))
            }
            Some("replicate") => {
                let options = Options::parse(&args[2..], &["--config", "--archive-config"], &[])?;
                let config =
                    IngestConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
                let archive = options.optional("--archive-config").map(PathBuf::from);
                let probe = store::device_probe()?;
                let clock = SystemClock::new();
                let now_ms = || clock.now_utc_ns().div_euclid(1_000_000);
                let pass = store::replicate(
                    &config,
                    archive.as_deref(),
                    probe.as_ref(),
                    &now_ms,
                    &mut std::io::stdout().lock(),
                )?;
                Ok(exit(pass))
            }
            Some("adopt") => {
                let options = Options::parse(
                    &args[2..],
                    &[
                        "--config",
                        "--archive-config",
                        "--from",
                        "--journal",
                        "--import-ledger",
                    ],
                    &[],
                )?;
                let request = store::AdoptRequest {
                    from: options.path("--from")?,
                    journal: options.optional("--journal").map(PathBuf::from),
                    import_ledger: options.optional("--import-ledger").map(PathBuf::from),
                    archive_config: options.optional("--archive-config").map(PathBuf::from),
                };
                let config =
                    IngestConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
                let probe = store::device_probe()?;
                let clock = SystemClock::new();
                let now_ms = || clock.now_utc_ns().div_euclid(1_000_000);
                let first_hot_date = ingest::first_hot_date(now_ms());
                let pass = store::adopt(
                    &config,
                    &request,
                    &first_hot_date,
                    probe.as_ref(),
                    &now_ms,
                    &mut std::io::stdout().lock(),
                )?;
                Ok(exit(pass))
            }
            Some("verify") => {
                let options = Options::parse(
                    &args[2..],
                    &["--config", "--archive-config", "--period-hours"],
                    &[],
                )?;
                let config =
                    IngestConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
                let period = options.value("--period-hours")?.to_owned();
                let archive = options.optional("--archive-config").map(PathBuf::from);
                let probe = store::device_probe()?;
                let clock = SystemClock::new();
                let now_ms = || clock.now_utc_ns().div_euclid(1_000_000);
                let pass = store::verify(
                    &config,
                    archive.as_deref(),
                    &period,
                    probe.as_ref(),
                    &now_ms,
                    &mut std::io::stdout().lock(),
                )?;
                Ok(exit(pass))
            }
            Some("repair") => {
                let options = Options::parse(
                    &args[2..],
                    &["--config", "--archive-config"],
                    &["--record-lost"],
                )?;
                let config =
                    IngestConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
                let archive = options.optional("--archive-config").map(PathBuf::from);
                let probe = store::device_probe()?;
                let clock = SystemClock::new();
                let now_ms = || clock.now_utc_ns().div_euclid(1_000_000);
                let pass = store::repair(
                    &config,
                    archive.as_deref(),
                    options.flag("--record-lost"),
                    probe.as_ref(),
                    &now_ms,
                    &mut std::io::stdout().lock(),
                )?;
                Ok(exit(pass))
            }
            Some("rebuild") => {
                let options = Options::parse(
                    &args[2..],
                    &["--config", "--archive-config"],
                    &["--record-lost", "--record-unlisted", "--replace-hot-ledger"],
                )?;
                let config =
                    IngestConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
                let archive = options.optional("--archive-config").map(PathBuf::from);
                let flags = store::rebuild::Flags {
                    record_lost: options.flag("--record-lost"),
                    record_unlisted: options.flag("--record-unlisted"),
                    replace_hot_ledger: options.flag("--replace-hot-ledger"),
                };
                let probe = store::device_probe()?;
                let clock = SystemClock::new();
                let now_ms = || clock.now_utc_ns().div_euclid(1_000_000);
                let first_hot_date = ingest::first_hot_date(now_ms());
                let pass = store::rebuild(
                    &config,
                    archive.as_deref(),
                    flags,
                    &first_hot_date,
                    probe.as_ref(),
                    &now_ms,
                    &mut std::io::stdout().lock(),
                )?;
                Ok(exit(pass))
            }
            _ => Err(
                "expected `mie store init --config <path>`, `mie store init-root \
                 --config <path> --root <path> --id <id>`, `mie store replicate --config \
                 <path> [--archive-config <path>]`, `mie store adopt --config <path> \
                 [--archive-config <path>] --from <path> [--journal <path>] [--import-ledger \
                 <dir>]`, `mie store verify --config <path> [--archive-config <path>] \
                 --period-hours <P>`, `mie store repair --config <path> [--archive-config \
                 <path>] [--record-lost]` or `mie store rebuild --config <path> \
                 [--archive-config <path>] [--record-lost] [--record-unlisted] \
                 [--replace-hot-ledger]`"
                    .to_owned(),
            ),
        },
        Some(other) => Err(format!("unknown command {other:?}")),
    }
}

fn exit(pass: bool) -> ExitCode {
    if pass {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Exit code of an `archive-kline-check` verdict (ADR-045): 0 pass, 1 fail,
/// 3 inconclusive; 2 stays the usage-error code.
fn verdict_code(verdict: KlineVerdict) -> u8 {
    match verdict {
        KlineVerdict::Pass => 0,
        KlineVerdict::Fail => 1,
        KlineVerdict::Inconclusive => 3,
    }
}

fn run_archive_import(config: &ArchiveConfig, request: &ImportRequest) -> Result<ExitCode, String> {
    let shutdown = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
        signal_hook::flag::register(signal, Arc::clone(&shutdown))
            .map_err(|e| format!("install the signal handler: {e}"))?;
    }
    let client = Arc::new(UreqDownload::new(DownloadTimeouts::default())?);
    let transports = ArchiveTransports {
        http: client.clone(),
        download: client,
        clock: Arc::new(SystemClock::new()),
    };
    let summary = archive::import(
        config,
        request,
        &transports,
        &shutdown,
        &mut std::io::stdout().lock(),
    )?;
    Ok(exit(summary.exit_code() == 0))
}

fn run_ingest(config: &IngestConfig) -> Result<ExitCode, String> {
    let shutdown = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
        signal_hook::flag::register(signal, Arc::clone(&shutdown))
            .map_err(|e| format!("install the signal handler: {e}"))?;
    }
    let timeouts = NetTimeouts::default();
    let transports = Transports {
        connector: Arc::new(TungsteniteConnector::new(timeouts)?),
        http: Arc::new(UreqHttp::new(timeouts)?),
        clock: Arc::new(SystemClock::new()),
        device_probe: Arc::from(store::device_probe()?),
    };
    let outcome = ingest::run(config, transports, shutdown)?;
    if let Some(error) = &outcome.error {
        eprintln!("mie ingest: run {} failed: {error}", outcome.run_id);
    }
    Ok(ExitCode::from(
        u8::try_from(outcome.exit_code()).unwrap_or(1),
    ))
}

/// `--flag value` pairs and valueless `--switch`es, each at most once.
struct Options(Vec<(String, String)>);

impl Options {
    fn parse(args: &[String], allowed: &[&str], switches: &[&str]) -> Result<Self, String> {
        let mut pairs: Vec<(String, String)> = Vec::new();
        let mut rest = args.iter();
        while let Some(flag) = rest.next() {
            let switch = switches.contains(&flag.as_str());
            if !switch && !allowed.contains(&flag.as_str()) {
                return Err(format!("unexpected argument {flag:?}"));
            }
            if pairs.iter().any(|(f, _)| f == flag) {
                return Err(format!("{flag} given twice"));
            }
            let value = if switch {
                String::new()
            } else {
                rest.next()
                    .ok_or_else(|| format!("{flag} needs a value"))?
                    .clone()
            };
            pairs.push((flag.clone(), value));
        }
        Ok(Self(pairs))
    }

    fn flag(&self, switch: &str) -> bool {
        self.0.iter().any(|(f, _)| f == switch)
    }

    fn optional(&self, flag: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(f, _)| f == flag)
            .map(|(_, v)| v.as_str())
    }

    /// `--from` and `--to` as an inclusive range of UTC days.
    fn days(&self) -> Result<(i64, i64), String> {
        let day = |flag: &str| -> Result<i64, String> {
            let value = self.value(flag)?;
            parse_day(value).ok_or_else(|| format!("{flag} {value:?} is not a date YYYY-MM-DD"))
        };
        let (from, to) = (day("--from")?, day("--to")?);
        if from > to {
            return Err("--from is after --to".to_owned());
        }
        Ok((from, to))
    }

    /// `--trade-correction fold|none`, default `fold` (ADR-050 D8).
    fn correction(&self) -> Result<TradeCorrection, String> {
        let value = self.optional("--trade-correction").unwrap_or("fold");
        TradeCorrection::parse(value)
            .ok_or_else(|| format!("--trade-correction {value:?} is neither fold nor none"))
    }

    /// The replay source of `--source live|archive|history` (default live)
    /// over the configuration at `config_path`, with `--streams` and
    /// `--trade-correction` for the archive. The history chain fixes both
    /// (ADR-052 D2).
    fn replay_source(&self, config_path: &Path) -> Result<ReplaySource, String> {
        match self.optional("--source").unwrap_or("live") {
            source @ ("live" | "history") => {
                for flag in ["--streams", "--trade-correction"] {
                    if self.optional(flag).is_some() {
                        return Err(format!("{flag} applies to --source archive only"));
                    }
                }
                let config = IngestConfig::load(config_path).map_err(|e| e.to_string())?;
                Ok(if source == "live" {
                    ReplaySource::Live(config)
                } else {
                    ReplaySource::History(config)
                })
            }
            "archive" => {
                let correction = self.correction()?;
                let config = ArchiveConfig::load(config_path).map_err(|e| e.to_string())?;
                let streams = match self.optional("--streams") {
                    Some(list) => {
                        let names: Vec<String> = list.split(',').map(str::to_owned).collect();
                        Some(
                            ArchiveConfig::expand(&names, &config.archive.kline_intervals)
                                .map_err(|e| e.to_string())?,
                        )
                    }
                    None => None,
                };
                Ok(ReplaySource::Archive {
                    config,
                    streams,
                    correction,
                })
            }
            other => Err(format!(
                "--source {other:?} is not live, archive or history"
            )),
        }
    }

    fn value(&self, flag: &str) -> Result<&str, String> {
        self.0
            .iter()
            .find(|(f, _)| f == flag)
            .map(|(_, v)| v.as_str())
            .ok_or_else(|| format!("{flag} is required"))
    }

    fn path(&self, flag: &str) -> Result<PathBuf, String> {
        self.value(flag).map(PathBuf::from)
    }

    fn fraction(&self, flag: &str, default: f64) -> Result<f64, String> {
        if !self.0.iter().any(|(f, _)| f == flag) {
            return Ok(default);
        }
        let value = self.value(flag)?;
        match value.parse::<f64>() {
            Ok(f) if (0.0..=1.0).contains(&f) => Ok(f),
            _ => Err(format!("{flag} {value:?} is not a fraction in [0, 1]")),
        }
    }

    fn millis(&self, flag: &str) -> Result<i64, String> {
        let value = self.value(flag)?;
        value
            .parse()
            .map_err(|_| format!("{flag} {value:?} is not an integer of epoch milliseconds"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kline_check_verdicts_map_to_distinct_exit_codes() {
        assert_eq!(verdict_code(KlineVerdict::Pass), 0);
        assert_eq!(verdict_code(KlineVerdict::Fail), 1);
        assert_eq!(verdict_code(KlineVerdict::Inconclusive), 3);
    }
}
