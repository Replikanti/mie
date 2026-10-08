//! The `mie` command line.

use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::catalog::parse_day;
use mie_adapter_binance::transport::{
    DownloadTimeouts, NetTimeouts, SystemClock, TungsteniteConnector, UreqDownload, UreqHttp,
};
use mie_cli::archive::{self, ArchiveTransports, ImportRequest};
use mie_cli::config::{ArchiveConfig, IngestConfig};
use mie_cli::ingest::{self, Transports};
use mie_cli::replay::{self, ReplayRequest, ReplaySource};
use mie_cli::report;
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use std::path::PathBuf;
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
        opened. See crates/mie-cli/ingest.example.toml.

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
        or changed upstream. See crates/mie-cli/archive.example.toml.

    mie archive-verify --config <path> --from <YYYY-MM-DD> --to <YYYY-MM-DD>
        Re-read every imported file of the range (hash-verified), normalize
        every record, check ledger <-> store consistency, list trade-id
        breaks and holes over 60 s, print the dataset version per stream.
        A configured stream without an imported file for a period of the
        range fails. Prints PASS or FAIL; exits 0 on PASS, 1 on FAIL.

    mie archive-kline-check --config <path> --from <YYYY-MM-DD> --to <YYYY-MM-DD>
                            [--trade-source aggTrades|trades]
        Build bars from archive trades (default aggTrades) through the
        domain and compare every complete bar with the archive klines
        (ADR-031). Exits 0 only when at least one complete bar was compared
        and every compared bar matched.

    mie replay --config <path> --from <ms|YYYY-MM-DD> --to <ms|YYYY-MM-DD>
               [--source live|archive] [--streams <a,b,...>]
        Replay [from, to) of the raw store through the core (a date is a
        UTC day: --from its start, --to its end, inclusive). live (default)
        reads an ingest config and recomputes every capture run that
        touches the window from its journal; archive reads an archive
        config and merges the backfill (--streams replaces the configured
        streams, which default to all but trades and bookDepth). Prints the
        dataset version, runs or streams, delivered events and gaps,
        trailing gaps, missing days, domain rejections, the final state and
        the event-stream hash; the same window prints the same bytes. Exits
        0 on PASS; 1 when the replay failed, the domain rejected an event or
        the window holds no event.

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
                &["--config", "--from", "--to", "--trade-source"],
                &[],
            )?;
            let config =
                ArchiveConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
            let (from_day, to_day) = options.days()?;
            let trade_source = match options.optional("--trade-source").unwrap_or("aggTrades") {
                "aggTrades" => ArchiveStream::AggTrades,
                "trades" => ArchiveStream::Trades,
                other => {
                    return Err(format!(
                        "--trade-source {other:?} is neither aggTrades nor trades"
                    ));
                }
            };
            let matched = archive::kline_check(
                &config,
                from_day,
                to_day,
                trade_source,
                &mut std::io::stdout().lock(),
            )?;
            Ok(exit(matched))
        }
        Some("replay") => {
            let options = Options::parse(
                &args[1..],
                &["--config", "--from", "--to", "--source", "--streams"],
                &[],
            )?;
            let config_path = options.path("--config")?;
            let from = replay::parse_bound("--from", options.value("--from")?, false)?;
            let to = replay::parse_bound("--to", options.value("--to")?, true)?;
            if from >= to {
                return Err(format!("--from {from} must be below --to {to}"));
            }
            let source = match options.optional("--source").unwrap_or("live") {
                "live" => {
                    if options.optional("--streams").is_some() {
                        return Err("--streams applies to --source archive only".to_owned());
                    }
                    ReplaySource::Live(IngestConfig::load(&config_path).map_err(|e| e.to_string())?)
                }
                "archive" => {
                    let config = ArchiveConfig::load(&config_path).map_err(|e| e.to_string())?;
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
                    ReplaySource::Archive { config, streams }
                }
                other => return Err(format!("--source {other:?} is neither live nor archive")),
            };
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
