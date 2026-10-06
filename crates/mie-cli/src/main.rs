//! The `mie` command line.

use mie_adapter_binance::transport::{NetTimeouts, SystemClock, TungsteniteConnector, UreqHttp};
use mie_cli::config::IngestConfig;
use mie_cli::ingest::{self, Transports};
use mie_cli::report;
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
            let options = Options::parse(&args[1..], &["--config"])?;
            let config =
                IngestConfig::load(&options.path("--config")?).map_err(|e| e.to_string())?;
            run_ingest(&config)
        }
        Some("capture-report") => {
            let options = Options::parse(
                &args[1..],
                &["--config", "--from", "--to", "--max-late-fraction"],
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
        Some(other) => Err(format!("unknown command {other:?}")),
    }
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

/// `--flag value` pairs, each flag exactly once.
struct Options(Vec<(String, String)>);

impl Options {
    fn parse(args: &[String], allowed: &[&str]) -> Result<Self, String> {
        let mut pairs: Vec<(String, String)> = Vec::new();
        let mut rest = args.iter();
        while let Some(flag) = rest.next() {
            if !allowed.contains(&flag.as_str()) {
                return Err(format!("unexpected argument {flag:?}"));
            }
            if pairs.iter().any(|(f, _)| f == flag) {
                return Err(format!("{flag} given twice"));
            }
            let value = rest.next().ok_or_else(|| format!("{flag} needs a value"))?;
            pairs.push((flag.clone(), value.clone()));
        }
        Ok(Self(pairs))
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
