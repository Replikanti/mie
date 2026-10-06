//! Sealed-file manifests and the dataset-version text (ADR-030).
//!
//! A manifest is a pure function of its file: no wall-clock field, a fixed
//! line order, `\n` line ends and canonical decimals. The parser is strict
//! and accepts exactly what [`render`] writes.

use crate::layout::{self, PartFile};
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{DatasetVersion, RawStoreError, RawStreamKey, SealedFile, is_sha256_hex};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

/// First line of every manifest: the format name and version.
const HEADER: &str = "mie-raw-manifest 1";

/// The keys after the header, in their fixed order.
const KEYS: [&str; 11] = [
    "path",
    "source",
    "instrument",
    "stream",
    "date",
    "part",
    "rows",
    "bytes",
    "min_event_time_ms",
    "max_event_time_ms",
    "sha256",
];

/// Lowercase hex SHA-256 of `data`.
pub(crate) fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

/// Lowercase hex of a byte string.
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

/// Checks a file's bytes against its manifest: size first, then SHA-256.
///
/// # Errors
///
/// [`RawStoreError::Integrity`] on any mismatch.
pub(crate) fn verify(data: &[u8], expected: &SealedFile) -> Result<(), RawStoreError> {
    if data.len() as u64 != expected.bytes {
        return Err(RawStoreError::Integrity(format!(
            "{}: {} bytes, manifest says {}",
            expected.relative_path,
            data.len(),
            expected.bytes
        )));
    }
    if sha256_hex(data) != expected.sha256 {
        return Err(RawStoreError::Integrity(format!(
            "{}: SHA-256 does not match the manifest",
            expected.relative_path
        )));
    }
    Ok(())
}

/// Renders the manifest of `file`.
pub(crate) fn render(file: &SealedFile) -> String {
    let values = [
        file.relative_path.clone(),
        file.stream.source().to_owned(),
        file.stream.instrument().to_owned(),
        file.stream.stream().to_owned(),
        file.date.clone(),
        file.part.to_string(),
        file.rows.to_string(),
        file.bytes.to_string(),
        file.min_event_time.as_millis().to_string(),
        file.max_event_time.as_millis().to_string(),
        file.sha256.clone(),
    ];
    let mut text = format!("{HEADER}\n");
    for (key, value) in KEYS.iter().zip(values) {
        text.push_str(key);
        text.push(' ');
        text.push_str(&value);
        text.push('\n');
    }
    text
}

/// Parses a manifest strictly: exact header, every key once in order, `\n`
/// line ends, canonical decimals, lowercase hex, and a path that matches the
/// other fields.
///
/// # Errors
///
/// [`RawStoreError::Corrupt`] for anything [`render`] cannot produce.
pub(crate) fn parse(text: &str) -> Result<SealedFile, RawStoreError> {
    let corrupt = |detail: &str| RawStoreError::Corrupt(format!("manifest: {detail}"));
    let body = text
        .strip_suffix('\n')
        .ok_or_else(|| corrupt("missing final line end"))?;
    let mut lines = body.split('\n');
    if lines.next() != Some(HEADER) {
        return Err(corrupt("unknown header or format version"));
    }
    let mut values = Vec::with_capacity(KEYS.len());
    for key in KEYS {
        let line = lines
            .next()
            .ok_or_else(|| corrupt(&format!("missing {key}")))?;
        let value = line
            .strip_prefix(key)
            .and_then(|rest| rest.strip_prefix(' '))
            .ok_or_else(|| corrupt(&format!("expected {key}, found {line:?}")))?;
        if value.is_empty() || value.contains([' ', '\r']) {
            return Err(corrupt(&format!("malformed {key} value {value:?}")));
        }
        values.push(value);
    }
    if lines.next().is_some() {
        return Err(corrupt("unexpected extra line"));
    }
    let [
        path,
        source,
        instrument,
        stream,
        date,
        part,
        rows,
        bytes,
        min,
        max,
        sha256,
    ] = values[..]
    else {
        return Err(corrupt("wrong number of fields"));
    };
    let number = |key: &str, text: &str| -> Result<u64, RawStoreError> {
        let canonical =
            text.bytes().all(|b| b.is_ascii_digit()) && (text == "0" || !text.starts_with('0'));
        canonical
            .then(|| text.parse().ok())
            .flatten()
            .ok_or_else(|| corrupt(&format!("{key} {text:?} is not a canonical decimal")))
    };
    let time = |key: &str, text: &str| -> Result<EventTime, RawStoreError> {
        let millis = i64::try_from(number(key, text)?)
            .map_err(|_| corrupt(&format!("{key} out of range")))?;
        layout::civil_date(millis).map_err(|_| corrupt(&format!("{key} out of range")))?;
        Ok(EventTime::from_millis(millis))
    };
    let stream = RawStreamKey::new(source, instrument, stream)
        .map_err(|e| corrupt(&format!("bad stream key: {e}")))?;
    let part = u32::try_from(number("part", part)?)
        .ok()
        .filter(|p| *p <= layout::MAX_PART)
        .ok_or_else(|| corrupt("part out of range"))?;
    if layout::parse_date(date).is_none() {
        return Err(corrupt(&format!("bad date {date:?}")));
    }
    if !is_sha256_hex(sha256) {
        return Err(corrupt("sha256 is not 64 lowercase hex characters"));
    }
    let file = SealedFile {
        relative_path: path.to_owned(),
        stream,
        date: date.to_owned(),
        part,
        rows: number("rows", rows)?,
        bytes: number("bytes", bytes)?,
        min_event_time: time("min_event_time_ms", min)?,
        max_event_time: time("max_event_time_ms", max)?,
        sha256: sha256.to_owned(),
    };
    if file.relative_path != layout::file_path(&file.stream, &file.date, PartFile::Data, part) {
        return Err(corrupt(
            "path does not match source, instrument, stream, date and part",
        ));
    }
    if file.min_event_time > file.max_event_time {
        return Err(corrupt("min_event_time_ms exceeds max_event_time_ms"));
    }
    Ok(file)
}

/// The canonical text a dataset version hashes (ADR-030). `files` must be
/// sorted by path. Stream lines are sorted here, bytewise on the rendered
/// `source/instrument/stream` text, which the ADR fixes. That order differs
/// from [`RawStreamKey`]'s field-wise order: field-wise `binance` precedes
/// `binance-um`, bytewise `binance-um/…` precedes `binance/…` (`-` < `/`).
pub(crate) fn dataset_text<'a>(
    window: ReplayWindow,
    streams: impl IntoIterator<Item = &'a RawStreamKey>,
    files: &[SealedFile],
) -> String {
    let mut text = format!(
        "mie-dataset 1\nwindow {} {}\n",
        window.start.as_millis(),
        window.end.as_millis()
    );
    let mut rendered: Vec<String> = streams.into_iter().map(ToString::to_string).collect();
    rendered.sort_unstable();
    rendered.dedup();
    for stream in rendered {
        let _ = writeln!(text, "stream {stream}");
    }
    for file in files {
        let _ = writeln!(
            text,
            "file {} {} {}",
            file.relative_path, file.rows, file.sha256
        );
    }
    text
}

/// The dataset version of a selection's canonical text.
pub(crate) fn dataset_version<'a>(
    window: ReplayWindow,
    streams: impl IntoIterator<Item = &'a RawStreamKey>,
    files: &[SealedFile],
) -> DatasetVersion {
    let hex = sha256_hex(dataset_text(window, streams, files).as_bytes());
    DatasetVersion::from_hex(&hex).expect("SHA-256 hex is a valid dataset version")
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOLDEN: &str = "mie-raw-manifest 1
path source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06/part-00007.parquet
source binance-um
instrument BTCUSDT
stream aggTrade
date 2026-10-06
part 7
rows 1000
bytes 52344
min_event_time_ms 1791244800000
max_event_time_ms 1791248399999
sha256 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
";

    fn golden_file() -> SealedFile {
        let stream = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
        SealedFile {
            relative_path: layout::file_path(&stream, "2026-10-06", PartFile::Data, 7),
            stream,
            date: "2026-10-06".to_owned(),
            part: 7,
            rows: 1000,
            bytes: 52_344,
            min_event_time: EventTime::from_millis(1_791_244_800_000),
            max_event_time: EventTime::from_millis(1_791_248_399_999),
            sha256: sha256_hex(b"test"),
        }
    }

    #[test]
    fn sha256_matches_the_standard_vector() {
        assert_eq!(
            sha256_hex(b"test"),
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        );
    }

    #[test]
    fn manifest_text_is_pinned_byte_for_byte() {
        assert_eq!(render(&golden_file()), GOLDEN);
    }

    #[test]
    fn manifests_round_trip() {
        assert_eq!(parse(GOLDEN).unwrap(), golden_file());
        let mut zero = golden_file();
        zero.part = 0;
        zero.relative_path = layout::file_path(&zero.stream, &zero.date, PartFile::Data, 0);
        zero.min_event_time = EventTime::from_millis(0);
        assert_eq!(parse(&render(&zero)).unwrap(), zero);
    }

    #[test]
    fn the_parser_is_strict() {
        let lines: Vec<&str> = GOLDEN.lines().collect();
        let join = |lines: &[&str]| lines.iter().map(|l| format!("{l}\n")).collect::<String>();
        let replace = |from: &str, to: &str| GOLDEN.replacen(from, to, 1);

        let mut missing = lines.clone();
        missing.remove(6);
        let mut extra = lines.clone();
        extra.push("note hello");
        let mut reordered = lines.clone();
        reordered.swap(2, 3);
        let bad = [
            join(&missing),
            join(&extra),
            join(&reordered),
            GOLDEN.replace('\n', "\r\n"),
            GOLDEN.trim_end().to_owned(),
            replace("rows 1000", "rows 1000 "),
            replace("rows 1000", "rows +1000"),
            replace("rows 1000", "rows 01000"),
            replace("rows 1000", "rows -1"),
            replace("rows 1000", "rows  1000"),
            replace("part 7", "part 100000"),
            replace("sha256 9f86", "sha256 9F86"),
            replace("sha256 9f86", "sha256 9f8"),
            replace("mie-raw-manifest 1", "mie-raw-manifest 2"),
            replace("date 2026-10-06", "date 2026-10-07"),
            replace("stream aggTrade", "stream depth"),
            replace("part 7", "part 8"),
            replace("max_event_time_ms 1791248399999", "max_event_time_ms 1"),
            replace(
                "max_event_time_ms 1791248399999",
                "max_event_time_ms 253402300800000",
            ),
            String::new(),
        ];
        for text in bad {
            assert!(
                matches!(parse(&text), Err(RawStoreError::Corrupt(_))),
                "{text:?}"
            );
        }
    }

    #[test]
    fn dataset_text_follows_the_documented_layout() {
        let file = golden_file();
        let window = ReplayWindow {
            start: EventTime::from_millis(10),
            end: EventTime::from_millis(20),
        };
        let text = dataset_text(window, [&file.stream], std::slice::from_ref(&file));
        assert_eq!(
            text,
            format!(
                "mie-dataset 1\nwindow 10 20\nstream binance-um/BTCUSDT/aggTrade\nfile {} 1000 {}\n",
                file.relative_path, file.sha256
            )
        );
        assert_eq!(
            dataset_version(window, [&file.stream], std::slice::from_ref(&file)).as_str(),
            sha256_hex(text.as_bytes())
        );
    }
}
