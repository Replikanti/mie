//! The dataset manifest and its content hash (ADR-057 D3).
//!
//! UTF-8, `\n` line ends, lines in this fixed order:
//!
//! ```text
//! mie-outcome-dataset 1
//! source <archive|live> <source>/<instrument> streams <list> correction <fold|none>
//! replay <start ms> <end ms>
//! records <start ms> <end ms>
//! data <64 hex>
//! engine <engine key>
//! sampling <id@N> <16 hex>
//! labeling <id@N> <16 hex>
//! events <n> rejections <n>
//! file contexts.parquet <rows> <sha256>
//! file outcomes.parquet <rows> <sha256>
//! file passages.parquet <rows> <sha256>
//! content <sha256 of every byte before this line>
//! code <code version>
//! ```
//!
//! The code version is the one line outside the content hash: a rebuild on
//! another commit that changes no versioned rule reproduces the hash. The
//! parser is strict: it accepts exactly what [`OutcomeManifest::render`]
//! writes.

use super::{OutcomeError, TABLES};
use crate::manifest::sha256_hex;
use mie_ports::raw::is_sha256_hex;

/// First line of every manifest: the format name and version.
const HEADER: &str = "mie-outcome-dataset 1";

/// One table of a dataset as its manifest names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeFile {
    /// The file name, one of [`TABLES`].
    pub name: String,
    /// Its rows.
    pub rows: u64,
    /// Lowercase hex SHA-256 of its bytes.
    pub sha256: String,
}

/// The manifest of an outcome dataset (ADR-057 D3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeManifest {
    /// The `source` line's value: kind, source/instrument, streams and
    /// trade correction.
    pub source: String,
    /// The replay window, `[start, end)` in epoch ms.
    pub replay: (i64, i64),
    /// The records window, `[start, end)` in epoch ms.
    pub records: (i64, i64),
    /// The raw data version (ADR-030), 64 lowercase hex.
    pub data: String,
    /// The engine key (ADR-041): feature-set version and state encoding.
    pub engine: String,
    /// The sampling policy `id@N` (ADR-054 D5).
    pub sampling: String,
    /// Its fingerprint, 16 lowercase hex.
    pub sampling_fingerprint: String,
    /// The labeling `id@N` (ADR-055 D10, ADR-056 D10′).
    pub labeling: String,
    /// Its fingerprint, 16 lowercase hex.
    pub labeling_fingerprint: String,
    /// Events the replay delivered, rejected ones included.
    pub events: u64,
    /// Events the engine rejected.
    pub rejections: u64,
    /// The tables, in [`TABLES`] order.
    pub files: Vec<OutcomeFile>,
    /// SHA-256 of every manifest byte before the `content` line.
    pub content: String,
    /// The code version: `git:<40 hex>`, with `-dirty`, or `unknown`.
    pub code: String,
}

impl OutcomeManifest {
    /// The lines the content hash covers: everything before `content`.
    pub(crate) fn hashed_text(&self) -> String {
        let mut text = format!("{HEADER}\n");
        text.push_str(&format!("source {}\n", self.source));
        text.push_str(&format!("replay {} {}\n", self.replay.0, self.replay.1));
        text.push_str(&format!("records {} {}\n", self.records.0, self.records.1));
        text.push_str(&format!("data {}\n", self.data));
        text.push_str(&format!("engine {}\n", self.engine));
        text.push_str(&format!(
            "sampling {} {}\n",
            self.sampling, self.sampling_fingerprint
        ));
        text.push_str(&format!(
            "labeling {} {}\n",
            self.labeling, self.labeling_fingerprint
        ));
        text.push_str(&format!(
            "events {} rejections {}\n",
            self.events, self.rejections
        ));
        for file in &self.files {
            text.push_str(&format!(
                "file {} {} {}\n",
                file.name, file.rows, file.sha256
            ));
        }
        text
    }

    /// The content hash of the hashed lines.
    pub(crate) fn compute_content(&self) -> String {
        sha256_hex(self.hashed_text().as_bytes())
    }

    /// The manifest text.
    pub fn render(&self) -> String {
        format!(
            "{}content {}\ncode {}\n",
            self.hashed_text(),
            self.content,
            self.code
        )
    }

    /// The row count the manifest gives `name`, if it lists it.
    pub fn rows(&self, name: &str) -> Option<u64> {
        self.files
            .iter()
            .find(|file| file.name == name)
            .map(|file| file.rows)
    }

    /// Parses a manifest strictly, without checking the content hash.
    ///
    /// # Errors
    ///
    /// [`OutcomeError::Integrity`] for any text [`render`](Self::render)
    /// cannot produce.
    pub fn parse(text: &str) -> Result<Self, OutcomeError> {
        let bad = |detail: String| OutcomeError::Integrity(format!("manifest: {detail}"));
        let body = text
            .strip_suffix('\n')
            .ok_or_else(|| bad("missing final line end".to_owned()))?;
        let mut lines = body.split('\n');
        let mut next = |key: &str| -> Result<&str, OutcomeError> {
            let line = lines
                .next()
                .ok_or_else(|| bad(format!("missing {key} line")))?;
            line.strip_prefix(key)
                .and_then(|rest| rest.strip_prefix(' '))
                .ok_or_else(|| bad(format!("expected {key}, found {line:?}")))
        };
        let words = |value: &str, n: usize, key: &str| -> Result<Vec<String>, OutcomeError> {
            let words: Vec<String> = value.split(' ').map(str::to_owned).collect();
            if words.len() != n || words.iter().any(String::is_empty) {
                return Err(bad(format!("malformed {key} value {value:?}")));
            }
            Ok(words)
        };
        let number = |text: &str| -> Result<i64, OutcomeError> {
            text.parse()
                .map_err(|_| bad(format!("{text:?} is not a number")))
        };
        let count = |text: &str| -> Result<u64, OutcomeError> {
            text.parse()
                .map_err(|_| bad(format!("{text:?} is not a count")))
        };

        let header = next("mie-outcome-dataset")?;
        if header != "1" {
            return Err(bad(format!("unknown format version {header:?}")));
        }
        let source = next("source")?.to_owned();
        let replay = words(next("replay")?, 2, "replay")?;
        let records = words(next("records")?, 2, "records")?;
        let data = words(next("data")?, 1, "data")?;
        let engine = words(next("engine")?, 1, "engine")?;
        let sampling = words(next("sampling")?, 2, "sampling")?;
        let labeling = words(next("labeling")?, 2, "labeling")?;
        let events = words(next("events")?, 3, "events")?;
        if events[1] != "rejections" {
            return Err(bad("expected `events <n> rejections <n>`".to_owned()));
        }
        let mut files = Vec::with_capacity(TABLES.len());
        for name in TABLES {
            let file = words(next("file")?, 3, "file")?;
            if file[0] != name || !is_sha256_hex(&file[2]) {
                return Err(bad(format!("expected file {name} <rows> <sha256>")));
            }
            files.push(OutcomeFile {
                name: file[0].clone(),
                rows: count(&file[1])?,
                sha256: file[2].clone(),
            });
        }
        let content = words(next("content")?, 1, "content")?;
        let code = words(next("code")?, 1, "code")?;
        if lines.next().is_some() {
            return Err(bad("unexpected extra line".to_owned()));
        }
        if !is_sha256_hex(&data[0]) || !is_sha256_hex(&content[0]) {
            return Err(bad("data or content is not 64 lowercase hex".to_owned()));
        }
        let manifest = Self {
            source,
            replay: (number(&replay[0])?, number(&replay[1])?),
            records: (number(&records[0])?, number(&records[1])?),
            data: data[0].clone(),
            engine: engine[0].clone(),
            sampling: sampling[0].clone(),
            sampling_fingerprint: sampling[1].clone(),
            labeling: labeling[0].clone(),
            labeling_fingerprint: labeling[1].clone(),
            events: count(&events[0])?,
            rejections: count(&events[2])?,
            files,
            content: content[0].clone(),
            code: code[0].clone(),
        };
        // Canonical form only: no leading zeros, no `+`, nothing the
        // renderer would write differently.
        if manifest.render() != text {
            return Err(bad("not in canonical form".to_owned()));
        }
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> OutcomeManifest {
        let mut manifest = OutcomeManifest {
            source: "archive binance-archive/BTCUSDT streams aggTrades,metrics correction fold"
                .to_owned(),
            replay: (1_759_276_800_000, 1_790_812_800_000),
            records: (1_759_276_800_000, 1_790_812_800_000),
            data: "5d".repeat(32),
            engine: "4c844593779da2e3-1".to_owned(),
            sampling: "conditional.sampling@1".to_owned(),
            sampling_fingerprint: "2c71fff23e8b0415".to_owned(),
            labeling: "conditional.labels@2".to_owned(),
            labeling_fingerprint: "6f90bff079e746f2".to_owned(),
            events: 603_323_345,
            rejections: 0,
            files: TABLES
                .iter()
                .zip([561_695, 2_808_475, 70_000])
                .map(|(name, rows)| OutcomeFile {
                    name: (*name).to_owned(),
                    rows,
                    sha256: "ab".repeat(32),
                })
                .collect(),
            content: String::new(),
            code: "git:0123456789abcdef0123456789abcdef01234567".to_owned(),
        };
        manifest.content = manifest.compute_content();
        manifest
    }

    #[test]
    fn golden_text() {
        let manifest = sample();
        let text = manifest.render();
        let expected = format!(
            "mie-outcome-dataset 1\n\
             source archive binance-archive/BTCUSDT streams aggTrades,metrics correction fold\n\
             replay 1759276800000 1790812800000\n\
             records 1759276800000 1790812800000\n\
             data {data}\n\
             engine 4c844593779da2e3-1\n\
             sampling conditional.sampling@1 2c71fff23e8b0415\n\
             labeling conditional.labels@2 6f90bff079e746f2\n\
             events 603323345 rejections 0\n\
             file contexts.parquet 561695 {h}\n\
             file outcomes.parquet 2808475 {h}\n\
             file passages.parquet 70000 {h}\n\
             content {content}\n\
             code git:0123456789abcdef0123456789abcdef01234567\n",
            data = "5d".repeat(32),
            h = "ab".repeat(32),
            content = manifest.content,
        );
        assert_eq!(text, expected);
        // The content hash covers every byte before its own line.
        let hashed = &text[..text.find("content ").unwrap()];
        assert_eq!(manifest.content, sha256_hex(hashed.as_bytes()));
        assert_eq!(OutcomeManifest::parse(&text).unwrap(), manifest);
    }

    #[test]
    fn the_code_version_is_outside_the_content_hash() {
        let manifest = sample();
        let mut other = manifest.clone();
        other.code = "unknown".to_owned();
        assert_eq!(other.compute_content(), manifest.content);
        let mut moved = manifest.clone();
        moved.records.1 -= 1;
        assert_ne!(moved.compute_content(), manifest.content);
    }

    #[test]
    fn the_parser_is_strict() {
        let text = sample().render();
        let broken = [
            text.replacen("mie-outcome-dataset 1", "mie-outcome-dataset 2", 1),
            text.replacen("replay 1759276800000", "replay 01759276800000", 1),
            text.replacen("events 603323345", "events +603323345", 1),
            text.replacen("file outcomes.parquet", "file passages.parquet", 1),
            text.replacen("\ncode ", "\ncode  ", 1),
            text.trim_end().to_owned(),
            format!("{text}extra\n"),
            text.replace('\n', "\r\n"),
            text.replacen("data 5d", "data 5D", 1),
        ];
        for text in broken {
            assert!(
                matches!(
                    OutcomeManifest::parse(&text),
                    Err(OutcomeError::Integrity(_))
                ),
                "{text}"
            );
        }
    }
}
