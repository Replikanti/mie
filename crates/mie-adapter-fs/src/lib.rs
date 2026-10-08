//! MIE research result store on the local filesystem (ADR-040).
//!
//! [`FsResultStore`] implements [`ResearchResultStore`] and
//! [`ResearchResultReader`] as one file per result:
//!
//! ```text
//! <root>/<experiment hex>/<pipeline id>@<N>.result
//! ```
//!
//! A file holds the result text v1 ([`mie_domain::research`]) followed by
//! the trailer line `sha256 <64 hex>`: the SHA-256 of every byte before it.
//!
//! - **Append only.** A result is written to a dot-prefixed temporary file
//!   (`create_new`), synced, made read-only and then hard-linked to its
//!   final name. A link never replaces an existing name, so two writers of
//!   one key cannot both win and a stored result is never overwritten: the
//!   loser gets [`ResultStoreError::AlreadyStored`]. The temporary name is
//!   removed and the directory synced. There is deliberately no
//!   check-then-rename fallback, which would race; a filesystem without hard
//!   links fails with [`ResultStoreError::Io`].
//! - **Collisions.** An append whose spec differs from a stored spec of the
//!   same experiment id fails with [`ResultStoreError::Collision`].
//! - **Verified reads.** A read checks the trailer
//!   ([`ResultStoreError::Integrity`]), parses the text and checks that the
//!   file sits at its own key's path ([`ResultStoreError::Corrupt`]).
//! - **Nothing is deleted.** Dot-files (temporary files a crash left) and
//!   unknown names are ignored, never removed.

use mie_domain::feature::{FeatureRegistry, catalog};
use mie_domain::research::{ExperimentId, ExperimentResult, HypothesisId, PipelineKey, ResultKey};
use mie_ports::outbound::{ResearchResultReader, ResearchResultStore, ResultStoreError};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// The extension of a result file.
pub const RESULT_EXTENSION: &str = "result";

/// The start of the trailer line.
const TRAILER: &str = "sha256 ";

/// Temporary files of this process, numbered so concurrent appends never
/// share a name.
static NEXT_TMP: AtomicU64 = AtomicU64::new(0);

/// The append-only result store under one root directory (module docs).
#[derive(Debug, Clone)]
pub struct FsResultStore {
    root: PathBuf,
    registry: FeatureRegistry,
}

impl FsResultStore {
    /// Opens the store at `root`, creating the directory if needed. Stored
    /// specs resolve their features through the catalog registry.
    ///
    /// # Errors
    ///
    /// [`ResultStoreError::Io`] when the directory cannot be created.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, ResultStoreError> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|e| io(&root, &e))?;
        Ok(Self {
            root,
            registry: catalog::registry(),
        })
    }

    /// The root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Where the result of `key` is stored.
    pub fn path_of(&self, key: &ResultKey) -> PathBuf {
        self.root
            .join(key.experiment.to_string())
            .join(format!("{}.{RESULT_EXTENSION}", key.pipeline))
    }

    /// The keys of the result files of `experiment`, sorted. Dot-files and
    /// other names are skipped.
    fn keys_of(&self, experiment: ExperimentId) -> Result<Vec<ResultKey>, ResultStoreError> {
        let dir = self.root.join(experiment.to_string());
        let mut keys: Vec<ResultKey> = sorted_names(&dir)?
            .iter()
            .filter(|name| !name.starts_with('.'))
            .filter_map(|name| name.strip_suffix(&format!(".{RESULT_EXTENSION}")))
            .filter_map(PipelineKey::parse)
            .map(|pipeline| ResultKey {
                experiment,
                pipeline,
            })
            .collect();
        keys.sort();
        Ok(keys)
    }

    /// Writes `bytes` to a new read-only temporary file in `dir`, synced.
    fn write_tmp(&self, dir: &Path, name: &str, bytes: &[u8]) -> Result<PathBuf, ResultStoreError> {
        loop {
            let n = NEXT_TMP.fetch_add(1, Ordering::Relaxed);
            let tmp = dir.join(format!(".{name}.{}-{n}.tmp", std::process::id()));
            let mut file = match OpenOptions::new().write(true).create_new(true).open(&tmp) {
                Ok(file) => file,
                // A crashed process with the same pid left it: never touch
                // it, take the next number.
                Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(io(&tmp, &e)),
            };
            let written = file
                .write_all(bytes)
                .and_then(|()| file.sync_all())
                .and_then(|()| file.metadata())
                .and_then(|metadata| {
                    let mut permissions = metadata.permissions();
                    permissions.set_readonly(true);
                    file.set_permissions(permissions)
                });
            return match written {
                Ok(()) => Ok(tmp),
                Err(e) => {
                    let _ = fs::remove_file(&tmp);
                    Err(io(&tmp, &e))
                }
            };
        }
    }
}

impl ResearchResultReader for FsResultStore {
    fn get(&self, key: &ResultKey) -> Result<Option<ExperimentResult>, ResultStoreError> {
        let path = self.path_of(key);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io(&path, &e)),
        };
        let text = verify(&bytes).map_err(|detail| {
            ResultStoreError::Integrity(format!("{}: {detail}", path.display()))
        })?;
        let result = ExperimentResult::parse(text, &self.registry)
            .map_err(|e| ResultStoreError::Corrupt(format!("{}: {e}", path.display())))?;
        if result.key() != *key {
            return Err(ResultStoreError::Corrupt(format!(
                "{} holds the result {}, not {key}",
                path.display(),
                result.key()
            )));
        }
        Ok(Some(result))
    }

    fn by_hypothesis(&self, hypothesis: &HypothesisId) -> Result<Vec<ResultKey>, ResultStoreError> {
        let mut found = Vec::new();
        for name in sorted_names(&self.root)? {
            let Some(experiment) = ExperimentId::from_hex(&name) else {
                continue;
            };
            if !self.root.join(&name).is_dir() {
                continue;
            }
            for key in self.keys_of(experiment)? {
                if let Some(result) = self.get(&key)?
                    && result.spec.hypothesis() == hypothesis
                {
                    found.push(key);
                }
            }
        }
        found.sort();
        Ok(found)
    }
}

impl ResearchResultStore for FsResultStore {
    fn append(&mut self, result: &ExperimentResult) -> Result<ResultKey, ResultStoreError> {
        let key = result.key();
        let dir = self.root.join(key.experiment.to_string());
        if !dir.is_dir() {
            fs::create_dir_all(&dir).map_err(|e| io(&dir, &e))?;
            sync_dir(&self.root)?;
        }
        for stored_key in self.keys_of(key.experiment)? {
            if let Some(stored) = self.get(&stored_key)?
                && stored.spec.canonical_text() != result.spec.canonical_text()
            {
                return Err(ResultStoreError::Collision {
                    experiment: key.experiment,
                });
            }
        }

        let text = result.canonical_text();
        let path = self.path_of(&key);
        let name = format!("{}.{RESULT_EXTENSION}", key.pipeline);
        let tmp = self.write_tmp(&dir, &name, &with_trailer(&text))?;
        let linked = fs::hard_link(&tmp, &path);
        // A temporary name that cannot be removed is a dot-file the store
        // ignores; the outcome of the link is what counts.
        let _ = fs::remove_file(&tmp);
        match linked {
            Ok(()) => {
                sync_dir(&dir)?;
                Ok(key)
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                let stored = self.get(&key)?.ok_or_else(|| {
                    ResultStoreError::Io(format!("{} exists but cannot be read", path.display()))
                })?;
                Err(ResultStoreError::AlreadyStored {
                    identical: stored.canonical_text() == text,
                    key,
                })
            }
            Err(e) => Err(io(&path, &e)),
        }
    }
}

/// The result text followed by its trailer line.
fn with_trailer(text: &str) -> Vec<u8> {
    let mut bytes = text.as_bytes().to_vec();
    bytes.extend_from_slice(format!("{TRAILER}{}\n", sha256_hex(text.as_bytes())).as_bytes());
    bytes
}

/// Checks the trailer and returns the text before it.
fn verify(bytes: &[u8]) -> Result<&str, String> {
    let body = bytes
        .strip_suffix(b"\n")
        .ok_or("the file does not end with a line break")?;
    let start = body.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
    let (text, trailer) = bytes.split_at(start);
    let digest = trailer
        .strip_prefix(TRAILER.as_bytes())
        .and_then(|rest| rest.strip_suffix(b"\n"))
        .filter(|hex| hex.len() == 64)
        .ok_or("the sha256 trailer line is missing")?;
    if digest != sha256_hex(text).as_bytes() {
        return Err("the sha256 trailer does not match the content".to_owned());
    }
    std::str::from_utf8(text).map_err(|_| "the content is not UTF-8".to_owned())
}

fn sha256_hex(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .fold(String::with_capacity(64), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

/// The entry names of `dir`, sorted; names that are not UTF-8 are skipped.
fn sorted_names(dir: &Path) -> Result<Vec<String>, ResultStoreError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io(dir, &e)),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| io(dir, &e))?;
        if let Ok(name) = entry.file_name().into_string() {
            names.push(name);
        }
    }
    names.sort();
    Ok(names)
}

fn sync_dir(dir: &Path) -> Result<(), ResultStoreError> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io(dir, &e))
}

fn io(path: &Path, error: &std::io::Error) -> ResultStoreError {
    ResultStoreError::Io(format!("{}: {error}", path.display()))
}
