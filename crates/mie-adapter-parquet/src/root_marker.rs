//! Root markers and the creation of a root's `.mie/` (ADR-062 D4, D7).
//!
//! Every root of a tiered store carries `<root>/.mie/root`, written once
//! when the root is initialized:
//!
//! ```text
//! mie-root 1
//! id <id>
//! ```
//!
//! The id passes [`validate_segment`]. Hot's marker always carries
//! [`HOT_ID`]: copy-ledger records only ever name copy targets (ADR-062 D11,
//! the hot copy never counts), so an operator-chosen hot id would carry no
//! information, and a fixed id lets every check recognize hot's marker
//! under any spelling of its path. The marker is what tells a mounted root
//! from an unmounted mount point, which is an ordinary empty directory.

use crate::layout::{COPY_LEDGER, META_DIR, ROOT_MARKER};
use crate::{copy_ledger, io_error, sync_dir};
use mie_ports::raw::{RawStoreError, validate_segment};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

/// The id of hot's marker, reserved: no other root may carry it.
pub const HOT_ID: &str = "hot";

/// The first line of every marker: the format name and version.
const HEADER: &str = "mie-root 1";

/// The staging directory of [`create_meta`], renamed to `.mie` when
/// complete.
const INIT_TMP: &str = ".mie-init.tmp";

/// The `.mie/` directory of `root`.
pub fn meta_dir(root: &Path) -> PathBuf {
    root.join(META_DIR)
}

/// The marker path of `root`.
pub fn marker_path(root: &Path) -> PathBuf {
    meta_dir(root).join(ROOT_MARKER)
}

/// The marker text of `id`.
///
/// # Errors
///
/// [`RawStoreError::Invalid`] when the id fails [`validate_segment`].
pub fn render(id: &str) -> Result<String, RawStoreError> {
    validate_segment(id)?;
    Ok(format!("{HEADER}\nid {id}\n"))
}

/// Parses a marker strictly: exactly what [`render`] writes. Returns the id.
///
/// # Errors
///
/// [`RawStoreError::Corrupt`] for anything else.
pub fn parse(text: &str) -> Result<String, RawStoreError> {
    let corrupt = |detail: &str| RawStoreError::Corrupt(format!("root marker: {detail}"));
    let body = text
        .strip_suffix('\n')
        .ok_or_else(|| corrupt("missing final line end"))?;
    let (header, id_line) = body
        .split_once('\n')
        .ok_or_else(|| corrupt("missing id line"))?;
    if header != HEADER {
        return Err(corrupt("unknown header or format version"));
    }
    let id = id_line
        .strip_prefix("id ")
        .ok_or_else(|| corrupt(&format!("expected `id <id>`, found {id_line:?}")))?;
    validate_segment(id).map_err(|e| corrupt(&format!("bad id: {e}")))?;
    Ok(id.to_owned())
}

/// Reads the marker of `root`. `None` only when it does not exist.
///
/// # Errors
///
/// [`RawStoreError::Corrupt`] for a malformed marker, [`RawStoreError::Io`]
/// for any read error other than "not found" (an unreadable root is never
/// taken for a root without a marker).
pub fn read(root: &Path) -> Result<Option<String>, RawStoreError> {
    let path = marker_path(root);
    match fs::read(&path) {
        Ok(bytes) => {
            let text = String::from_utf8(bytes).map_err(|_| {
                RawStoreError::Corrupt(format!("root marker {}: not UTF-8", path.display()))
            })?;
            parse(&text).map(Some).map_err(|error| match error {
                RawStoreError::Corrupt(detail) => {
                    RawStoreError::Corrupt(format!("{detail} ({})", path.display()))
                }
                other => other,
            })
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("read", &path)(error)),
    }
}

/// Creates `<root>/.mie/` holding the marker of `id` and, with
/// `with_ledger` (hot), a copy ledger with its header only.
///
/// Everything is written into `<root>/.mie-init.tmp/`, fsynced, and the
/// directory is renamed to `.mie` and the root fsynced: after a crash there
/// is either no `.mie/` or a complete one. Two separate file renames could
/// leave a marker without a ledger, which looks like a degraded start, or a
/// ledger without a marker, which looks like a lost hot. A leftover
/// `.mie-init.tmp` of a crashed earlier attempt is removed first.
///
/// # Errors
///
/// [`RawStoreError::Invalid`] when `<root>/.mie` exists or the id is
/// invalid; [`RawStoreError::Io`] on I/O failure.
pub fn create_meta(root: &Path, id: &str, with_ledger: bool) -> Result<(), RawStoreError> {
    let marker = render(id)?;
    let meta = meta_dir(root);
    match fs::symlink_metadata(&meta) {
        Ok(_) => {
            return Err(RawStoreError::Invalid(format!(
                "{} already exists",
                meta.display()
            )));
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(io_error("inspect", &meta)(error)),
    }
    let staging = root.join(INIT_TMP);
    match fs::remove_dir_all(&staging) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(io_error("remove", &staging)(error)),
    }
    fs::create_dir(&staging).map_err(io_error("create", &staging))?;
    let mut files = vec![(ROOT_MARKER, marker)];
    if with_ledger {
        files.push((COPY_LEDGER, copy_ledger::HEADER.to_owned()));
    }
    for (name, text) in files {
        let path = staging.join(name);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(io_error("create", &path))?;
        file.write_all(text.as_bytes())
            .and_then(|()| file.sync_all())
            .map_err(io_error("write", &path))?;
    }
    sync_dir(&staging)?;
    // A concurrent init that created a non-empty `.mie` in between makes
    // this rename fail (the target is not empty).
    fs::rename(&staging, &meta).map_err(io_error("rename into place", &meta))?;
    sync_dir(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;

    #[test]
    fn the_marker_text_is_pinned() {
        assert_eq!(render("cold-a").unwrap(), "mie-root 1\nid cold-a\n");
        assert_eq!(parse("mie-root 1\nid cold-a\n").unwrap(), "cold-a");
        assert_eq!(parse(&render(HOT_ID).unwrap()).unwrap(), HOT_ID);
        assert!(matches!(render("a/b"), Err(RawStoreError::Invalid(_))));
    }

    #[test]
    fn the_marker_parser_is_strict() {
        for bad in [
            "",
            "mie-root 1\nid cold-a",
            "mie-root 2\nid cold-a\n",
            "mie-root 1\n",
            "mie-root 1\nid  cold-a\n",
            "mie-root 1\nid cold-a \n",
            "mie-root 1\nid cold/a\n",
            "mie-root 1\nid \n",
            "mie-root 1\nid cold-a\nid cold-b\n",
            "mie-root 1\r\nid cold-a\r\n",
            "mie-root 1\nname cold-a\n",
        ] {
            assert!(
                matches!(parse(bad), Err(RawStoreError::Corrupt(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn create_meta_writes_atomically_once() {
        let dir = TempDir::new("marker");
        let root = dir.path();
        assert_eq!(read(root).unwrap(), None);

        // A leftover staging directory of a crashed attempt is replaced.
        fs::create_dir(root.join(INIT_TMP)).unwrap();
        fs::write(root.join(INIT_TMP).join("junk"), b"x").unwrap();
        create_meta(root, HOT_ID, true).unwrap();
        assert!(!root.join(INIT_TMP).exists());
        assert_eq!(read(root).unwrap().as_deref(), Some(HOT_ID));
        assert_eq!(
            fs::read_to_string(meta_dir(root).join(COPY_LEDGER)).unwrap(),
            copy_ledger::HEADER
        );
        let mut names: Vec<String> = fs::read_dir(meta_dir(root))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, ["copy-ledger", "root"]);

        // An existing `.mie` is refused and left alone.
        assert!(matches!(
            create_meta(root, "cold-a", false),
            Err(RawStoreError::Invalid(_))
        ));
        assert_eq!(read(root).unwrap().as_deref(), Some(HOT_ID));

        // Without the ledger: the marker only.
        let cold = TempDir::new("marker-cold");
        create_meta(cold.path(), "cold-a", false).unwrap();
        assert_eq!(read(cold.path()).unwrap().as_deref(), Some("cold-a"));
        assert!(!meta_dir(cold.path()).join(COPY_LEDGER).exists());
    }

    #[test]
    fn a_corrupt_marker_is_an_error_not_an_absence() {
        let dir = TempDir::new("marker-corrupt");
        fs::create_dir(meta_dir(dir.path())).unwrap();
        fs::write(marker_path(dir.path()), b"mie-root 1\nid hot").unwrap();
        assert!(matches!(read(dir.path()), Err(RawStoreError::Corrupt(_))));
        fs::write(marker_path(dir.path()), [0xff, 0xfe]).unwrap();
        assert!(matches!(read(dir.path()), Err(RawStoreError::Corrupt(_))));
    }
}
