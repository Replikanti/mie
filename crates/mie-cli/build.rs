//! Embeds the code version of the build as `MIE_CODE_VERSION` (ADR-057 D4):
//! `git:<commit>`, with `-dirty` when a tracked file differs from it, or
//! `unknown` outside a git checkout or without git.
//!
//! The outcome dataset names it on its manifest's `code` line, outside the
//! content hash, so a rebuild that changes no versioned rule reproduces the
//! hash and this line explains a hash that moved anyway.
//!
//! The script reruns when the checkout moves: the worktree's `HEAD`, the
//! ref it points at and `packed-refs` (located with `git rev-parse
//! --git-path`, which is right in a linked worktree too), plus the
//! workspace's crates and manifests, whose edits flip `-dirty`.

use std::path::Path;
use std::process::Command;

/// Runs git in the workspace root; its trimmed stdout on success.
fn git(root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .current_dir(root)
        // `git status` refreshes the index when it may; a build must not
        // write into the checkout.
        .env("GIT_OPTIONAL_LOCKS", "0")
        .args(args)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8(output.stdout).ok())
        .flatten()
        .map(|text| text.trim().to_owned())
}

/// Asks cargo to rerun when `path` (as git prints it, relative to `root`
/// or absolute) changes; skipped when it does not exist, since cargo
/// would then rerun on every build.
fn watch(root: &Path, path: &str) {
    let path = root.join(path);
    if path.exists() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR");
    let root = Path::new(&manifest_dir).join("../..");
    for path in ["crates", "Cargo.toml", "Cargo.lock"] {
        watch(&root, path);
    }
    let version = match git(&root, &["rev-parse", "HEAD"]) {
        Some(commit) if commit.len() == 40 && commit.bytes().all(|b| b.is_ascii_hexdigit()) => {
            for path in ["HEAD", "packed-refs"] {
                if let Some(path) = git(&root, &["rev-parse", "--git-path", path]) {
                    watch(&root, &path);
                }
            }
            if let Some(reference) = git(&root, &["symbolic-ref", "-q", "HEAD"])
                && let Some(path) = git(&root, &["rev-parse", "--git-path", &reference])
            {
                watch(&root, &path);
            }
            let dirty = git(&root, &["status", "--porcelain", "--untracked-files=no"])
                .is_none_or(|status| !status.is_empty());
            format!(
                "git:{}{}",
                commit.to_ascii_lowercase(),
                if dirty { "-dirty" } else { "" }
            )
        }
        _ => "unknown".to_owned(),
    };
    println!("cargo:rustc-env=MIE_CODE_VERSION={version}");
}
