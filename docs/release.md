# Releasing

A release is a `vX.Y.Z` git tag on a commit in `main`. Pushing the tag runs
[`.github/workflows/release.yml`](../.github/workflows/release.yml), which
tests the workspace, builds the `mie` binary for Linux x86_64 and publishes it
as a GitHub Release with a SHA-256 sidecar and the matching
[`CHANGELOG.md`](../CHANGELOG.md) section as release notes. Nothing is
published to crates.io (`publish = false`).

## Versioning

[Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the version
is below 1.0, a breaking change (CLI flags, config or file formats, anything a
stored result depends on) bumps the **minor** version and is marked
**Breaking:** under `Changed` in the changelog; everything else bumps the
patch. The single source of truth is `[workspace.package] version` in
`Cargo.toml`.

## Recipe

1. **Release PR.**
   - Bump `[workspace.package] version` in `Cargo.toml` (skip when it already
     is the target, as for 0.1.0) and refresh the lockfile with
     `cargo check --workspace`.
   - In `CHANGELOG.md` rename `## [Unreleased]` to
     `## [X.Y.Z] - YYYY-MM-DD` and add a fresh empty `## [Unreleased]` above
     it. Headings are Keep a Changelog: `Added`, `Changed`, `Fixed`,
     `Removed`.
   - Preview the notes: `tools/release-notes.sh X.Y.Z`. It fails when the
     section is missing or empty.
   - The PR body carries the usual `Closes #N` or `No-Issue: <reason>` line.
2. **Tag after the merge.** Tag the squash-merge commit and push the tag
   immediately: `git tag vX.Y.Z <merge-sha> && git push origin vX.Y.Z`. A merge
   without the tag is not a release.
3. **Verify.** The workflow publishes `mie-vX.Y.Z-linux-x86_64` and
   `mie-vX.Y.Z-linux-x86_64.sha256`. Download both and run
   `sha256sum -c mie-vX.Y.Z-linux-x86_64.sha256`.

## What the workflow checks

Before building anything, on tag pushes: the tag equals `v` + the `mie-cli`
workspace version, the tagged commit is an ancestor of `origin/main`, and
`CHANGELOG.md` has a non-empty section for it (no release with an empty body).
Then `cargo test --workspace --locked` and
`cargo build --release --locked -p mie-cli --target x86_64-unknown-linux-gnu`
(`--locked`: a drifting `Cargo.lock` fails the release). The build has a
20 minute timeout; if a cold cache ever exceeds it, raise the timeout rather than drop
the tests.

## Dry run

`Actions > Release > Run workflow` on `main` (or
`gh workflow run release.yml`) builds and tests exactly like a tag push and
uploads the `mie-dist` artifact (binary named `mie-dev-<sha>-linux-x86_64`
plus its `.sha256`), but skips the tag guards and never publishes. Use it to
validate workflow or toolchain changes before cutting a tag.

## A bad release

Never move a published tag silently. Delete the GitHub Release and the tag
(`gh release delete vX.Y.Z --cleanup-tag`), fix forward on `main` and tag
again; if consumers may already have the artifact, publish the fix as the next
patch version instead.

## Reproducibility pin

Research results should name the engine they were produced with. `mie` has no
`--version` flag today, so the pin is the release tag plus its commit sha
(`git rev-parse vX.Y.Z`); the published checksum identifies the binary
itself.
