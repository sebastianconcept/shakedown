# Shakedown documentation

Canonical product and feature documentation for this project lives **outside** the repository:

**`/Users/seb/docs/shakedown/`**

That directory is the adopted-docs root (mission, architecture, MVP spine, adoption guides, feature plans such as `feature/mvp-kafka-vertical/`, archived PRDs, etc.). Edit and version those files there, not under this repo’s `docs/` tree.

This repo may keep code-adjacent notes (e.g. crate READMEs under `crates/`) only.

Repository workflow commands are centralized in the root [`justfile`](../justfile) (`just help`, `just qa`, `just test` vs `just test-workspace`, `just test-cli`, workspace and per-target Docker recipes). For every way to run tests (workspace, all-features, Docker `#[ignore]` binaries, full `--include-ignored` sweep, CI recommendations), see [crates/lib/README.md § Running tests](../crates/lib/README.md#running-tests).
