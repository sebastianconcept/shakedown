# Shakedown documentation

This repository keeps **code-adjacent** documentation: crate READMEs under `crates/`, decision notes such as [`DECISIONS.md`](../DECISIONS.md) at the repo root when present, and feature folders under `feature/` when your checkout includes them.

Some teams also maintain a **separate** product or adoption doc tree (mission, architecture, long-form PRDs). That material is not required to build or test this crate; treat this repo’s READMEs and `docs/` as the source of truth for how to run and extend the code here.

Repository workflow commands are centralized in the root [`justfile`](../justfile) (`just help`, `just qa`, `just test` vs `just test-workspace`, workspace and per-target Docker recipes). For every way to run tests (workspace, all-features, Docker `#[ignore]` binaries, full `--include-ignored` sweep, CI recommendations), see [crates/lib/README.md § Running tests](../crates/lib/README.md#running-tests).
