# Shakedown documentation

Canonical product and feature documentation for this project lives **outside** the repository:

**`/Users/seb/docs/shakedown/`**

That directory is the adopted-docs root (mission, architecture, MVP spine, adoption guides, feature plans such as `feature/mvp-kafka-vertical/`, archived PRDs, etc.). Edit and version those files there, not under this repo’s `docs/` tree.

This repo may keep code-adjacent notes (e.g. crate READMEs under `crates/`) only.

Repository workflow commands are centralized in the root [`justfile`](../justfile) (`just help`, `just qa`, and targeted test recipes).
