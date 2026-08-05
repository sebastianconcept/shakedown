# Shakedown documentation

Start here depending on what you need:

| If you want… | Go here |
|--------------|---------|
| Product pitch and no-Docker quick start | [Workspace README](../README.md) |
| Features, CI, helpers, operator workflows | [crates/lib/README.md](../crates/lib/README.md) |
| Adopting Shakedown in your CI | [adopting-in-ci.md](adopting-in-ci.md) |
| `#[shakedown::test]` and related macros | [crates/macros/README.md](../crates/macros/README.md) |
| How this workspace organizes code | [code-organization-guide.md](code-organization-guide.md) |
| API reference | [docs.rs/shakedown](https://docs.rs/shakedown) |
| Walkthrough tutorial | [shakedown.dev/guide](https://shakedown.dev/guide) |

## What lives in this repo

Code-adjacent docs: crate READMEs under `crates/`, this `docs/` folder, and [CHANGELOG.md](../CHANGELOG.md) / [CONTRIBUTING.md](../CONTRIBUTING.md) at the repo root.

Workflow commands live in the root [`justfile`](../justfile) (`just help`, `just qa`, `just test` vs `just test-workspace`, per-target Docker `#[ignore]` recipes). For every way to run library tests (workspace, all-features, Docker `#[ignore]` binaries, full `--include-ignored` sweep, CI recommendations), see [crates/lib/README.md § Running tests](../crates/lib/README.md#running-tests). For a single sweep of every `tests/*_vertical.rs` engine smoke binary (`just test-shakedown-resources-smoke`), see [§ Resource smoke coverage](../crates/lib/README.md#resource-smoke-coverage).

## Book examples

Long-form book chapter tests and companion microservices will live in a sibling **shakedown-example** repository when that material is ready. Until then, start with the [workspace quick start](../README.md) and the in-repo [service_with_mock](../crates/examples/examples/service_with_mock.rs) example.
