# Contributing to Shakedown

Thanks for helping improve Shakedown. This document is the short path to a useful PR.

## Prerequisites

- Rust **≥ 1.88** (workspace MSRV)
- [just](https://github.com/casey/just) recommended for local recipes
- Docker for ignored integration / vertical smoke tests

## Before you open a PR

From the repo root:

```bash
just qa
```

That runs format check, `cargo check --workspace`, clippy with `-D warnings`, and workspace tests (nextest when installed).

For Cargo-only parity with some CI setups:

```bash
just qa-full
```

## Quality bar

| Check | Command |
|-------|---------|
| Format | `just fmt-check` or `cargo fmt --all -- --check` |
| Clippy | `just clippy` |
| Tests | `just test` / `cargo test --workspace` |
| Macros UI | `cargo test -p shakedown-macros` (trybuild) |

## MSRV policy

Bump `rust-version` in the workspace [`Cargo.toml`](Cargo.toml) only when a dependency floor (for example testcontainers) requires it. Document the reason in the PR and CHANGELOG.

## Adding a new integration

New engines or backends should follow the existing pattern:

1. Opt-in Cargo feature on `shakedown` (and a matching feature on `shakedown-macros` when `resources = [...]` needs compile-time registration).
2. Implement the async `Resource` lifecycle and bindings/helpers as appropriate.
3. Add a `tests/*_vertical.rs` (or focused integration) binary, usually `#[ignore]` when Docker is required.
4. Register the `[[test]]` target in `crates/lib/Cargo.toml` and keep `scripts/check-vertical-tests-registered.sh` green.
5. Update operator docs in `crates/lib/OPERATOR.md`.

## Docker smoke (maintainers)

```bash
just test-shakedown-resources-smoke
```

Requires a working Docker socket. Prefer serial / low parallelism on small machines.

## API stability (0.x)

Until 1.0, breaking changes may ship in minor versions when necessary; document them in [CHANGELOG.md](CHANGELOG.md). Prefer additive, feature-gated APIs. Do not treat `#[shakedown::register_resource]` as part of the 0.1 contract (it is a reserved pass-through).

## Publishing (maintainers)

Packaging is in-repo (`just package-assert`, `just publish-dry-run-macros`, `just publish-dry-run-lib`). A real publish is **not** a just recipe.

1. Confirm `just package-assert` lists `LICENSE` and no `tests/` paths.
2. Dry-run both crates (`just publish-dry-run-macros`, then `just publish-dry-run-lib` — the library dry-run may warn until the macros crate is on crates.io).
3. crates.io account: verified email and 2FA. Use trusted publishing for `sebastianconcept/shakedown` **or** set repository secret `CARGO_REGISTRY_TOKEN`.
4. Publish macros, wait until that version is on the index, publish the library — **or** push a `v*` git tag so `.github/workflows/release.yml` does that wait for you.
5. Confirm docs.rs builds and the crates.io README renders.
6. `cargo owner --add` if a backup account or org should own the crates.
7. After the upload, set the CHANGELOG date and crates.io / GitHub release links for that version.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
