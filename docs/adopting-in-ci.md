# Adopting Shakedown in CI

Shakedown is designed for a **two-speed** pipeline: a fast gate that never needs Docker, and a slower job that proves real integrations.

## Fast gate (every push / PR)

Run scenarios that do not start containers. Non-empty `resources = [...]` on
`#[shakedown::test]` emits `#[ignore]` by default; use `ignore = false` for
in-process engines (SQLite, Fake) so the fast job actually runs them.

```bash
# In-memory SQL (tests must use ignore = false to run without --include-ignored)
cargo test --features sqlite

# Or service-under-test unit paths + default mock-services (still skip #[ignore])
cargo test --features services
```

In this repository’s own CI, the fast path is roughly:

```bash
cargo test --features services -p shakedown
```

Ignored Docker tests are **skipped** unless you pass `--include-ignored` (Cargo) or `--run-ignored all` (nextest).

## Docker job (nightly, weekly, or release)

Compile the features you care about and **explicitly** run ignored tests:

```bash
cargo test -p shakedown \
  --features "kafka,postgres,mock-services,services" \
  -- --include-ignored --test-threads=1
```

With [cargo-nextest](https://nexte.st/):

```bash
cargo nextest run -p shakedown \
  --features "kafka,postgres,mock-services,services" \
  --run-ignored all
```

Prefer `--test-threads=1` (or lower nextest parallelism) on small runners so the Docker daemon is not overwhelmed.

## Copy-paste GitHub Actions sketch

```yaml
name: CI

on: [push, pull_request]

jobs:
  fast:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test --features sqlite

  docker:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - name: Integration (Docker)
        run: >-
          cargo test --features "kafka,postgres,mock-services,services"
          -- --include-ignored --test-threads=1
```

## Maintainer smoke (this repository)

For a full sweep of every `tests/*_vertical.rs` engine binary in the `shakedown` workspace:

```bash
just test-shakedown-resources-smoke
```

That recipe enables ignored tests across the resource matrix. See [crates/lib/README.md § Resource smoke coverage](../crates/lib/README.md#resource-smoke-coverage).

## Common pitfall

A job that never passes `--include-ignored` / `--run-ignored all` can stay green while skipping every Docker-backed proof. Treat that as incomplete coverage, not a full shakedown.
