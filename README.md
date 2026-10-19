# Shakedown

[![CI](https://github.com/sebastianconcept/shakedown/actions/workflows/ci.yml/badge.svg)](https://github.com/sebastianconcept/shakedown/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Not published.** `shakedown` 0.1.0 and `shakedown-macros` 0.1.0 are not on crates.io. This tree does not publish them.

**A harness that produces the evidence your team can replay.** Shakedown is a Rust library for realistic system and integration testing. Declare scenarios with `#[shakedown::test]` against real databases, message buses, and other dependencies where it matters, with enough structure to separate product bugs from flaky harness setup. Start without Docker (`sqlite`, mocks); opt into container-backed stacks via Cargo features when you need them.

**Unit tests are green. Production still breaks.** CI rarely exercises the hard parts together the way production does: real databases, real buses, ordering, and I/O under stress. Shakedown is built to cover that gap.

## Requirements

| Requirement | Notes |
|-------------|--------|
| **Rust ≥ 1.88** | Workspace MSRV (`rust-version`); aligned with testcontainers |
| **Docker** | Only for container-backed features (Kafka, Postgres, Redis, …). Not needed for `sqlite` or default `mock-services` |
| **Tokio** | Scenarios use `#[tokio::test]` via `#[shakedown::test]`; add `tokio` with `macros` + `rt-multi-thread` |

## Quick start (no Docker)

Add `shakedown` with the `sqlite` feature (in-memory SQLite per scenario). You also need a Tokio test runtime.

```toml
[dev-dependencies]
shakedown = { version = "0.1", features = ["sqlite"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use shakedown::Context;

// `ignore = false`: non-empty `resources = [...]` would otherwise emit `#[ignore]`
// (Docker-oriented default). SQLite needs no container runtime.
#[shakedown::test(resources = [Sqlite], ignore = false)]
async fn quick_start(ctx: &mut Context) {
    ctx.sqlite()
        .expect("sqlite helpers")
        .execute("CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY)", ())
        .await
        .expect("create table");
}
```

Default features alone also work with a fake resource (no SQL). Use `ignore = false` so the test runs in a normal `cargo test` pass:

```rust
use shakedown::Context;

#[shakedown::test(resources = [Fake], ignore = false)]
async fn smoke(ctx: &mut Context) {
    assert!(!ctx.is_empty());
}
```

## Feature presets

Enable only what your scenarios need. Full matrix: [Operator documentation](crates/lib/README.md).

| Stack | Suggested features |
|-------|-------------------|
| SQL only, no Docker | `sqlite` |
| Postgres integration | `postgres` (+ `sqlx` if you use the migrator) |
| Kafka + HTTP mocks | `kafka`, `mock-services`, `services` |
| Full resilience | add `fault-injection`, `load`, `cluster` as needed |

## What you can prove

| Area | What you get |
|------|----------------|
| Standalone resources | Kafka, Postgres, Redis, SQLite, RabbitMQ, and more via opt-in Cargo features (testcontainers-backed where needed) |
| Cluster layouts | Multi-node topologies via opt-in features such as `kafka-cluster`, `mongodb-cluster`, and `redis-sentinel` (the `cluster` feature provides shared networking) |
| Fault injection | Network faults on proxied dependencies (`fault-injection`) |
| Load and resilience | Load profiles, metrics assertions, and baselines (`load`) |
| Services | Build and inject config for services under test (`services`) |

See [Operator documentation](crates/lib/README.md) for the full feature matrix, CI setup, and `just` recipes.

## Adopting in CI

Keep a **fast** job on every push (no Docker) and a **Docker** job that runs ignored integration tests when you need real infrastructure:

```yaml
jobs:
  fast:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo test --features sqlite   # or services / mock-services — no Docker

  docker:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: >-
          cargo test --features "kafka,postgres,mock-services,services"
          -- --include-ignored
```

Neither `cargo test` nor nextest runs `#[ignore]` tests unless you ask (`--include-ignored` or `--run-ignored all`). Details and maintainer smoke recipes: [Operator docs — Running tests](crates/lib/README.md#running-tests) and [Adopting in CI](docs/adopting-in-ci.md).

## Going further

- [API reference on docs.rs](https://docs.rs/shakedown): types and modules (empty until a publish; 0.1.0 is not published)
- Walkthrough: this README, until a guide exists. `https://shakedown.dev/guide` is a coming-soon page.
- [Operator docs](crates/lib/README.md): features, CI, day-to-day workspace commands
- [In-repo example](crates/examples/examples/service_with_mock.rs): Postgres + Kafka + mock + service under test (Docker)

## Stability (0.x)

Until **1.0**, this crate follows Cargo’s `0.x` semver: minor versions may add features; breaking changes are documented in [CHANGELOG.md](CHANGELOG.md). The 1.0 goal is a stable macro surface (`#[shakedown::test]` / `#[shakedown::faulty_test]`) and core lifecycle API (`Context`, `run_scenario`, `Resource`).

## The Book

*Shakedown Testing for Software* (working title) is the long-form companion: first scenario through outbox flows, diagnostics, clusters, and load. This repository is the implementation. Until a guide exists, start with this README.

## Repository layout

| Path | Role |
|------|------|
| `crates/lib` | `shakedown` library crate |
| `crates/macros` | `#[shakedown::test]` and related procedural macros |
| `crates/examples` | Runnable adoption scenarios (not published to crates.io) |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security reports: [SECURITY.md](SECURITY.md).

Licensed under the [MIT License](LICENSE).
