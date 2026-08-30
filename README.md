# Shakedown

[![CI](https://github.com/sebastianconcept/shakedown/actions/workflows/ci.yml/badge.svg)](https://github.com/sebastianconcept/shakedown/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/sebastianconcept/shakedown/blob/master/LICENSE)
[![crates.io](https://img.shields.io/crates/v/shakedown.svg)](https://crates.io/crates/shakedown)

**A harness that produces the evidence your team can replay.**

Unit tests are green. Production still breaks. CI rarely exercises the hard parts together the way production does: real databases, real buses, ordering, and I/O under stress.

Shakedown is a Rust library for that gap. Declare scenarios with `#[shakedown::test]`. Start with sqlite or mocks (no Docker). Turn on Cargo features when you need Kafka, Postgres, or the rest.

When a scenario is red, the harness records a **failure timeline** that classifies infrastructure events versus assertion failures. A clean run with a red assertion is your code. An OOM or container restart during seed is the rig. That distinction is the point: evidence you can share with a reviewer who was not in the room.

## Requirements

| Requirement | Notes |
|-------------|--------|
| **Rust ≥ 1.88** | Workspace MSRV (`rust-version`); aligned with testcontainers. CI also checks this toolchain. |
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

Until **1.0**, minor versions may break; [CHANGELOG](https://github.com/sebastianconcept/shakedown/blob/master/CHANGELOG.md) is the contract. Docker is not required for the first scenario. Non-empty `resources` emit `#[ignore]` unless you set `ignore = false`.

## Feature presets

Enable only what your scenarios need. Full matrix: [Operator documentation](https://github.com/sebastianconcept/shakedown/blob/master/crates/lib/OPERATOR.md).

| Stack | Suggested features |
|-------|-------------------|
| SQL only, no Docker | `sqlite` |
| Postgres integration | `postgres` (+ `sqlx` if you use the migrator) |
| Kafka + HTTP mocks | `kafka`, `mock-services`, `services` |
| Full resilience | add `fault-injection`, `load`, `cluster` as needed |

For Kafka clients in tests, use the re-exported crate (`use shakedown::rdkafka::…`) instead of a second `rdkafka` dependency. The harness pins `rdkafka = "=0.38"` so versions cannot drift.

## What you can prove

| Area | What you get |
|------|----------------|
| Standalone resources | Kafka, Postgres, Redis, SQLite, RabbitMQ, and more via opt-in Cargo features (testcontainers-backed where needed) |
| Cluster layouts | Multi-node topologies via `kafka-cluster`, `mongodb-cluster`, `postgres-cluster`, `redis-sentinel`, and `rabbitmq-cluster` (the `cluster` feature provides shared networking) |
| Fault injection | Network faults on proxied dependencies (`fault-injection`) |
| Load and resilience | Load profiles, metrics assertions, and baselines (`load`) |
| Services | Build and inject config for services under test (`services`) |
| Failure classification | Phase-tagged diagnostics and failure timelines so a red run is essential (your code) or accidental (the rig) |

See [Operator documentation](https://github.com/sebastianconcept/shakedown/blob/master/crates/lib/OPERATOR.md) for the full feature matrix, CI setup, and `just` recipes.

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

Neither `cargo test` nor nextest runs `#[ignore]` tests unless you ask (`--include-ignored` or `--run-ignored all`). Details and maintainer smoke recipes: [Operator docs — Running tests](https://github.com/sebastianconcept/shakedown/blob/master/crates/lib/OPERATOR.md#running-tests) and [Adopting in CI](https://github.com/sebastianconcept/shakedown/blob/master/docs/adopting-in-ci.md).

## Going further

- [API reference on docs.rs](https://docs.rs/shakedown): types and modules
- [Quick start on shakedown.dev](https://shakedown.dev/docs/guides/quick-start/): five minutes to a green sqlite scenario
- [Operator docs](https://github.com/sebastianconcept/shakedown/blob/master/crates/lib/OPERATOR.md): features, CI, day-to-day workspace commands
- [In-repo example](https://github.com/sebastianconcept/shakedown/blob/master/crates/examples/examples/service_with_mock.rs): Postgres + Kafka + mock + service under test (Docker)

## Stability (0.x)

Until **1.0**, this crate follows Cargo’s `0.x` semver: minor versions may add features; breaking changes are documented in [CHANGELOG.md](https://github.com/sebastianconcept/shakedown/blob/master/CHANGELOG.md). The 1.0 goal is a stable macro surface (`#[shakedown::test]` / `#[shakedown::faulty_test]`) and core lifecycle API (`Context`, `run_scenario`, `Resource`).

## Repository layout

| Path | Role |
|------|------|
| `crates/lib` | `shakedown` library crate |
| `crates/macros` | `#[shakedown::test]` and related procedural macros |
| `crates/examples` | Runnable adoption scenarios (not published to crates.io) |

## Contributing

See [CONTRIBUTING.md](https://github.com/sebastianconcept/shakedown/blob/master/CONTRIBUTING.md). Security reports: [SECURITY.md](https://github.com/sebastianconcept/shakedown/blob/master/SECURITY.md).

Licensed under the [MIT License](https://github.com/sebastianconcept/shakedown/blob/master/LICENSE).
