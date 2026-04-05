# Shakedown

[![CI](https://github.com/sebastianconcept/shakedown/actions/workflows/ci.yml/badge.svg)](https://github.com/sebastianconcept/shakedown/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![crates.io](https://img.shields.io/crates/v/shakedown.svg)](https://crates.io/crates/shakedown)

The familiar pain is **tests pass, production breaks**: everything looked fine in CI because the hard parts—integrations, ordering, and real I/O—never ran together the way production does. **Shakedown** is a Rust testing library for **progressive confidence**: run scenarios against real infrastructure where it matters, keep fast no-Docker paths for the first win, and leave structured evidence when something fails so you can tell a real regression from a flaky harness.

## Quick start (no Docker)

Add `shakedown` with the **`sqlite`** feature (in-memory SQLite per scenario; no container runtime). You also need a Tokio test runtime.

```toml
[dev-dependencies]
shakedown = { version = "0.1", features = ["sqlite"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use shakedown::Context;

#[shakedown::test(resources = [Sqlite])]
async fn quick_start(ctx: &mut Context) {
    ctx.sqlite()
        .execute("CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY)", ())
        .await
        .expect("create table");
}
```

## What you can test

| Area | What Shakedown helps with |
|------|---------------------------|
| Standalone resources | Kafka, Postgres, Redis, SQLite, RabbitMQ, and other engines via opt-in Cargo features and testcontainers-backed images |
| Cluster layouts | Declarative multi-node topologies where the `cluster` feature applies |
| Fault injection | Network faults on proxied dependencies (`fault-injection` feature) |
| Load & resilience | Load profiles, metrics assertions, and baselines (`load` feature) |
| Services | Build and inject configuration for services under test (`services` feature) |

See [Operator documentation](crates/lib/README.md) for the full feature matrix, CI setup, and `just` recipes.

## Going further

- **[API reference on docs.rs](https://docs.rs/shakedown)** — generated type and module documentation
- **[Online guide](https://shakedown.dev/guide)** — walkthrough-style tutorial
- **[Operator docs](crates/lib/README.md)** — how to enable features, run tests in CI, and operate the workspace day to day

## The Book

***Shakedown Testing for Software*** (working title) is the long-form companion: from your first scenario through outbox flows, diagnostics, cluster topologies, and load. This repository is the implementation; the guide and book outline the methodology and narrative.

## Repository layout

| Path | Role |
|------|------|
| `crates/lib` | `shakedown` library crate |
| `crates/macros` | `#[shakedown::test]` and related procedural macros |
| `crates/examples` | Scenario catalog aligned with the book |

Licensed under the [MIT License](LICENSE).
