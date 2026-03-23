# Feature Plan — mvp-kafka-vertical

**Slug**: `mvp-kafka-vertical`  
**Milestone**: MVP row 2 (MVP.md)  
**Status**: ✅ Completed (US-004 — Kafka bindings, seed topics, readiness probes)

## Overview

Implement the first **real** infrastructure resource (**Kafka**) using testcontainers-modules (Kraft), rdkafka behind kafka feature, and an async, scenario-aware resource API. Refactor run_scenario and Context so provision/readiness/seed/teardown can perform async I/O and attach KafkaBindings (bootstrap + topic prefix + scenario id) to the context.

## Canonical decisions

- Scoping brief decisions: [DECISIONS.md](../../DECISIONS.md) (D-1–D-6)
- Tech stack: [TECH-STACK.md](../../TECH-STACK.md)
- Architecture: [ARCHITECTURE.md](../../ARCHITECTURE.md) §3.7 Resource affinity, §3.9 readiness

## Acceptance criteria

### AC-001: Workspace and dependency baseline

- ✅ Workspace `rust-version` is 1.88 (D-5); root and crate manifests agree
- ✅ `shakedown` declares a `kafka` Cargo feature (default-on per D-2)
- ✅ Heavy deps (`rdkafka`, `testcontainers` family) are behind kafka feature
- ⏳ `cargo check` / `cargo test` without `--features kafka` remains viable

### AC-002: Async, scenario-aware resource contract

- ✅ Public `Resource` trait (`#[async_trait]`) with async provision/seed/readiness/teardown and `scenario_id`
- ✅ First-party resources stored in `BuiltinResource` enum (Fake, Kafka)
- ✅ `Context` holds `Vec<RegisteredResource>` with `Builtin` and `Extension(Box<dyn Resource>)` arms
- ⏳ `FakeResource` and existing lifecycle tests are updated; all pre-existing unit tests pass without Docker

### AC-003: Docker-backed environment and Kafka container

- ✅ Kafka runs via testcontainers-modules `kafka::apache` (KRaft); no ZooKeeper in the default path
- ✅ Image override: `KafkaResource::with_image_tag` / `with_jvm_image` / env `SHAKEDOWN_KAFKA_IMAGE_TAG` + `with_image_tag_from_env`
- ✅ Suite-level broker: `OnceLock` + `Mutex` + ref-count; last teardown runs `rm`; documented in [`SPECS.md`](SPECS.md) and [`VERIFY.md`](VERIFY.md)

### AC-004: Kafka bindings and isolation on Context

- ✅ Bindings expose bootstrap brokers and topic prefix for current scenario
- ✅ Topic creation happens in seed / apply_default_seed, not in readiness-only checks
- ✅ check_ready verifies broker/admin readiness, not full topic graph

### AC-005: Proof integration test

- ⏳ One integration test that requires Docker to pass in CI when enabled
- ⏳ Test demonstrates message flow using bindings from Context (or thin test-only helper)
- ⏳ Test is gated (`--features kafka`, `#[ignore]`, or CI matrix) and documented in VERIFY.md

### AC-006: Documentation and handoff

- ✅ README and progress docs updated; points to next milestone
- ✅ PROGRESS.md links this feature folder (this file)

## Implementation status

| Component | Status | Notes |
|-----------|--------|-------|
| Public async Resource trait | ✅ Done | In `crates/lib/src/resources.rs` |
| BuiltinResource enum (Fake, Kafka) | ✅ Done | Enum dispatch for first-party resources |
| RegisteredResource enum (Builtin, Extension) | ✅ Done | Hybrid dispatch model |
| FakeResource lifecycle impl | ✅ Done | All async methods implemented |
| KafkaResource struct + impl | ✅ Done | Suite-shared KRaft broker, seed/readiness/teardown |
| Context with scenario_id + KafkaBindings | ✅ Done | Bootstrap + topic prefix exposed on `Context`; D-4 semantics documented |
| run_scenario awaits async phases | ✅ Done | Lifecycle properly awaits all async methods |
| Integration test scaffolding | 🔄 In Progress | `tests/kafka_docker.rs` (US-003); produce/consume proof is US-005 |

## Files touched (repo)

- `crates/lib/Cargo.toml` - MSRV 1.88, kafka feature gate
- `crates/lib/src/resources.rs` - Async Resource trait, Fake/Kafka impls, suite broker
- `crates/lib/src/lifecycle.rs` - run_scenario awaits async phases
- `crates/lib/src/lib.rs` - Public API exports
- `crates/lib/tests/kafka_docker.rs` - Docker integration (ignored by default)
- `feature/mvp-kafka-vertical/PROGRESS.md` - This file
- `feature/mvp-kafka-vertical/SPECS.md` - Implementation SPECS (suite broker decision)
- `feature/mvp-kafka-vertical/VERIFY.md` - How to run Docker tests
- `feature/mvp-kafka-vertical/NOTES.md` - Execution log

## Next steps

1. US-005: produce → consume proof scenario using `Context` bindings
2. Wire CI job with Docker for `-- --ignored` on `kafka_docker` (optional until CI ready)

## Open questions

- Panic-in-`run` path does not run teardown yet (see `SPECS.md`)
