# Shakedown — Orchestration library for shakedown testing

**Shakedown** is a Rust framework for shakedown testing — proving systems are safe to
promote to production by running realistic scenarios that exercise the full stack.

## Overview

This library provides:

- **Scenario orchestration** — lifecycle phases: provision → readiness → seed → run → assert → teardown
- **Resource providers** — async [`Resource`] trait for databases, message queues, and other infrastructure
- **Error classification** — structured errors distinguishing scenario vs infrastructure failures
- **Event logging hooks** — structured event collection for failure timelines

## Workspace layout

```text
shakedown/
├── crates/
│   ├── lib/          # this crate: orchestration engine
│   ├── macros/       # procedural macros (shakedown-macros)
│   └── cli/          # CLI binary (shakedown-cli)
```

## Extension points

- **[`Resource`] trait** — implement for new infrastructure (with `async_trait` for `Box<dyn Resource>` extensions)
- **[`EnvironmentBackend`] trait** — implement for alternative execution environments (also `async_trait` for `dyn` use)
- **`Context::register_builtin` / `register_extension`** — wire first-party or third-party resources

## Milestone status

- **Current**: `mvp-kafka-vertical` — async scenario-aware `Resource`, `BuiltinResource` / `RegisteredResource`, Kafka via `testcontainers-modules` (KRaft) behind `--features kafka`
- **Next**: `mvp-macros-system-test` — procedural macros for test discovery

Default `cargo test -p shakedown` does not require Docker. Integration tests that start Kafka use `cargo test -p shakedown --features kafka` (and Docker) when present under `tests/`.

## See also

Product docs (PRD, SPECS, TECH-STACK) live in the adopted documentation root — workspace [`docs/README.md`](../../docs/README.md).
