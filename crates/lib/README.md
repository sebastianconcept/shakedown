# Shakedown — Orchestration library for shakedown testing

**Shakedown** is a Rust framework for shakedown testing — proving systems are safe to
promote to production by running realistic scenarios that exercise the full stack.

## Overview

This library provides:

- **Scenario orchestration** — lifecycle phases: provision → readiness → seed → run → assert → teardown
- **Resource providers** — traits for integrating databases, message queues, and other infrastructure
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

- **ResourceProvider** trait — implement for new infrastructure integrations (Kafka, Postgres, etc.)
- **EnvironmentBackend** trait — implement for alternative execution environments
- **register_resource!** macro — register resources declaratively

## Milestone status

- **Current**: `mvp-foundation` — orchestration skeleton, error types, extension seams
- **Next**: `mvp-kafka-vertical` — real resources (Kafka, Postgres), full API

## See also

- [Product PRD](https://github.com/your-org/shakedown/blob/main/docs/feature/mvp-foundation/PRD.md)
- [Technical SPECS](https://github.com/your-org/shakedown/blob/main/docs/feature/mvp-foundation/SPECS.md)
- [TECH-STACK.md](https://github.com/your-org/shakedown/blob/main/docs/TECH-STACK.md)

## Example

```rust
use shakedown::Context;

#[shakedown_macros::register_resource]
struct FakeResource;

impl shakedown::Resource for FakeResource {
    fn provision(_ctx: &mut Context) -> Self { FakeResource }

    fn apply_default_seed(&self) {}

    fn check_ready(&self) -> bool { true }

    fn teardown(self) {}
}

#[shakedown_macros::system_test]
async fn my_scenario(_ctx: Context) {
    // Scenario implementation
}
```
