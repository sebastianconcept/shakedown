# Shakedown Macros

Procedural attribute macros for shakedown testing.

## Available Attributes

### `#[shakedown::test]`

Wraps an async test function with the full scenario lifecycle:

```rust,ignore
use shakedown::Context;

#[shakedown::test]
async fn order_created_event(ctx: &mut Context) {
    // Scenario body here
    let event = ctx.event_queue().consume_one().await;
}
```

The macro generates a `#[tokio::test]` wrapper that calls [`run_scenario`](../lib/src/lifecycle.rs):

- Creates `Context::new()` and `EventLog::new()`
- Registers resources (when specified via attribute argument)
- Calls `run_scenario(&mut ctx, scenario_id, |ctx, _event_log| { ... }, event_log)`
- Panics with a clear error if `run_scenario` returns `Err`

### `#[shakedown::faulty_test]`

Same as `#[shakedown::test]` but marks the scenario as faulty for future fault injection:

```rust,ignore
use shakedown::Context;

#[shakedown::faulty_test]
async fn fault_injected_scenario(ctx: &mut Context) {
    // Scenario body here
}
```

The generated code calls `ctx.set_scenario_kind(ScenarioKind::Faulty)` before running the scenario.

### Optional Attributes Argument (US-003 - Deferred)

Future iteration will support resource registration via attribute:

```rust,ignore
#[shakedown::test(resources = [Fake])]  // Registers FakeResource automatically
async fn test(ctx: &mut Context) { ... }
```

## Usage Examples

### Basic test

```rust,ignore
use shakedown::Context;

#[shakedown::test]
async fn basic_test(ctx: &mut Context) {
    let scenario_id = ctx.scenario_id().to_string();
    assert!(!scenario_id.is_empty());
}
```

### Test with resources (deferred - US-003)

```rust,ignore
#[shakedown::test(resources = [Fake])]  // Auto-registers FakeResource
async fn with_fake_resource(ctx: &mut Context) {
    let _fake_handle = ctx.resources_mut()[0].as_builtin();
}
```

### Faulty test

```rust,ignore
#[shakedown::faulty_test]
async fn faulty_scenario(ctx: &mut Context) {
    let kind = ctx.scenario_kind();
    assert_eq!(kind, ScenarioKind::Faulty);
}
```

## Implementation Notes

- Macros are defined in `crates/macros/src/lib.rs` and re-exported by `shakedown` lib crate.
- The macro emits code that calls only public APIs from `::shakedown::*`.
- `shakedown-macros` does NOT depend on `shakedown` (no circular dependency).
- Users need `tokio = { version = "1", features = ["macros", "rt-multi-thread"] }` in their dev-dependencies for `#[tokio::test]` resolution.

## Next Milestone

[`mvp-postgres-mariadb`](../lib/README.md) — Postgres and MariaDB resources.
