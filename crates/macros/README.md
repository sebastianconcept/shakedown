# Shakedown Macros

Procedural attribute macros for shakedown testing.

## Available Attributes

### `#[shakedown::test]`

Wraps an async test function with the full scenario lifecycle:

```rust,ignore
use shakedown::Context;

#[shakedown::test]
async fn order_created_event(ctx: &mut Context) {
    // Scenario body — e.g. exercise resources and assert on ctx / lifecycle
    assert!(!ctx.scenario_id().to_string().is_empty());
}
```

The macro expands to an outer `#[::tokio::test]` async function that calls [`run_scenario`](../lib/src/lifecycle.rs):

- Creates `Context::new()` and `EventLog::new()`
- Registers builtin resources when given `resources = [...]`
- Calls `run_scenario(&mut ctx, scenario_id, |ctx| Box::pin(async move { ... }), &mut event_log).await`
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

Additional `name = <expr>` attribute arguments are **ignored** until `mvp-fault-injection` wires fault settings; only `resources = [...]` is interpreted today.

### Optional `resources` argument

Register builtin resources on the context before the scenario runs:

```rust,ignore
#[shakedown::test(resources = [Fake])]
async fn test(ctx: &mut Context) { ... }

#[shakedown::test(resources = [Fake, Kafka])]  // needs `shakedown` with `kafka` feature
async fn with_kafka(ctx: &mut Context) { ... }
```

Compile-time UI coverage for these attributes lives in `tests/trybuild/` (pass + `compile_fail` cases with `.stderr` snapshots).

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

### Test with resources

```rust,ignore
#[shakedown::test(resources = [Fake])]
async fn with_fake_resource(ctx: &mut Context) {
    assert!(!ctx.resources().is_empty());
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
