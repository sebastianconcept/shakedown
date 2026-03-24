# shakedown-macros

Procedural macros for Shakedown testing framework.

## Status: `mvp-macros-system-test` implemented

This crate provides procedural macro attributes for writing scenario tests with minimal boilerplate.

### What's implemented (US-001)

- **`#[shakedown::test]`** — Wraps an async function with full lifecycle orchestration
  - Adds `#[::tokio::test]` attribute for test discovery
  - Creates `Context` and `EventLog` internally
  - Calls `run_scenario()` with scenario ID derived from function name
  - Handles errors by panicking with clear messages

- **`#[shakedown::faulty_test]`** — Same as `#[shakedown::test]` but marks the scenario as faulty
  - Sets `ctx.set_scenario_kind(ScenarioKind::Faulty)` before running the scenario
  - Reserved for `mvp-fault-injection` to wire Toxiproxy without renaming

- **`register_resource!`** — Pass-through stub (deprecated, kept for backwards compatibility)

### Usage examples

#### Basic test

```rust,no_run
use shakedown::Context;

#[shakedown::test]
async fn order_created_event(ctx: &mut Context) {
    // Scenario body using ctx
}
```

This expands to:

```rust
#[::tokio::test]
async fn order_created_event() {
    let mut ctx = ::shakedown::Context::new();
    let mut event_log = ::shakedown::EventLog::new();

    let result = ::shakedown::run_scenario(
        &mut ctx,
        "order_created_event",
        |ctx: &mut ::shakedown::Context, _event_log: &mut ::shakedown::EventLog| async move {
            // User's scenario body
            order_created_event_body
        },
        &mut event_log,
    ).await;

    if let Err(e) = result {
        panic!("shakedown scenario failed: {}", e);
    }
}
```

#### With builtin resources

```rust,no_run
use shakedown::Context;

#[shakedown::test(resources = [Fake])]
async fn with_fake_resource(ctx: &mut Context) {
    // ...
}
```

Generates:

```rust,ignore
ctx.register_builtin(::shakedown::BuiltinResource::Fake(
    ::shakedown::FakeResource::default(),
));
```

#### Faulty test

```rust,no_run
use shakedown::Context;

#[shakedown::faulty_test]
async fn faulty_scenario(ctx: &mut Context) {
    // This scenario may have faults injected (in mvp-fault-injection)
}
```

### Validation

The macro validates:
1. Function is `async` — otherwise compile error
2. First parameter is `ctx: &mut Context` — otherwise compile error
3. Resource identifiers in `resources = [...]` are known names (`Fake`, `Kafka`)

### Re-export from `shakedown` lib

The crate `shakedown-macros` does **not** depend on `shakedown` (would be circular). Instead:
- Macros emit `::shakedown::*` paths in generated code
- The user's crate depends on both `shakedown` and `tokio` (for `#[::tokio::test]`)
- This matches the spec pattern where macros are thin callers over the lib

### See also

- [Shakedown lib README](../README.md) — project overview and milestones
- [Feature PRD](../../feature/mvp-macros-system-test/PRD.md) — user stories and acceptance criteria
- [Feature SPECS](../../feature/mvp-macros-system-test/SPECS.md) — technical specifications
- [IDEATION.md](../../IDEATION.md) — scoping brief for this milestone
