# Shakedown Macros

Procedural attribute macros for shakedown testing. They expand to `#[::tokio::test]` wrappers that call [`run_scenario`](../lib/src/lifecycle/mod.rs) on the `shakedown` crate — same lifecycle as manual harness code.

## Quick start

Add `shakedown` and `tokio` (with test macros) to your crate’s **dev-dependencies**. The generated `#[::tokio::test]` attribute resolves `tokio` from **your** crate, not from `shakedown` alone:

```toml
[dev-dependencies]
shakedown = { version = "0.1", path = "../path/to/shakedown" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Then annotate async functions with `ctx: &mut shakedown::Context` (or `use shakedown::Context`).

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

The macro expands to an outer `#[::tokio::test]` async function that calls [`run_scenario`](../lib/src/lifecycle/mod.rs):

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

**`fault_profile = "..."`** — Recognized and expanded to `ctx.set_fault_profile("...")` immediately after `set_scenario_kind(Faulty)` (before `run_scenario`). Use it to tag a scenario for future harness presets or for your own assertions via [`Context::fault_profile`](../lib/src/lifecycle/mod.rs).

**Other `key = <expr>` pairs** — Reserved for future macro wiring (same parse shape as today). They are accepted and **ignored** so call sites can stay forward-compatible; toxic specs and runtime fault control remain on [`Context`] / `FaultHandle` APIs.

**`resources = [...]`** — Same as [`test`](#optional-resources-argument): compile-time builtin registration.

### Optional `resources` argument

Register builtin resources on the context before the scenario runs:

```rust,ignore
#[shakedown::test(resources = [Fake])]
async fn test(ctx: &mut Context) { ... }

#[shakedown::test(resources = [Fake, Kafka])]  // needs `shakedown` with `kafka` feature
async fn with_kafka(ctx: &mut Context) { ... }

#[shakedown::test(resources = [Postgres, MariaDB, Mysql, Sqlite])]
async fn with_relational_resources(ctx: &mut Context) { ... }
```

Accepted resource names today are `Fake`, `Kafka`, `Rabbit`, `Postgres`, `MariaDB`, `Mysql`, and `Sqlite` (feature-gated where applicable).

Compile-time UI coverage for these attributes lives in `tests/trybuild/` (pass + `compile_fail` cases with `.stderr` snapshots).

## Runtime integration tests

End-to-end runs (macro expansion → `tokio::test` → `run_scenario`) live in the `shakedown` crate:

- [`crates/lib/tests/macro_integration.rs`](../lib/tests/macro_integration.rs) — `FakeResource` only, no Docker.
- [`crates/lib/tests/macro_kafka.rs`](../lib/tests/macro_kafka.rs) — `#[shakedown::test(resources = [Kafka])]`. The `macro_kafka` integration target uses `required-features = ["kafka"]` in `crates/lib/Cargo.toml`, so it is omitted from default `cargo test --workspace` builds. With `--features kafka`, the test is still **`#[ignore]`** until you opt in (requires Docker):

  ```text
  cargo test -p shakedown --features kafka macro_kafka -- --ignored
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

### Test with resources

```rust,ignore
#[shakedown::test(resources = [Fake])]
async fn with_fake_resource(ctx: &mut Context) {
    assert!(!ctx.resources().is_empty());
}
```

### Faulty test

```rust,ignore
use shakedown::{Context, ScenarioKind};

#[shakedown::faulty_test]
async fn faulty_scenario(ctx: &mut Context) {
    let kind = ctx.scenario_kind();
    assert_eq!(kind, ScenarioKind::Faulty);
}
```

## Testing

From the repo root: `cargo test -p shakedown-macros` (unit tests + trybuild UI tests), or `just test-macros`. Full workspace and Docker-backed `shakedown` integration tests are documented in [`crates/lib/README.md` § Running tests](../lib/README.md#running-tests).

## Implementation Notes

- Macros are defined in `crates/macros/src/lib.rs` and re-exported by `shakedown` lib crate.
- The macro emits code that calls only public APIs from `::shakedown::*`.
- `shakedown-macros` does NOT depend on `shakedown` (no circular dependency).
- Users need `tokio = { version = "1", features = ["macros", "rt-multi-thread"] }` in their dev-dependencies for `#[tokio::test]` resolution.

## Next milestone

After macros ship, the orchestration library adds relational resources:

- **Slug:** `mvp-postgres-mariadb` — Postgres, MariaDB, Mysql, and Sqlite `Resource` providers on the same async harness as Kafka.
- **Spine:** MVP row 4 in adopted docs [`/Users/seb/docs/shakedown/MVP.md`](/Users/seb/docs/shakedown/MVP.md).
- **Crate overview:** [`crates/lib/README.md`](../lib/README.md).
