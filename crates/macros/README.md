# Shakedown Macros

Procedural attribute macros for Shakedown. They expand to `#[::tokio::test]` wrappers that call [`run_scenario`](https://docs.rs/shakedown/latest/shakedown/fn.run_scenario.html) on the `shakedown` crate, so you get the same lifecycle as manual harness code without writing the boilerplate.

## Quick start

Add `shakedown` and `tokio` (with test macros) to your crate’s **dev-dependencies**. The generated `#[::tokio::test]` attribute resolves `tokio` from **your** crate, not from `shakedown` alone:

```toml
[dev-dependencies]
shakedown = { version = "0.1", features = ["sqlite"] }
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
    // Scenario body: exercise resources and assert on ctx / lifecycle
    assert!(!ctx.scenario_id().to_string().is_empty());
}
```

The macro expands to an outer `#[::tokio::test]` async function that calls [`run_scenario`](https://docs.rs/shakedown/latest/shakedown/fn.run_scenario.html):

- Creates `Context::new()` and `EventLog::new()`
- Registers builtin resources when given `resources = [...]`
- Calls `run_scenario(&mut ctx, scenario_id, |ctx| Box::pin(async move { ... }), &mut event_log).await`
- Panics with a clear error if `run_scenario` returns `Err`

### `#[shakedown::faulty_test]`

Same as `#[shakedown::test]`, but marks the scenario as faulty so fault injection (Toxiproxy) can apply when you register it:

```rust,ignore
use shakedown::Context;

#[shakedown::faulty_test]
async fn fault_injected_scenario(ctx: &mut Context) {
    // Scenario body here
}
```

The generated code calls `ctx.set_scenario_kind(ScenarioKind::Faulty)` before running the scenario.

**`fault_profile = "..."`:** expanded to `ctx.set_fault_profile("...")` immediately after `set_scenario_kind(Faulty)` (before `run_scenario`). Use it to tag a scenario for harness presets or your own assertions via [`Context::fault_profile`](https://docs.rs/shakedown/latest/shakedown/struct.Context.html#method.fault_profile).

**Other `key = <expr>` pairs:** reserved for future macro wiring (same parse shape as today). They are accepted and **ignored** so call sites stay forward-compatible. Toxic specs and fault control during the test remain on [`Context`] / `FaultHandle` APIs.

**`resources = [...]`:** same as [`test`](#optional-resources-argument): compile-time builtin registration.

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

**Builtin identifiers** (each name must match a `shakedown` Cargo feature unless `Fake`):

| `resources = [...]` name | `shakedown` feature |
|--------------------------|---------------------|
| `Fake` | always available |
| `Kafka` | `kafka` |
| `KafkaCluster` | `kafka-cluster` |
| `Rabbit` | `rabbitmq` |
| `RabbitCluster` | `rabbitmq-cluster` |
| `Postgres` | `postgres` |
| `PostgresReplicaSet` | `postgres-cluster` |
| `MariaDB` | `mariadb` |
| `Mysql` | `mysql` |
| `Sqlite` | `sqlite` |
| `DuckDb` | `duckdb` |
| `Redis` | `redis` |
| `RedisSentinel` | `redis-sentinel` |
| `Valkey` | `valkey` |
| `Mongo` | `mongodb` |
| `MongoReplicaSet` | `mongodb-cluster` |
| `Nats` | `nats` |
| `Neo4j` | `neo4j` |
| `Elasticsearch` | `elasticsearch` |
| `OpenSearch` | `opensearch` |
| `Qdrant` | `qdrant` |
| `Weaviate` | `weaviate` |
| `InfluxDb` | `influxdb` |
| `Timescaledb` | `timescaledb` |
| `Toxiproxy` | `fault-injection` |

Example:

```rust,ignore
#[shakedown::test(resources = [Elasticsearch, Postgres])]
async fn search_after_write(ctx: &mut Context) { /* ... */ }
```

Compile-time UI coverage for these attributes lives in [`crates/macros/tests/trybuild/`](https://github.com/sebastianconcept/shakedown/tree/master/crates/macros/tests/trybuild) (pass + `compile_fail` cases with `.stderr` snapshots). That tree is not shipped in the crates.io tarball.

## Runtime integration tests

End-to-end runs (macro expansion → `tokio::test` → `run_scenario`) live in the `shakedown` crate:

- [`macro_integration.rs`](https://github.com/sebastianconcept/shakedown/blob/master/crates/lib/tests/macro_integration.rs): `FakeResource` only, no Docker. Tests use `ignore = false` so they run in a normal `cargo test -p shakedown` pass (non-empty `resources` otherwise emits `#[ignore]` by default).
- [`macro_kafka.rs`](https://github.com/sebastianconcept/shakedown/blob/master/crates/lib/tests/macro_kafka.rs): `#[shakedown::test(resources = [Kafka])]`. The `macro_kafka` integration target uses `required-features = ["kafka"]` in `crates/lib/Cargo.toml`, so it is omitted from default `cargo test --workspace` builds. With `--features kafka`, the test is still **`#[ignore]`** until you opt in (requires Docker):

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
#[shakedown::test(resources = [Fake], ignore = false)]
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

From the repo root: `cargo test -p shakedown-macros` (unit tests + trybuild UI tests), or `just test-macros`. Full workspace and Docker-backed `shakedown` integration tests are documented in [Operator docs — Running tests](https://github.com/sebastianconcept/shakedown/blob/master/crates/lib/OPERATOR.md#running-tests).

## `#[shakedown::register_resource]`

Reserved for a future release and **not part of the 0.1 contract**. Today it is a pass-through
(no expansion). Do not build production tests that depend on it doing anything beyond leaving
the annotated item unchanged.

## Implementation Notes

- Macros are defined in `crates/macros/src/lib.rs` and re-exported by `shakedown` lib crate.
- The macro emits code that calls only public APIs from `::shakedown::*`.
- `shakedown-macros` does NOT depend on `shakedown` (no circular dependency).
- Users need `tokio = { version = "1", features = ["macros", "rt-multi-thread"] }` in their dev-dependencies for `#[tokio::test]` resolution.

## Related docs

- **Crate overview and resource details:** [Operator README](https://github.com/sebastianconcept/shakedown/blob/master/crates/lib/OPERATOR.md), including [Extra infrastructure features](https://github.com/sebastianconcept/shakedown/blob/master/crates/lib/OPERATOR.md#extra-infrastructure-features) (`redis` … `timescaledb`) and [Typed helpers](https://github.com/sebastianconcept/shakedown/blob/master/crates/lib/OPERATOR.md#typed-helpers).
- **Library API:** [docs.rs/shakedown](https://docs.rs/shakedown/latest/shakedown/)
