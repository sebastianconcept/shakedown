# Shakedown — Orchestration library for shakedown testing

**Shakedown** is a Rust framework for shakedown testing — proving systems are safe to
promote to production by running realistic scenarios that exercise the full stack.

## Overview

This library provides:

- **Scenario orchestration** — lifecycle phases: provision → readiness → seed → run → assert → teardown
- **Resource providers** — async [`Resource`] trait for databases, message queues, and other infrastructure
- **Error classification** — structured errors distinguishing scenario vs infrastructure failures
- **Event logging hooks** — structured event collection for failure timelines (human-readable stderr summary on infrastructure failure; pretty-printed JSON under `target/shakedown/timelines/{scenario_id}.json` for CI and tooling)

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

- **Current (shipped in repo):** `mvp-outbox` — first-party outbox helpers are available on top of relational + Kafka bindings (`ctx.outbox().setup()`, row-state assertions, payload assertions, and Kafka publish assertions).
- **Previous milestone:** `mvp-postgres-mariadb` — first-party relational resources are complete: **PostgreSQL**, **MariaDB**, **Oracle MySQL**, and **SQLite** (`PostgresResource` / `MariaDBResource` / `MysqlResource` / `SqliteResource`, matching `*Bindings` on `Context`) with suite-shared Docker containers for TCP engines and per-scenario isolation.
- **Next milestone:** `mvp-fault-injection` — Toxiproxy-backed fault scenarios on the `#[shakedown::faulty_test]` path.

Default `cargo test -p shakedown` does not require Docker.

## Common workflows with `just`

This repository includes a root [`justfile`](../../justfile) with common commands:

```text
just help
just qa
just qa-full
just test-lib
just test-macros
just test-outbox-sqlite
```

`just qa` uses `cargo nextest run --workspace` for faster feedback loops when
`cargo-nextest` is installed, and falls back to `cargo test --workspace` otherwise.
`just qa-full` always runs `cargo test --workspace` for full parity checks.

Docker-backed ignored tests are opt-in:

```text
just test-kafka-ignored
just test-postgres-ignored
just test-outbox-postgres-ignored
```

**Kafka (KRaft, `testcontainers-modules::kafka::apache`):**

- [`KafkaResource`](src/resources.rs) uses a **suite-level** broker: the first provision in a process starts one container; further `KafkaResource` instances share it until the last `Resource::teardown` removes the container (see adopted docs `SPECS-IMPLEMENTATION.md`, linked from [`feature/mvp-kafka-vertical/README.md`](../../feature/mvp-kafka-vertical/README.md)).
- **Image tag:** [`KafkaResource::with_image_tag`](src/resources.rs), or set `SHAKEDOWN_KAFKA_IMAGE_TAG` and use [`KafkaResource::with_image_tag_from_env`](src/resources.rs).
- **JVM image:** [`KafkaResource::with_jvm_image`](src/resources.rs) (`apache/kafka` instead of `apache/kafka-native`).

### KafkaBindings (US-004)

[`KafkaBindings`](src/resources.rs) exposes structured bindings on [`Context`]:

```rust
#[cfg(feature = "kafka")]
{
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Kafka(KafkaResource::new()));
    
    // After provision in run_scenario(), KafkaBindings are set automatically:
    if let Some(bindings) = ctx.kafka_bindings() {
        println!("Bootstrap: {}", bindings.bootstrap_servers);
        println!("Topic prefix: {}", bindings.topic_prefix);
        
        // Use bindings to construct an rdkafka client:
        // let client = ClientConfig::new()
        //     .set("bootstrap.servers", &bindings.bootstrap_servers)
        //     .set("client.id", format!("{}client", bindings.client_id_prefix.as_ref().unwrap_or(&"")))
        //     .create()?;
    }
}
```

Topics are created in `apply_default_seed` phase under the scenario's isolated namespace.
See [`KafkaResource::apply_default_seed`](src/resources.rs) and [DECISIONS.md D-4](../../DECISIONS.md) for details.

**Postgres (`testcontainers-modules::postgres`, `postgres:16` default tag):**

- [`PostgresResource`](src/resources.rs) uses a **suite-level** server: the first provision in a process starts one container; further instances share it until the last `Resource::teardown` drops the refcounted container. Each scenario gets a dedicated database name derived from [`sanitize_sql_database_name`](src/resources.rs).
- **Image tag:** [`PostgresResource::with_image_tag`](src/resources.rs), or set `SHAKEDOWN_POSTGRES_IMAGE_TAG` and use [`PostgresResource::with_image_tag_from_env`](src/resources.rs). Default image tag is **`16`** (non-alpine, aligned with adopted SPECS).

[`PostgresBindings`](src/resources.rs) is applied to [`Context`] after provision (same pattern as `KafkaBindings`). Default seed for Postgres is empty; scenarios own schema and data.

**MariaDB (`testcontainers-modules::mariadb`, `mariadb:11` default tag):**

- [`MariaDBResource`](src/resources.rs) mirrors Postgres: suite-level container, per-scenario `CREATE DATABASE` / `DROP DATABASE`, `mysql_async` for admin and readiness (`SELECT 1` on the scenario database). Root user matches the official image (empty password in the testcontainers module).
- **Image tag:** [`MariaDBResource::with_image_tag`](src/resources.rs), or `SHAKEDOWN_MARIADB_IMAGE_TAG` via [`MariaDBResource::with_image_tag_from_env`](src/resources.rs). Default tag is **`11`** (adopted SPECS).
- [`MariaDBBindings`](src/resources.rs) is applied after provision (`engine: mariadb` in handle metadata).

**Oracle MySQL (`testcontainers-modules::mysql`, `mysql:8.4` default tag):**

- [`MysqlResource`](src/resources.rs) is a **separate** builtin from MariaDB (R-8): suite-level container, per-scenario database, `mysql_async` for admin and readiness. Uses the official **`mysql`** Docker image, not `mariadb`.
- **Image tag:** [`MysqlResource::with_image_tag`](src/resources.rs), or `SHAKEDOWN_MYSQL_IMAGE_TAG` via [`MysqlResource::with_image_tag_from_env`](src/resources.rs). Default tag is **`8.4`** (adopted SPECS).
- [`MysqlBindings`](src/resources.rs) is applied after provision (`engine: mysql` in handle metadata).

Integration tests that start Kafka or Postgres are `#[ignore]` unless Docker is available:

```text
cargo test -p shakedown --features kafka --test macro_kafka -- --ignored
cargo test -p shakedown --features postgres --test postgres_vertical -- --ignored
cargo test -p shakedown --features kafka --test diagnostics_integration -- --ignored
```

Verification steps: adopted docs `VERIFY.md` (see [`feature/mvp-kafka-vertical/README.md`](../../feature/mvp-kafka-vertical/README.md) for the path under `~/docs/shakedown/`).

## Infrastructure diagnostics (bollard)

When you enable any **Docker-backed** resource feature (`kafka`, `postgres`, `mariadb`, `mysql`), the crate also enables the **`diagnostics`** feature unless you opt out with **`no-diagnostics`**.

What it provides (read-only — never starts or stops containers; testcontainers owns lifecycle):

- **Container log streaming** — stdout/stderr lines are recorded as [`InfraEvent::ContainerLog`](src/event.rs) entries in the same [`EventLog`](src/event.rs) used for failure timelines.
- **Docker Engine events** — filtered to containers labeled `io.shakedown.framework=true` (applied by Shakedown resources), including `stop`, non-zero `die`, OOM, health transitions, and `restart`.
- **Resource stats** — periodic CPU/memory/network samples in bounded per-container ring buffers, merged into the event log after the scenario; use [`Context::assert_resource`](src/lifecycle.rs) for `peak_memory_under` / `avg_cpu_under` when diagnostics are active.

**Cargo features:**

| Feature | Effect |
|--------|--------|
| `diagnostics` | Implied by Docker-backed resource features; compile in bollard and background monitors. |
| `no-diagnostics` | Compile out bollard-backed diagnostics; use when you cannot depend on the Docker socket API. |

**Resource assertion example** (requires Docker-backed resource + diagnostics, not `no-diagnostics`):

```rust,ignore
async fn scenario(ctx: &mut Context) -> Result<(), shakedown::ShakedownError> {
    let container_name = "my-labeled-container"; // Docker name, as in `docker ps`
    ctx.assert_resource(container_name)
        .peak_memory_under(256 * 1024 * 1024)?;
    ctx.assert_resource(container_name).avg_cpu_under(80.0)?;
    Ok(())
}
```

If the Docker socket is missing or unusable, [`try_connect`](src/diagnostics/connection.rs) returns `None`, diagnostics are skipped for that run, and scenarios **do not fail** because of observability alone.

**Stats tuning** (optional): `SHAKEDOWN_STATS_INTERVAL_MS` (default `1000`) and `SHAKEDOWN_STATS_BUFFER_DEPTH` (default `60`, capped) control how often samples are taken and how many per container are retained for timelines and [`Context::assert_resource`](src/lifecycle.rs).

**Integration tests** (`cargo test -p shakedown --features kafka`):

- [`tests/diagnostics_graceful.rs`](tests/diagnostics_graceful.rs) — `FakeResource` only, invalid `DOCKER_HOST` pointing at a non-existent Unix socket; asserts the scenario succeeds and no infrastructure rows are recorded (no Docker daemon required).
- [`tests/diagnostics_integration.rs`](tests/diagnostics_integration.rs) — log streaming, Docker `stop` events, and stats rings against a labeled Alpine container; run ignored tests with `--ignored` when Docker is available (see commands above).

## Outbox helpers

**Publication errors:** Kafka-specific `OutboxKafkaRecordNotFound` was replaced by transport-neutral
[`OutboxPublicationNotFound`](crate::error::ShakedownError::OutboxPublicationNotFound) and
[`OutboxPublicationTimeout`](crate::error::ShakedownError::OutboxPublicationTimeout) (see `Display`
for a `kafka` / `rabbitmq` prefix). Match on those variants or on
[`ShakedownError`](crate::error::ShakedownError) generally instead of matching on removed names.

Enable outbox assertions with relational + Kafka features:

```toml
[dependencies]
shakedown = { version = "...", features = ["outbox", "postgres", "kafka"] }
```

Minimal scenario template:

```rust
#[shakedown::default_seed(resources = [Postgres, Kafka])]
async fn seed(ctx: &mut Context) -> Result<(), ShakedownError> {
    ctx.outbox().setup().await?;
    ctx.kafka().ensure_topics(&["orders"]).await?;
    Ok(())
}

#[shakedown::test(resources = [Postgres, Kafka])]
async fn outbox_order_published(ctx: &mut Context) -> Result<(), ShakedownError> {
    // Trigger your service path that writes to outbox_event and publishes to Kafka.

    ctx.outbox()
        .assert_sent("order-42", "OrderPlaced")
        .await?
        .with_payload::<serde_json::Value>(|payload| {
            payload["total"].as_f64().is_some_and(|total| total == 99.0)
        })
        .await?;

    ctx.outbox()
        .assert_kafka_published("orders", "order-42")
        .await?;

    Ok(())
}
```

For fast offline checks, `features = ["outbox", "sqlite"]` supports row-state assertions in the
scenario-isolated in-memory SQLite database (`assert_pending`, `assert_sent`, `assert_failed`,
`assert_retry`).

SQLite limitation: `SELECT FOR UPDATE SKIP LOCKED` is unavailable, so claim-locking/publisher
concurrency semantics are not validated on the SQLite path. Use Postgres/MariaDB/MySQL paths when
you need lock semantics coverage.

## See also

Product docs (PRD, SPECS, TECH-STACK) live in the adopted documentation root — workspace [`docs/README.md`](../../docs/README.md) points at **`~/docs/shakedown/`**.
