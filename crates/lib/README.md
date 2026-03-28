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

- **Current (shipped in repo):** `mvp-outbox` plus **publication verification** — row-level `outbox_event` assertions plus **transport-neutral** [`PublicationVerifier`](src/publication/verifier.rs): first-party **Kafka** and **RabbitMQ** publication proof (`assert_kafka_published`, `assert_rabbit_published`) on top of relational bindings; custom buses via the same trait (see [Outbox helpers](#outbox-helpers) below).
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

The outbox API has **two layers**. You can use row assertions **without** any message bus; publication proof is **optional** and gated on **`kafka`** and/or **`rabbitmq`**.

1. **Row assertions** — SQL against the canonical `outbox_event` table (`assert_pending`, `assert_sent`, `assert_failed`, `assert_retry`, payload checks). These require `outbox` plus a **relational** feature (`postgres`, `mariadb`, `mysql`, or `sqlite`). They do **not** require Kafka or RabbitMQ.
2. **Publication proof** — observe a message for your aggregate on a bus via [`PublicationVerifier`](src/publication/verifier.rs). First-party implementations: **Kafka** ([`KafkaPublicationVerifier`](src/publication/kafka_verifier.rs)) and **RabbitMQ** ([`RabbitMqPublicationVerifier`](src/publication/rabbit_verifier.rs)), exposed as [`Outbox::assert_kafka_published`](src/outbox.rs) and [`Outbox::assert_rabbit_published`](src/outbox.rs). Enable the matching Cargo feature; `outbox` alone does **not** pull in either transport.

**Publication errors** are transport-neutral: [`OutboxPublicationNotFound`](crate::error::ShakedownError::OutboxPublicationNotFound) and [`OutboxPublicationTimeout`](crate::error::ShakedownError::OutboxPublicationTimeout) carry a `transport` field (`"kafka"`, `"rabbitmq"`, …) and are distinct from row errors such as `OutboxRowNotFound`. Prefer matching on these variants (or [`ShakedownError`](crate::error::ShakedownError) generally) for publication failures.

### Supported combinations (matrix)

| What you need | Cargo features | Typical CI / local command (Docker where marked) |
|---------------|----------------|---------------------------------------------------|
| Relational outbox **rows only** (fast, no Docker) | `outbox`, `sqlite` | `cargo test -p shakedown --features outbox,sqlite` |
| Postgres outbox + **Kafka** publication proof | `outbox`, `postgres`, `kafka` | `cargo test -p shakedown --features outbox,postgres,kafka --test outbox_postgres -- --ignored` |
| Postgres outbox + **RabbitMQ** publication proof (no Kafka) | `outbox`, `postgres`, `rabbitmq` | `cargo test -p shakedown --features outbox,postgres,rabbitmq --test outbox_rabbit -- --ignored` |
| Rabbit verifier **unit tests** without Docker DB | `outbox`, `sqlite`, `rabbitmq` | `cargo test -p shakedown --features outbox,rabbitmq,sqlite` |
| **Custom bus** (SQS, NATS, …) | `outbox` (+ your client crates) | Implement [`PublicationVerifier`](#implementing-publicationverifier) in your crate or test module; no first-party feature required |

Full quality gates for this crate often include: `cargo check --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`, `cargo test -p shakedown --features outbox,postgres,kafka`, and `cargo test -p shakedown --features outbox,postgres,rabbitmq -- --ignored`.

### Feature flags (examples)

**Postgres + Kafka** — row assertions and Kafka publication proof:

```toml
[dependencies]
shakedown = { version = "...", features = ["outbox", "postgres", "kafka"] }
```

**Postgres + RabbitMQ** — same row APIs; use `assert_rabbit_published` instead of Kafka (declare/bind queue in seed per Option A topology — exchange + queue + routing key; see `SPECS.md` under `~/docs/shakedown/feature/publication-verifier-rabbitmq/`):

```toml
[dependencies]
shakedown = { version = "...", features = ["outbox", "postgres", "rabbitmq"] }
```

**SQLite** — row assertions only in-process (no Docker):

```toml
[dependencies]
shakedown = { version = "...", features = ["outbox", "sqlite"] }
```

### Example: Postgres + Kafka

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

### Example: Postgres + RabbitMQ

Use `#[shakedown::test(resources = [Postgres, Rabbit])]`, declare a direct or topic exchange and a durable queue in seed, bind with routing key equal to your aggregate id, publish from the test, then assert:

```rust
// After outbox row assertions, publication proof consumes from the namespaced queue:
ctx.outbox()
    .assert_rabbit_published("orders_ex", "proof_q", "order-42")
    .await?;
```

See [`tests/outbox_rabbit.rs`](tests/outbox_rabbit.rs) for a full Docker-backed flow.

### Implementing `PublicationVerifier`

Use this when your team’s bus is not Kafka or RabbitMQ (e.g. **AWS SQS** is not implemented first-party, but the trait is shaped so you can add an impl without a breaking API change).

1. **Trait** — Implement [`PublicationVerifier`](src/publication/verifier.rs) for your type with an associated `Options` struct (exchange, queue URL, region, etc.).
2. **`assert_observed`** — Take [`Context`](src/lifecycle.rs), a [`PublicationObserveSpec`](src/publication/spec.rs) (`aggregate_id`, `logical_channel`, optional `timeout`), and `&Self::Options`. Poll or consume until [`message_matches_aggregate_id`](src/publication/matching.rs) succeeds or the resolved deadline is exceeded ([`DEFAULT_PUBLICATION_OBSERVE_TIMEOUT`](src/publication/spec.rs): 20s when `timeout` is `None`).
3. **Bindings** — Read connection details from [`Context::kafka_bindings()`](src/resources.rs), [`Context::rabbit_bindings()`](src/resources.rs), or fields you attach via your own resources; keep scenario isolation consistent with how your publisher names topics/queues.
4. **Errors** — Return [`OutboxPublicationNotFound`](crate::error::ShakedownError::OutboxPublicationNotFound) or [`OutboxPublicationTimeout`](crate::error::ShakedownError::OutboxPublicationTimeout) with `transport` set to a stable label (e.g. `"sqs"`) so failure timelines classify publication failures correctly.

To use your verifier from tests, call `PublicationVerifier::assert_observed(&your_verifier, &ctx, &spec, &options)` (or wrap it in a small helper on `Outbox` in your codebase).

### SQLite limitations

`SELECT FOR UPDATE SKIP LOCKED` is unavailable on SQLite, so claim-locking / publisher concurrency semantics are not validated on that path. Use Postgres/MariaDB/MySQL when you need lock semantics coverage.

## See also

- **Adopted product docs** (PRD, SPECS, decisions) — workspace [`docs/README.md`](../../docs/README.md) points at **`~/docs/shakedown/`**.
- **Pluggable outbox + queue** — design background: **`~/docs/shakedown/ideation/scoping-brief-pluggable-outbox-queue.md`**
- **Verifier naming decision** — *Verifier traits for custom resource assertions* in **`~/docs/shakedown/DECISIONS.md`**
