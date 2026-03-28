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
- **Also shipped:** `mvp-fault-injection` — suite-level **Toxiproxy** as a [`Resource`](src/resources.rs), [`FaultHandle`](src/fault_injection.rs) on [`Context`](src/lifecycle.rs), and **Kafka** client bootstrap via the proxy for `ScenarioKind::Faulty` when both **Kafka** and **Toxiproxy** are registered (see [Fault injection](#fault-injection-toxiproxy-and-the-kafka-path)).
- **Previous milestone:** `mvp-postgres-mariadb` — first-party relational resources are complete: **PostgreSQL**, **MariaDB**, **Oracle MySQL**, and **SQLite** (`PostgresResource` / `MariaDBResource` / `MysqlResource` / `SqliteResource`, matching `*Bindings` on `Context`) with suite-shared Docker containers for TCP engines and per-scenario isolation.
- **Next milestone (roadmap):** `mvp-bridges` — minimal HTTP/gRPC bridge story; see the MVP table in adopted docs (`MVP.md` under `~/docs/shakedown/`).

Default `cargo test -p shakedown` does not require Docker.

## Running tests

Use [`just help`](../../justfile) to list recipes from the repo root [`justfile`](../../justfile). Below, **cargo** commands are shown explicitly so CI and copy-paste work without `just`.

### Workspace (all crates)

| Command | What it runs | Docker |
|---------|----------------|--------|
| `cargo test --workspace` | `shakedown`, `shakedown-macros` (incl. trybuild), CLI; **default features** per crate | Only where tests are not `#[ignore]` |
| `just test` | Same targets as `cargo test --workspace`, but uses **`cargo nextest run`** when `cargo-nextest` is installed (faster); falls back to plain Cargo otherwise | Same |
| `just test-workspace` | Same as `cargo test --workspace` (always plain Cargo; use when you want parity with CI that does not use nextest) | Same |
| `cargo test --workspace --all-features` | Same targets with **every** optional feature enabled | Ignored Docker tests are still **skipped** unless you pass `-- --include-ignored` |
| `just test-workspace-all-features` | Same as `cargo test --workspace --all-features` | Same |

`cargo test --workspace --all-features` enables `shakedown`’s `no-diagnostics` feature, which **excludes** the **bollard** diagnostics integration tests from compilation. To exercise those, use a Docker-backed feature set **without** `no-diagnostics` (see below).

### CI recommendations

Align automation with the same commands as in this section:

1. **Fast gate (no Docker):** `cargo test --workspace` (or `just test-workspace` / `just qa-full` for Cargo-only parity).
2. **Optional compile-all:** `cargo test --workspace --all-features` — remember this enables `no-diagnostics`, so it does **not** compile bollard diagnostics tests.
3. **Docker runner:** `just test-shakedown-all-include-ignored` (or the equivalent `cargo test -p shakedown --features "kafka,postgres,mariadb,mysql,outbox,rabbitmq,sqlite" -- --include-ignored`) with a working Docker socket — covers ignored integration tests and diagnostics. Uses **nextest** when installed, same as `just test`.
4. **Macros only:** `cargo test -p shakedown-macros` when you need trybuild without rebuilding the full workspace.
5. **CLI:** `cargo test -p shakedown-cli` (or `just test-cli`) — currently the binary crate has no test targets; the recipe is reserved for future tests.

### `shakedown` library crate

| Command | What it runs | Docker |
|---------|----------------|--------|
| `cargo test -p shakedown` | Unit + integration tests for default (minimal) features | Not required for the default set |
| `cargo test -p shakedown --features outbox,sqlite` | Outbox helpers + SQLite vertical tests | Not required |
| `cargo test -p shakedown --features outbox,rabbitmq,sqlite` | Adds Rabbit publication helpers’ unit coverage | Not required for SQLite-only tests |
| `cargo test -p shakedown --features "kafka,postgres,mariadb,mysql,outbox,rabbitmq,sqlite" -- --include-ignored` | **All** tests: normal + **`#[ignore]`** + doc tests; compiles diagnostics (bollard) | **Required** for ignored integration tests |
| `just test-lib` | `cargo test -p shakedown` | Same as column above |
| `just test-shakedown-all-include-ignored` | Same as the long `--include-ignored` row; uses **nextest** when installed, else `cargo test` | **Required** for ignored tests |

`--include-ignored` runs both non-ignored and ignored tests in one pass. To run **only** ignored tests, use `-- --ignored` on a matching feature set (see per-target recipes below).

### `shakedown-macros` (incl. trybuild)

| Command | What it runs | Docker |
|---------|----------------|--------|
| `cargo test -p shakedown-macros` | Macro unit tests + **trybuild** compile-pass / compile-fail UI tests | No |
| `just test-macros` | Same | No |

### `shakedown-cli` binary crate

| Command | What it runs | Docker |
|---------|----------------|--------|
| `cargo test -p shakedown-cli` | Tests for the CLI package (none yet; target exists for future `[[test]]` binaries) | No |
| `just test-cli` | Same | No |

### Docker-backed `#[ignore]` tests (one binary each)

These need a running Docker engine. They are **skipped** in a normal `cargo test` unless you pass `-- --ignored` or use `--include-ignored` with a feature set that builds the test binary.

| Integration test | Cargo command | `just` recipe |
|------------------|---------------|----------------|
| Kafka macro provisioning | `cargo test -p shakedown --features kafka --test macro_kafka -- --ignored` | `just test-kafka-ignored` |
| Postgres vertical | `cargo test -p shakedown --features postgres --test postgres_vertical -- --ignored` | `just test-postgres-ignored` |
| MariaDB vertical | `cargo test -p shakedown --features mariadb --test mariadb_vertical -- --ignored` | `just test-mariadb-ignored` |
| MySQL vertical | `cargo test -p shakedown --features mysql --test mysql_vertical -- --ignored` | `just test-mysql-ignored` |
| Outbox + Postgres + Kafka | `cargo test -p shakedown --features outbox,postgres,kafka --test outbox_postgres -- --ignored` | `just test-outbox-postgres-ignored` |
| Outbox + Postgres + RabbitMQ | `cargo test -p shakedown --features outbox,postgres,rabbitmq --test outbox_rabbit -- --ignored` | `just test-outbox-rabbit-ignored` |
| Diagnostics (bollard: logs, events, stats) | `cargo test -p shakedown --features kafka --test diagnostics_integration -- --ignored` | `just test-diagnostics-integration-ignored` |
| Toxiproxy vertical (control API) | `cargo test -p shakedown --features fault-injection --test toxiproxy_vertical -- --ignored` | `just test-toxiproxy-ignored` |
| Kafka + Toxiproxy (Faulty bootstrap / proxy) | `cargo test -p shakedown --features kafka,fault-injection --test fault_injection_kafka -- --ignored` | `just test-fault-injection-kafka-ignored` |

**Note:** `tests/diagnostics_integration.rs` also has a **non-ignored** test (`try_connect_graceful_when_docker_unreachable`); it runs with `cargo test -p shakedown --features kafka` without `--ignored`.

### Quality / formatting (not tests, but common before merge)

| Command | Purpose |
|---------|---------|
| `just qa` | `fmt-check`, `check`, `clippy`, then workspace tests (uses **nextest** if installed) |
| `just qa-full` | Same checks, then `cargo test --workspace` (no nextest) |
| `just fmt` / `just fmt-check` / `just check` / `just clippy` | See [`justfile`](../../justfile) |

## Common workflows with `just`

This repository includes a root [`justfile`](../../justfile). Quick entry points:

```text
just help
just qa
just qa-full
just test
just test-workspace
just test-workspace-all-features
just test-lib
just test-macros
just test-cli
just test-shakedown-all-include-ignored
just test-outbox-sqlite
just test-toxiproxy-ignored
just test-fault-injection-kafka-ignored
```

`just test` and `just qa` use `cargo nextest run` when `cargo-nextest` is installed, and fall back to `cargo test` otherwise. `just test-workspace` and `just qa-full` always use plain `cargo test --workspace` for parity with Cargo-only CI. `just test-shakedown-all-include-ignored` uses nextest the same way when available.

Docker-backed tests are opt-in; see [Running tests](#running-tests) for the full matrix and individual `just test-*-ignored` recipes.

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

### Fault injection (Toxiproxy) and the Kafka path

Shakedown injects **TCP-level** faults on traffic that flows through **[Toxiproxy](https://github.com/Shopify/toxiproxy)** (latency, bandwidth, timeout toxics, and so on). This is **not** a general chaos layer: it does **not** simulate arbitrary failure modes (for example process crashes, disk exhaustion, or cloud-AZ loss). It answers “what happens when the **network path to a dependency** is degraded?” — aligned with *trusting the result* (see **MISSION.md** in adopted docs, mode 3): failures are classifiable as harness-level TCP faults vs application bugs.

**Cargo feature:** enable the capability with **`fault-injection`** (kebab-case). It pulls in the Toxiproxy control-plane client and suite-level container wiring; it does **not** enable Kafka by itself — compose with **`kafka`** for the Kafka vertical:

```toml
[dev-dependencies]
shakedown = { path = "../..", features = ["kafka", "fault-injection"] }
```

**Registration:** register both **Kafka** and **Toxiproxy** builtins (order: Kafka first, then Toxiproxy matches the harness proxy hook). From a test you can use the macro:

```rust,ignore
#[shakedown::faulty_test(resources = [Kafka, Toxiproxy])]
async fn kafka_fault_example(ctx: &mut Context) -> Result<(), shakedown::ShakedownError> {
    // KafkaBindings already point at the Toxiproxy listen address for Faulty + both resources.
    let _bootstrap = &ctx.kafka_bindings().expect("kafka after provision").bootstrap_servers;

    let faults = ctx
        .fault_handle_mut()
        .expect("enable `fault-injection` and register Toxiproxy; see FaultInjectionError::NotAvailable");

    faults
        .add_toxic(
            "shakedown_kafka",
            &shakedown::Toxic {
                kind: shakedown::ToxicKind::Latency {
                    latency: 250,
                    jitter: 0,
                },
                name: "slow_path".to_string(),
                toxicity: 1.0,
                direction: shakedown::StreamDirection::Downstream,
            },
        )
        .await?;

    // Exercise your producer/consumer using the same bootstrap string (proxied path).

    faults.reset_proxy("shakedown_kafka").await?;
    Ok(())
}
```

Use [`run_scenario`](src/lifecycle.rs) with [`ScenarioKind::Faulty`](src/lifecycle.rs) (via [`Context::set_scenario_kind`](src/lifecycle.rs)) and register [`KafkaResource`](src/resources.rs) / [`ToxiproxyResource`](src/resources.rs) manually if you are not using the macro — see [`tests/fault_injection_kafka.rs`](tests/fault_injection_kafka.rs).

**`KafkaBindings` and host:port:** after provision, [`KafkaBindings::bootstrap_servers`](src/resources.rs) is the **single client-facing** address your code should use (`bootstrap.servers` for rdkafka, etc.). For **`ScenarioKind::Faulty`** with **`fault-injection`** enabled and **both** resources registered, the harness rewrites that field to the **Toxiproxy listen** address (pass-through proxy to the real broker) so toxics apply transparently. For **`ScenarioKind::Normal`**, or if Toxiproxy is not registered, **`bootstrap_servers` stays the direct broker** endpoint. Scenarios do not need separate “proxy vs direct” branches for binding selection — only for **when** you call [`FaultHandle`](src/fault_injection.rs) to add or remove toxics.

**CI:** Docker-backed proof tests are `#[ignore]`; run them explicitly, e.g. `cargo test -p shakedown --features kafka,fault-injection --test fault_injection_kafka -- --ignored` or `just test-fault-injection-kafka-ignored`.

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

Integration tests that start containers (Kafka, Postgres, MariaDB, MySQL, outbox flows, bollard diagnostics) are usually `#[ignore]`; run them with Docker available. Commands and `just` recipes are listed in [Running tests](#running-tests) (Docker-backed `#[ignore]` tests).

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

**Integration tests** (`cargo test -p shakedown --features kafka`; see [Running tests](#running-tests) for `just` recipes):

- [`tests/diagnostics_graceful.rs`](tests/diagnostics_graceful.rs) — `FakeResource` only, invalid `DOCKER_HOST` pointing at a non-existent Unix socket; asserts the scenario succeeds and no infrastructure rows are recorded (no Docker daemon required).
- [`tests/diagnostics_integration.rs`](tests/diagnostics_integration.rs) — log streaming, Docker `stop` events, and stats rings against a labeled Alpine container; Docker-backed cases use `just test-diagnostics-integration-ignored` or the matching `cargo` line in [Running tests](#running-tests).

## Outbox helpers

The outbox API has **two layers**. You can use row assertions **without** any message bus; publication proof is **optional** and gated on **`kafka`** and/or **`rabbitmq`**.

1. **Row assertions** — SQL against the canonical `outbox_event` table (`assert_pending`, `assert_sent`, `assert_failed`, `assert_retry`, payload checks). These require `outbox` plus a **relational** feature (`postgres`, `mariadb`, `mysql`, or `sqlite`). They do **not** require Kafka or RabbitMQ.
2. **Publication proof** — observe a message for your aggregate on a bus via [`PublicationVerifier`](src/publication/verifier.rs). First-party implementations: **Kafka** ([`KafkaPublicationVerifier`](src/publication/kafka_verifier.rs)) and **RabbitMQ** ([`RabbitMqPublicationVerifier`](src/publication/rabbit_verifier.rs)), exposed as [`Outbox::assert_kafka_published`](src/outbox.rs) and [`Outbox::assert_rabbit_published`](src/outbox.rs). Enable the matching Cargo feature; `outbox` alone does **not** pull in either transport.

**Publication errors** are transport-neutral and distinct from row errors such as `OutboxRowNotFound`. **First-party Kafka and Rabbit** verifiers return [`OutboxPublicationNotFound`](crate::error::ShakedownError::OutboxPublicationNotFound) when the observation window expires without a matching message (`transport`: `"kafka"` or `"rabbitmq"`). [`OutboxPublicationTimeout`](crate::error::ShakedownError::OutboxPublicationTimeout) is available for **custom** [`PublicationVerifier`](#implementing-publicationverifier) implementations that need a separate “time-bounded infrastructure failure” classification (first-party code does not emit it today). Both variants carry `transport`, `channel`, and `aggregate_id`; prefer matching on these variants (or [`ShakedownError`](crate::error::ShakedownError) generally) for publication failures.

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
4. **Errors** — When the resolved deadline passes without a matching message, return [`OutboxPublicationNotFound`](crate::error::ShakedownError::OutboxPublicationNotFound) with `transport` set to a stable label (e.g. `"sqs"`), matching first-party Kafka/Rabbit. Optionally return [`OutboxPublicationTimeout`](crate::error::ShakedownError::OutboxPublicationTimeout) if your verifier distinguishes an infrastructure/connectivity time limit from “no matching publication observed.”

To use your verifier from tests, call `PublicationVerifier::assert_observed(&your_verifier, &ctx, &spec, &options)` (or wrap it in a small helper on `Outbox` in your codebase).

### SQLite limitations

`SELECT FOR UPDATE SKIP LOCKED` is unavailable on SQLite, so claim-locking / publisher concurrency semantics are not validated on that path. Use Postgres/MariaDB/MySQL when you need lock semantics coverage.

## See also

- **Adopted product docs** (PRD, SPECS, decisions) — workspace [`docs/README.md`](../../docs/README.md) points at **`~/docs/shakedown/`**.
- **Pluggable outbox + queue** — design background: **`~/docs/shakedown/ideation/scoping-brief-pluggable-outbox-queue.md`**
- **Verifier naming decision** — *Verifier traits for custom resource assertions* in **`~/docs/shakedown/DECISIONS.md`**
