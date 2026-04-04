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
│   └── macros/       # procedural macros (shakedown-macros)
```

## Extension points

- **[`Resource`] trait** — implement for new infrastructure (with `async_trait` for `Box<dyn Resource>` extensions)
- **[`EnvironmentBackend`] trait** — implement for alternative execution environments (also `async_trait` for `dyn` use)
- **`Context::register_builtin` / `register_extension`** — wire first-party or third-party resources

## Milestone status

| MVP milestone | Status |
|---------------|--------|
| `mvp-typed-helpers` | ✅ **Complete** (2026-03-29) — typed helpers on [`Context`](src/lifecycle/mod.rs) ([`KafkaHelpers`](src/resources/kafka/helpers.rs), [`RabbitHelpers`](src/resources/rabbit/helpers.rs), relational [`PostgresHelpers`](src/resources/postgres/helpers.rs) / [`MariaDbHelpers`](src/resources/mariadb/helpers.rs) / [`MysqlHelpers`](src/resources/mysql/helpers.rs) / [`SqliteHelpers`](src/resources/sqlite/helpers.rs)); see [Typed helpers](#typed-helpers) |
| `extra-infra` | ✅ **Complete** (2026-03-31) — twelve additional first-party resources (Redis, Valkey, MongoDB, NATS, Neo4j, DuckDB, Elasticsearch, OpenSearch, Qdrant, Weaviate, InfluxDB, TimescaleDB); see [Extra infrastructure features](#extra-infrastructure-features-extra-infra) |
| `mvp-examples` | **Next** — living `crates/examples` catalog showcasing common harness patterns |

- **Current (shipped in repo):** `mvp-outbox` plus **publication verification** — row-level `outbox_event` assertions plus **transport-neutral** [`PublicationVerifier`](src/publication/verifier.rs): first-party **Kafka**, **RabbitMQ**, **NATS** (JetStream), and **Redis Streams** publication proof (`assert_kafka_published`, `assert_rabbit_published`, `assert_nats_published`, `assert_redis_streams_published`) on top of relational, document, or in-process storage bindings; custom buses via the same trait (see [Outbox helpers](#outbox-helpers) below). **Extended outbox** (2026-04): **MongoDB** and **DuckDB** storage paths match the same `Outbox` row APIs as SQL engines.
- **Also shipped:** `mock-services` — mock upstream HTTP/gRPC servers bound on localhost so scenarios assert outbound calls at the **network** layer; see [Mock services](#mock-services).
- **Also shipped:** **`services`** (service-under-test) — opt-in Cargo feature to run **application containers** via testcontainers during scenarios: [`ServiceUnderTest`](src/service/builder.rs) → [`ServiceSpec`](src/service/types.rs), convention-based **env injection** (Kafka, relational DBs, RabbitMQ, Redis/Valkey, MongoDB, Neo4j, mock services, `depends_on` peers), **convergence health checks**, [`Context::service_url`](src/lifecycle/mod.rs) / [`service_handle`](src/lifecycle/mod.rs) on the test host (`127.0.0.1`), and **bollard** diagnostics labels — see [Service under test](#service-under-test-services-feature).
- **Also shipped:** `mvp-fault-injection` — suite-level **Toxiproxy** as a [`Resource`](src/resources.rs), [`FaultHandle`](src/fault_injection.rs) on [`Context`](src/lifecycle/mod.rs), and **Kafka** client bootstrap via the proxy for `ScenarioKind::Faulty` when both **Kafka** and **Toxiproxy** are registered (see [Fault injection](#fault-injection-toxiproxy-and-the-kafka-path)).
- **Previous milestone:** `mvp-postgres-mariadb` — first-party relational resources are complete: **PostgreSQL**, **MariaDB**, **Oracle MySQL**, and **SQLite** (`PostgresResource` / `MariaDBResource` / `MysqlResource` / `SqliteResource`, matching `*Bindings` on `Context`) with suite-shared Docker containers for TCP engines and per-scenario isolation.
- **Roadmap:** Further milestones are tracked in product planning materials linked from the workspace [`docs/README.md`](../../docs/README.md).

Default `cargo test -p shakedown` does not require Docker.

## Extra infrastructure features (`extra-infra`)

The following **Cargo features** add first-party [`Resource`](src/resources.rs) implementations, [`BuiltinResource`](src/resources.rs) variants, bindings on [`Context`](src/lifecycle/mod.rs), optional [`EnvMapping`](src/service/types.rs) / service injection, and typed helpers. Each feature is **independent** unless noted (for example OpenSearch reuses internal HTTP logic with Elasticsearch but has a separate feature and types).

### Cargo features (one line each)

| Feature | What it enables |
|---------|-----------------|
| `redis` | Suite-shared **Redis** (`redis:7`), RESP client, key-prefix isolation, [`RedisHelpers`](src/resources/redis/helpers.rs) (`ctx.redis()`). |
| `valkey` | Same public types as Redis against **Valkey** (`valkey/valkey:8`); still `ctx.redis()`. |
| `mongodb` | **MongoDB** `mongod`, per-scenario database, [`MongoHelpers`](src/resources/mongo/helpers.rs). |
| `nats` | **NATS** with **JetStream** enabled in the suite container, subject-prefix isolation, [`NatsHelpers`](src/resources/nats/helpers.rs); publication proof uses JetStream streams. |
| `neo4j` | **Neo4j 5.x** Bolt (Enterprise image default for multi-database isolation), [`Neo4jHelpers`](src/resources/neo4j/helpers.rs). |
| `duckdb` | **DuckDB** in-process (no Docker), named in-memory DB per scenario, [`DuckDbHelpers`](src/resources/duckdb/helpers.rs). |
| `elasticsearch` | **Elasticsearch 8** single-node, index-prefix isolation, [`ElasticsearchHelpers`](src/resources/elasticsearch/helpers.rs); builds shared HTTP [`search_operations`](src/resources/search_operations.rs). |
| `opensearch` | **OpenSearch 2** single-node, same helper shape as ES with separate types, [`OpenSearchHelpers`](src/resources/opensearch/helpers.rs). |
| `qdrant` | **Qdrant** vector store, collection-prefix isolation, gRPC helpers [`QdrantHelpers`](src/resources/qdrant/helpers.rs). |
| `weaviate` | **Weaviate** REST API, class-prefix isolation, [`WeaviateHelpers`](src/resources/weaviate/helpers.rs). |
| `influxdb` | **InfluxDB 2.x** HTTP (line protocol + Flux), per-scenario bucket, [`InfluxDbHelpers`](src/resources/influxdb/helpers.rs). |
| `timescaledb` | **TimescaleDB** on Postgres wire protocol, [`PostgresBindings`](src/resources.rs)-shaped URLs, [`TimescaledbHelpers`](src/resources/timescaledb/helpers.rs) (includes `assert_hypertable_exists`). |

### Docker vs in-process

| Engines | Runtime |
|---------|---------|
| **Docker** (testcontainers, one suite-shared container per engine where applicable) | `redis`, `valkey`, `mongodb`, `nats`, `neo4j`, `elasticsearch`, `opensearch`, `qdrant`, `weaviate`, `influxdb`, `timescaledb` |
| **In-process** (no container) | `duckdb` — matches the SQLite model (`spawn_blocking`, no `testcontainers`). |

Integration tests that start containers are usually `#[ignore]`; enable with `-- --ignored` when Docker is available. DuckDB tests run in normal `cargo test` when the feature is enabled.

### Default env vars and image overrides

**Service under test** (`services` feature, `auto_inject`): convention keys used when the matching bindings are present on [`Context`](src/lifecycle/mod.rs). Rename with [`EnvMapping`](src/service/types.rs) / [`MappingOverrides`](src/service/types.rs).

| Engine | Default primary env var | Additional convention vars | Image tag override env |
|--------|---------------------------|-----------------------------|-------------------------|
| Redis / Valkey | `REDIS_URL` | — | `SHAKEDOWN_REDIS_IMAGE_TAG` / `SHAKEDOWN_VALKEY_IMAGE_TAG` |
| MongoDB | `MONGODB_URI` | — | `SHAKEDOWN_MONGO_IMAGE_TAG` |
| NATS | `NATS_URL` | — | `SHAKEDOWN_NATS_IMAGE_TAG` |
| Neo4j | `NEO4J_URI` (Bolt URL) | — | `SHAKEDOWN_NEO4J_IMAGE_TAG` |
| DuckDB | `DUCKDB_URL` | — | — (no image) |
| Elasticsearch | `ELASTICSEARCH_URL` | — | `SHAKEDOWN_ELASTICSEARCH_IMAGE_TAG` |
| OpenSearch | `OPENSEARCH_URL` | — | `SHAKEDOWN_OPENSEARCH_IMAGE_TAG` |
| Qdrant | `QDRANT_URL` (gRPC URL) | — | `SHAKEDOWN_QDRANT_IMAGE_TAG` |
| Weaviate | `WEAVIATE_URL` (REST) | — | `SHAKEDOWN_WEAVIATE_IMAGE_TAG` |
| InfluxDB 2.x | `INFLUXDB_URL` | `INFLUXDB_TOKEN`, `INFLUXDB_ORG`, `INFLUXDB_BUCKET` | `SHAKEDOWN_INFLUXDB_IMAGE_TAG` |
| TimescaleDB | `TIMESCALEDB_URL` | — | `SHAKEDOWN_TIMESCALEDB_IMAGE_TAG` |

### Standalone default vs cluster topologies

**First-party builtins target a single standalone node per engine** (one container per engine per test process, suite-shared and refcounted) with **scenario-scoped logical isolation** (prefixes, databases, buckets, etc.). They do **not** start multi-node or highly available clusters.

If you need **cluster** or **replica-set** layouts (Kafka KRaft quorum, Redis Sentinel, MongoDB replica sets, OpenSearch multi-node, and so on), treat that as **custom infrastructure**: implement [`Resource`](src/resources.rs) for your topology, use **Docker Compose** or an external environment, or point bindings at a team-managed endpoint and document the contract in your scenarios. See [Extension points](#extension-points).

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
3. **Docker runner:** `just test-all-include-ignored` (or the equivalent `cargo test -p shakedown --features "kafka,postgres,mariadb,mysql,outbox,rabbitmq,sqlite" -- --include-ignored`) with a working Docker socket — covers ignored integration tests and diagnostics. Uses **nextest** when installed, same as `just test`.
4. **Per-engine vertical smoke (Docker, heavy):** `just test-shakedown-resources-smoke` — all `tests/*_vertical.rs` binaries with `--all-features` and ignored tests enabled; see [Resource smoke coverage](#resource-smoke-coverage).
5. **Macros only:** `cargo test -p shakedown-macros` when you need trybuild without rebuilding the workspace.

### `shakedown` library crate

| Command | What it runs | Docker |
|---------|----------------|--------|
| `cargo test -p shakedown` | Unit + integration tests for default (minimal) features; includes **`mock_service_http`** when `mock-services` is in default features | Not required for the default set |
| `cargo test -p shakedown --features mock-services --test mock_service_http` | HTTP [`HttpMockService`](src/mock_service/http/stub.rs) + coded [`HttpMockServiceTrait`](src/mock_service/http/mod.rs) integration tests only | No |
| `cargo test -p shakedown --features grpc-mock-services --test mock_service_grpc` | gRPC [`GrpcMockService`](src/mock_service/grpc/stub.rs) unary integration test | No |
| `cargo test -p shakedown --features outbox,sqlite` | Outbox helpers + SQLite vertical tests | Not required |
| `cargo test -p shakedown --features outbox,rabbitmq,sqlite` | Adds Rabbit publication helpers’ unit coverage | Not required for SQLite-only tests |
| `cargo test -p shakedown --features services` | Service-under-test unit tests + integration **targets** (Docker cases still **`#[ignore]`** unless you pass `-- --ignored`) | Optional Docker for ignored targets |
| `cargo test -p shakedown --features "kafka,postgres,mariadb,mysql,outbox,rabbitmq,sqlite" -- --include-ignored` | **All** tests: normal + **`#[ignore]`** + doc tests; compiles diagnostics (bollard) | **Required** for ignored integration tests |
| `just test-lib` | `cargo test -p shakedown` | Same as column above |
| `just test-all-include-ignored` | Same as the long `--include-ignored` row; uses **nextest** when installed, else `cargo test` | **Required** for ignored tests |
| `just test-shakedown-resources-smoke` | Runs **every** `tests/*_vertical.rs` integration binary with `--all-features` and `--run-ignored all` / `--include-ignored` (nextest when installed) | **Required** for Docker-backed verticals |

`--include-ignored` runs both non-ignored and ignored tests in one pass. To run **only** ignored tests, use `-- --ignored` on a matching feature set (see per-target recipes below).

### Resource smoke coverage

Each first-party engine has a minimal **vertical** integration test under [`tests/`](tests/) named `*_vertical.rs`: provision the suite container (or in-process DB), assert bindings/helpers, teardown. Together they are the canonical **basic coverage** sweep for [`BuiltinResource`](src/resources/mod.rs).

| Engine / [`BuiltinResource`](src/resources/mod.rs) | Primary smoke test binary | Cargo feature |
|---------------------------------------------------|---------------------------|---------------|
| Postgres | `postgres_vertical` | `postgres` |
| Postgres replica set | `postgres_replica_set_vertical` | `postgres-cluster` |
| MariaDB | `mariadb_vertical` | `mariadb` |
| MySQL | `mysql_vertical` | `mysql` |
| SQLite | `sqlite_vertical` | `sqlite` |
| DuckDB | `duckdb_vertical` | `duckdb` |
| Redis | `redis_vertical` | `redis` |
| Valkey | `valkey_vertical` | `valkey` |
| Redis Sentinel | `redis_sentinel_vertical` | `redis-sentinel` |
| MongoDB | `mongo_vertical` | `mongodb` |
| MongoDB replica set | `mongo_replica_set_vertical` | `mongodb-cluster` |
| Kafka | `macro_kafka` (not `*_vertical`) | `kafka` |
| Kafka cluster | `kafka_cluster_vertical` | `kafka-cluster` |
| RabbitMQ | `helpers_rabbit` (not `*_vertical`) | `rabbitmq` |
| RabbitMQ cluster | `rabbit_cluster_vertical` | `rabbitmq-cluster` |
| NATS | `nats_vertical` | `nats` |
| Neo4j | `neo4j_vertical` | `neo4j` |
| Elasticsearch | `elasticsearch_vertical` | `elasticsearch` |
| OpenSearch | `opensearch_vertical` | `opensearch` |
| Qdrant | `qdrant_vertical` | `qdrant` |
| Weaviate | `weaviate_vertical` | `weaviate` |
| InfluxDB | `influxdb_vertical` | `influxdb` |
| TimescaleDB | `timescaledb_vertical` | `timescaledb` |
| Toxiproxy | `toxiproxy_vertical` | `fault-injection` |

**One command:** [`just test-shakedown-resources-smoke`](../../justfile) runs all `*_vertical.rs` targets (see [`justfile`](../../justfile)). It does **not** include standalone Kafka/Rabbit smoke binaries (`macro_kafka`, `helpers_rabbit`); run those separately if needed.

[`just check-vertical-tests-registered`](../../justfile) fails if a `tests/*_vertical.rs` file is missing a matching `[[test]]` name in `Cargo.toml` (the weekly **Resource smoke** workflow runs this before nextest).

Filter for **nextest** (when using `cargo nextest list` / `run`): integration tests from `tests/*.rs` are marked `(skipped)` until you pass **`--run-ignored all`** — that is how Docker `#[ignore]` tests are included.

### `shakedown-macros` (incl. trybuild)

| Command | What it runs | Docker |
|---------|----------------|--------|
| `cargo test -p shakedown-macros` | Macro unit tests + **trybuild** compile-pass / compile-fail UI tests | No |
| `just test-macros` | Same | No |

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
| Service lifecycle (`whoami`) | `cargo test -p shakedown --features services --test services_lifecycle -- --ignored` | — |
| Service host.docker.internal | `cargo test -p shakedown --features services --test service_host_resolution -- --ignored` | — |
| Multi-service `depends_on` | `cargo test -p shakedown --features services --test services_multi_dependency -- --ignored` | — |
| Service diagnostics (labels + timeline) | `cargo test -p shakedown --features services --test services_diagnostics -- --ignored` | — |
| Kafka + mock service + whoami | `cargo test -p shakedown --features kafka,services --test services_kafka_mock_service_whoami -- --ignored` | — |
| Postgres + two whoami | `cargo test -p shakedown --features postgres,services --test services_multi_postgres_whoami -- --ignored` | — |
| Redis vertical + helpers | `cargo test -p shakedown --features redis --test redis_vertical --test helpers_redis -- --ignored` | — |
| Valkey vertical | `cargo test -p shakedown --features valkey --test valkey_vertical -- --ignored` | — |
| MongoDB vertical + helpers | `cargo test -p shakedown --features mongodb --test mongo_vertical --test helpers_mongo -- --ignored` | — |
| NATS vertical + helpers | `cargo test -p shakedown --features nats --test nats_vertical --test helpers_nats -- --ignored` | — |
| Neo4j vertical + helpers | `cargo test -p shakedown --features neo4j --test neo4j_vertical --test helpers_neo4j -- --ignored` | — |
| DuckDB vertical + helpers | `cargo test -p shakedown --features duckdb --test duckdb_vertical --test helpers_duckdb` | — (no Docker) |
| Elasticsearch vertical + helpers | `cargo test -p shakedown --features elasticsearch --test elasticsearch_vertical --test helpers_elasticsearch -- --ignored` | — |
| OpenSearch vertical + helpers | `cargo test -p shakedown --features opensearch --test opensearch_vertical --test helpers_opensearch -- --ignored` | — |
| Qdrant vertical + helpers | `cargo test -p shakedown --features qdrant --test qdrant_vertical --test helpers_qdrant -- --ignored` | — |
| Weaviate vertical + helpers | `cargo test -p shakedown --features weaviate --test weaviate_vertical --test helpers_weaviate -- --ignored` | — |
| InfluxDB vertical + helpers | `cargo test -p shakedown --features influxdb --test influxdb_vertical --test helpers_influxdb -- --ignored` | — |
| TimescaleDB vertical + helpers | `cargo test -p shakedown --features timescaledb --test timescaledb_vertical --test helpers_timescaledb -- --ignored` | — |

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
just test-all-include-ignored
just test-outbox-sqlite
just test-toxiproxy-ignored
just test-fault-injection-kafka-ignored
```

`just test` and `just qa` use `cargo nextest run` when `cargo-nextest` is installed, and fall back to `cargo test` otherwise. `just test-workspace` and `just qa-full` always use plain `cargo test --workspace` for parity with Cargo-only CI. `just test-all-include-ignored` uses nextest the same way when available.

Docker-backed tests are opt-in; see [Running tests](#running-tests) for the full matrix and individual `just test-*-ignored` recipes.

**Kafka (KRaft, `testcontainers-modules::kafka::apache`):**

- [`KafkaResource`](src/resources.rs) uses a **suite-level** broker: the first provision in a process starts one container; further `KafkaResource` instances share it until the last `Resource::teardown` removes the container. For a walkthrough of Docker-backed checks, see [`feature/mvp-kafka-vertical/README.md`](../../feature/mvp-kafka-vertical/README.md) when that tree exists in your checkout.
- **Image tag:** [`KafkaResource::with_image_tag`](src/resources.rs), or set `SHAKEDOWN_KAFKA_IMAGE_TAG` and use [`KafkaResource::with_image_tag_from_env`](src/resources.rs).
- **JVM image:** [`KafkaResource::with_jvm_image`](src/resources.rs) (`apache/kafka` instead of `apache/kafka-native`).

### KafkaBindings

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

Shakedown injects **TCP-level** faults on traffic that flows through **[Toxiproxy](https://github.com/Shopify/toxiproxy)** (latency, bandwidth, timeout toxics, and so on). This is **not** a general chaos layer: it does **not** simulate arbitrary failure modes (for example process crashes, disk exhaustion, or cloud-AZ loss). It answers “what happens when the **network path to a dependency** is degraded?” so you can tell **harness-level TCP faults** apart from **application bugs** when interpreting a failing run.

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

Use [`run_scenario`](src/lifecycle/mod.rs) with [`ScenarioKind::Faulty`](src/lifecycle/mod.rs) (via [`Context::set_scenario_kind`](src/lifecycle/mod.rs)) and register [`KafkaResource`](src/resources.rs) / [`ToxiproxyResource`](src/resources.rs) manually if you are not using the macro — see [`tests/fault_injection_kafka.rs`](tests/fault_injection_kafka.rs).

**`KafkaBindings` and host:port:** after provision, [`KafkaBindings::bootstrap_servers`](src/resources.rs) is the **single client-facing** address your code should use (`bootstrap.servers` for rdkafka, etc.). For **`ScenarioKind::Faulty`** with **`fault-injection`** enabled and **both** resources registered, the harness rewrites that field to the **Toxiproxy listen** address (pass-through proxy to the real broker) so toxics apply transparently. For **`ScenarioKind::Normal`**, or if Toxiproxy is not registered, **`bootstrap_servers` stays the direct broker** endpoint. Scenarios do not need separate “proxy vs direct” branches for binding selection — only for **when** you call [`FaultHandle`](src/fault_injection.rs) to add or remove toxics.

**CI:** Docker-backed proof tests are `#[ignore]`; run them explicitly, e.g. `cargo test -p shakedown --features kafka,fault-injection --test fault_injection_kafka -- --ignored` or `just test-fault-injection-kafka-ignored`.

**Postgres (`testcontainers-modules::postgres`, `postgres:16` default tag):**

- [`PostgresResource`](src/resources.rs) uses a **suite-level** server: the first provision in a process starts one container; further instances share it until the last `Resource::teardown` drops the refcounted container. Each scenario gets a dedicated database name derived from [`sanitize_sql_database_name`](src/resources.rs).
- **Image tag:** [`PostgresResource::with_image_tag`](src/resources.rs), or set `SHAKEDOWN_POSTGRES_IMAGE_TAG` and use [`PostgresResource::with_image_tag_from_env`](src/resources.rs). Default image tag is **`16`** (Debian-based, non-alpine).

[`PostgresBindings`](src/resources.rs) is applied to [`Context`] after provision (same pattern as `KafkaBindings`). Default seed for Postgres is empty; scenarios own schema and data.

**MariaDB (`testcontainers-modules::mariadb`, `mariadb:11` default tag):**

- [`MariaDBResource`](src/resources.rs) mirrors Postgres: suite-level container, per-scenario `CREATE DATABASE` / `DROP DATABASE`, `mysql_async` for admin and readiness (`SELECT 1` on the scenario database). Root user matches the official image (empty password in the testcontainers module).
- **Image tag:** [`MariaDBResource::with_image_tag`](src/resources.rs), or `SHAKEDOWN_MARIADB_IMAGE_TAG` via [`MariaDBResource::with_image_tag_from_env`](src/resources.rs). Default tag is **`11`**.
- [`MariaDBBindings`](src/resources.rs) is applied after provision (`engine: mariadb` in handle metadata).

**Oracle MySQL (`testcontainers-modules::mysql`, `mysql:8.4` default tag):**

- [`MysqlResource`](src/resources.rs) is a **separate** builtin from MariaDB (R-8): suite-level container, per-scenario database, `mysql_async` for admin and readiness. Uses the official **`mysql`** Docker image, not `mariadb`.
- **Image tag:** [`MysqlResource::with_image_tag`](src/resources.rs), or `SHAKEDOWN_MYSQL_IMAGE_TAG` via [`MysqlResource::with_image_tag_from_env`](src/resources.rs). Default tag is **`8.4`**.
- [`MysqlBindings`](src/resources.rs) is applied after provision (`engine: mysql` in handle metadata).

**Redis (`redis` feature, `testcontainers-modules::redis`, default tag `7`):**

- [`RedisResource`](src/resources.rs) uses a **suite-level** broker: first provision starts one container; refcounted teardown removes it when the last scenario ends. Each scenario gets a [`sanitize_redis_key_prefix`](src/resources.rs) (`shakedown:<scenario>:`) applied by [`RedisHelpers`](src/resources/redis/helpers.rs) and removed with **SCAN + DEL** on teardown.
- **Image tag:** [`RedisResource::with_image_tag`](src/resources.rs), or `SHAKEDOWN_REDIS_IMAGE_TAG` via [`RedisResource::with_image_tag_from_env`](src/resources.rs). Default tag is **`7`** (`redis:7`).
- [`RedisBindings`](src/resources.rs) on [`Context`](src/lifecycle/mod.rs) after provision (`engine: redis` in handle metadata). Use [`Context::redis()`](src/lifecycle/mod.rs) for helpers.

**Valkey (`valkey` feature, `testcontainers-modules::valkey`, default tag `8`):**

- [`ValkeyResource`](src/resources.rs) mirrors Redis: same suite/refcount model, same [`RedisBindings`](src/resources.rs) and [`RedisHelpers`](src/resources/redis/helpers.rs) (`ctx.redis()`), handle metadata `engine: valkey`.
- **Image tag:** [`ValkeyResource::with_image_tag`](src/resources.rs), or `SHAKEDOWN_VALKEY_IMAGE_TAG` via [`ValkeyResource::with_image_tag_from_env`](src/resources.rs). Default tag is **`8`** (`valkey/valkey:8`).

**MongoDB (`mongodb` feature, `testcontainers-modules::mongo`, default tag `7`):**

- [`MongoResource`](src/resources.rs) uses a **suite-level** `mongod`: first provision starts one container; refcounted teardown **drops** the scenario database and removes the container when the last scenario ends. Isolation is a **per-scenario database** name derived from the scenario id (`sanitize_sql_database_name` → `shakedown_*`). Provision materializes the database with a sentinel insert into `_shakedown_init`.
- **Image tag:** [`MongoResource::with_image_tag`](src/resources.rs), or **`SHAKEDOWN_MONGO_IMAGE_TAG`** via [`MongoResource::with_image_tag_from_env`](src/resources.rs). Default tag is **`7`** (`mongo:7`). The suite container sets **`MONGO_INITDB_ROOT_USERNAME`/`PASSWORD`** to fixed test credentials (`root` / `root`); connection URIs include **`authSource=admin`**.
- [`MongoBindings`](src/resources.rs) on [`Context`](src/lifecycle/mod.rs) after provision (`engine: mongodb` in handle metadata). Use [`Context::mongo()`](src/lifecycle/mod.rs) for [`MongoHelpers`](src/resources/mongo/helpers.rs) (`insert_document`, `find_documents`, assertions).

**Neo4j (`neo4j` feature, `testcontainers-modules::neo4j`, default tag `5-enterprise`):**

- **Enterprise default:** Per-scenario isolation uses **`CREATE DATABASE`** against the system catalog (Neo4j 5 **Enterprise**). The container sets **`NEO4J_ACCEPT_LICENSE_AGREEMENT=yes`** and **`NEO4J_AUTH=neo4j/shakedown`** (fixed test credentials).
- **Image tag:** [`Neo4jResource::with_image_tag`](src/resources.rs), or **`SHAKEDOWN_NEO4J_IMAGE_TAG`**. Community Edition does **not** support multiple databases — provisioning will fail unless you stay on Enterprise (or accept a non-isolated single-DB setup outside this resource’s contract).
- [`Neo4jBindings`](src/resources.rs) (`bolt_url`, `http_url`, `database`, …) on [`Context`](src/lifecycle/mod.rs). Use [`Context::neo4j()`](src/lifecycle/mod.rs) for [`Neo4jHelpers`](src/resources/neo4j/helpers.rs): `execute_cypher`, `query_cypher`, `assert_node_exists`, `assert_relationship_exists`. Readiness runs **`RETURN 1`** on the **`system`** database over Bolt.

**NATS (`nats` feature, `testcontainers-modules::nats`, default tag `2`):**

- [`NatsResource`](src/resources.rs) uses a **suite-level** broker: first provision starts one container; refcounted teardown removes it when the last scenario ends. Each scenario gets a [`sanitize_nats_subject_prefix`](src/resources.rs) (`shakedown.<scenario>.`) applied by [`NatsHelpers`](src/resources/nats/helpers.rs) to logical subjects. The container starts with **JetStream** (`--jetstream`) so [`NatsPublicationVerifier`](src/publication/nats_verifier.rs) can assert persisted messages.
- **Image tag:** [`NatsResource::with_image_tag`](src/resources.rs), or **`SHAKEDOWN_NATS_IMAGE_TAG`** via [`NatsResource::with_image_tag_from_env`](src/resources.rs). Default tag is **`2`** (`nats:2`).
- [`NatsBindings`](src/resources.rs) on [`Context`](src/lifecycle/mod.rs) after provision (`engine: nats` in handle metadata). Use [`Context::nats()`](src/lifecycle/mod.rs) for [`NatsHelpers`](src/resources/nats/helpers.rs) (`publish`, `subscribe_until` → [`ConsumedMessage`](src/helper_prelude.rs) list). Readiness uses **`async_nats`** connect + **`flush`** (client handshake to the broker).

**Weaviate (`weaviate` feature, `semitechnologies/weaviate` via `GenericImage`, default tag `1.28.2` — aligned with `testcontainers-modules::weaviate`):**

- [`WeaviateResource`](src/resources.rs) uses a **suite-level** node: first provision starts one container; refcounted teardown deletes schema classes matching the scenario class prefix, then may remove the container. A short container wait plus lifecycle **`GET /v1/.well-known/ready`** avoids flaky HTTP wait strategies on slow hosts. **`AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true`** is set for local tests.
- **Image tag:** [`WeaviateResource::with_image_tag`](src/resources.rs), or **`SHAKEDOWN_WEAVIATE_IMAGE_TAG`** via [`WeaviateResource::with_image_tag_from_env`](src/resources.rs).
- [`WeaviateBindings`](src/resources.rs) (`rest_url`, `class_prefix`, …) on [`Context`](src/lifecycle/mod.rs). Use [`Context::weaviate()`](src/lifecycle/mod.rs) for [`WeaviateHelpers`](src/resources/weaviate/helpers.rs) (`create_class`, `create_object`, `get_object`, assertions). Readiness uses **`GET /v1/.well-known/ready`**.

**InfluxDB 2.x (`influxdb` feature, `influxdb:2` via `GenericImage`):**

- [`InfluxDbResource`](src/resources.rs) uses a **suite-level** node: first provision starts one container with **`DOCKER_INFLUXDB_INIT_MODE=setup`**, fixed org **`shakedown`**, and admin token **`shakedown-admin-token`**; each scenario gets a **per-scenario bucket** via **`POST /api/v2/buckets`**; refcounted teardown **`DELETE`s** that bucket, then may remove the container. Helpers use **`reqwest`** only (no bundled Influx 2.x client crate).
- **Image tag:** [`InfluxDbResource::with_image_tag`](src/resources.rs), or **`SHAKEDOWN_INFLUXDB_IMAGE_TAG`** via [`InfluxDbResource::with_image_tag_from_env`](src/resources.rs). Default tag is **`2`** (`influxdb:2`).
- [`InfluxDbBindings`](src/resources.rs) (`url`, `token`, `organization`, `bucket`, …) on [`Context`](src/lifecycle/mod.rs). Use [`Context::influxdb()`](src/lifecycle/mod.rs) for [`InfluxDbHelpers`](src/resources/influxdb/helpers.rs) (`write_point`, `query`, `assert_point_exists`). Readiness uses **`GET /health`** with JSON **`status: pass`**.

**TimescaleDB (`timescaledb` feature, `timescale/timescaledb:latest-pg16` via `GenericImage`):**

- [`TimescaledbResource`](src/resources.rs) uses a **suite-level** Postgres-compatible server: first provision starts one container (`POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` = **`postgres`**); each scenario gets **`CREATE DATABASE`** isolation like [`PostgresResource`](src/resources.rs); refcounted teardown drops the database and may remove the container. Bindings reuse **[`PostgresBindings`](src/resources.rs)** on [`Context::timescaledb_bindings`](src/lifecycle/mod.rs) (distinct from vanilla Postgres when both are registered).
- **Image tag:** [`TimescaledbResource::with_image_tag`](src/resources.rs), or **`SHAKEDOWN_TIMESCALEDB_IMAGE_TAG`** via [`TimescaledbResource::with_image_tag_from_env`](src/resources.rs). Default tag is **`latest-pg16`** (`timescale/timescaledb:latest-pg16`).
- Use [`Context::timescaledb()`](src/lifecycle/mod.rs) for [`TimescaledbHelpers`](src/resources/timescaledb/helpers.rs): delegates SQL helpers to the same path as [`PostgresHelpers`](src/resources/postgres/helpers.rs) and adds **`assert_hypertable_exists`**. Readiness uses **`SELECT 1`** via **`tokio-postgres`**.

**Service injection:** with the **`services`** feature, [`EnvMapping::redis`](src/service/types.rs) and convention injection default to **`REDIS_URL`** for [`RedisBindings::connection_url`](src/resources.rs) (shared name for Redis and Valkey). [`EnvMapping::mongo`](src/service/types.rs) and convention injection default to **`MONGODB_URI`** for [`MongoBindings::connection_url`](src/resources.rs) (per-service URL partitioning matches relational DBs when multiple services are registered). [`EnvMapping::nats`](src/service/types.rs) and convention injection default to **`NATS_URL`** for [`NatsBindings::connection_url`](src/resources.rs). [`EnvMapping::neo4j`](src/service/types.rs) and convention injection default to **`NEO4J_URI`** for [`Neo4jBindings::bolt_url`](src/resources.rs). [`EnvMapping::weaviate`](src/service/types.rs) and convention injection default to **`WEAVIATE_URL`** for [`WeaviateBindings::rest_url`](src/resources.rs). [`EnvMapping::influxdb`](src/service/types.rs) and convention injection default to **`INFLUXDB_URL`** for [`InfluxDbBindings::url`](src/resources.rs); **`INFLUXDB_TOKEN`**, **`INFLUXDB_ORG`**, and **`INFLUXDB_BUCKET`** are also set for Influx 2.x clients. [`EnvMapping::timescaledb`](src/service/types.rs) and convention injection default to **`TIMESCALEDB_URL`** (distinct from Postgres **`DATABASE_URL`**).

Integration tests that start containers (Kafka, Postgres, MariaDB, MySQL, Redis, Valkey, MongoDB, NATS, Neo4j, Weaviate, InfluxDB, TimescaleDB, outbox flows, bollard diagnostics) are usually `#[ignore]`; run them with Docker available. Commands and `just` recipes are listed in [Running tests](#running-tests) (Docker-backed `#[ignore]` tests).

Verification steps for Kafka vertical work are summarized in [`feature/mvp-kafka-vertical/README.md`](../../feature/mvp-kafka-vertical/README.md) (checklists and expected outcomes for Docker-backed runs).

## Infrastructure diagnostics (bollard)

When you enable any **Docker-backed** resource feature (`kafka`, `postgres`, `mariadb`, `mysql`, `redis`, `valkey`, `mongodb`, `nats`, `neo4j`, `elasticsearch`, `opensearch`, `qdrant`, `weaviate`, `influxdb`, `timescaledb`), the crate also enables the **`diagnostics`** feature unless you opt out with **`no-diagnostics`**.

What it provides (read-only — never starts or stops containers; testcontainers owns lifecycle):

- **Container log streaming** — stdout/stderr lines are recorded as [`InfraEvent::ContainerLog`](src/event.rs) entries in the same [`EventLog`](src/event.rs) used for failure timelines.
- **Docker Engine events** — filtered to containers labeled `io.shakedown.framework=true` (applied by Shakedown resources), including `stop`, non-zero `die`, OOM, health transitions, and `restart`.
- **Resource stats** — periodic CPU/memory/network samples in bounded per-container ring buffers, merged into the event log after the scenario; use [`Context::assert_resource`](src/lifecycle/mod.rs) for `peak_memory_under` / `avg_cpu_under` when diagnostics are active.

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

**Stats tuning** (optional): `SHAKEDOWN_STATS_INTERVAL_MS` (default `1000`) and `SHAKEDOWN_STATS_BUFFER_DEPTH` (default `60`, capped) control how often samples are taken and how many per container are retained for timelines and [`Context::assert_resource`](src/lifecycle/mod.rs).

**Integration tests** (`cargo test -p shakedown --features kafka`; see [Running tests](#running-tests) for `just` recipes):

- [`tests/diagnostics_graceful.rs`](tests/diagnostics_graceful.rs) — `FakeResource` only, invalid `DOCKER_HOST` pointing at a non-existent Unix socket; asserts the scenario succeeds and no infrastructure rows are recorded (no Docker daemon required).
- [`tests/diagnostics_integration.rs`](tests/diagnostics_integration.rs) — log streaming, Docker `stop` events, and stats rings against a labeled Alpine container; Docker-backed cases use `just test-diagnostics-integration-ignored` or the matching `cargo` line in [Running tests](#running-tests).

## Typed helpers

The harness always exposes **structured bindings** on [`Context`] after provisioning (for example [`KafkaBindings`](src/resources.rs), [`RabbitBindings`](src/resources.rs), [`PostgresBindings`](src/resources.rs), [`RedisBindings`](src/resources.rs) for Redis/Valkey, [`MongoBindings`](src/resources.rs) for MongoDB, [`NatsBindings`](src/resources.rs) for NATS, [`Neo4jBindings`](src/resources.rs) for Neo4j). On top of that, Shakedown offers **optional typed helpers** — thin facades that apply scenario isolation (topic prefixes, namespaced AMQP names, scoped database URLs, Redis key prefixes, MongoDB database names, NATS subject prefixes, Neo4j database names) so common steps stay short.

There are **three patterns**; pick based on what you are proving:

### Pattern 1: bindings only (bring your own client)

**When to use this:** You need **full control** — a different driver stack, TLS/SASL, connection pooling, consumer groups, advanced Kafka APIs, or custom SQL session settings. Read `ctx.kafka_bindings()`, `ctx.rabbit_bindings()`, `ctx.postgres_bindings()`, etc., and construct your own clients from those values. Nothing in the helpers replaces that path.

### Pattern 2: typed helpers (produce / consume / assert)

**When to use this:** You want the **happy path** for scenario steps: produce or consume messages with isolation baked in, or run SQL assertions against the scenario-scoped database without hand-rolling connection URLs. Helpers use the **same effective bindings** as raw accessors (including Toxiproxy-rewritten Kafka bootstrap when fault injection is active — see [Fault injection](#fault-injection-toxiproxy-and-the-kafka-path)).

Entry points: `ctx.kafka()` ([`KafkaHelpers`](src/resources/kafka/helpers.rs)), `ctx.rabbit()` ([`RabbitHelpers`](src/resources/rabbit/helpers.rs)), `ctx.postgres()` ([`PostgresHelpers`](src/resources/postgres/helpers.rs)), `ctx.mariadb()` / `ctx.mysql()` / `ctx.sqlite()` ([`MariaDbHelpers`](src/resources/mariadb/helpers.rs), [`MysqlHelpers`](src/resources/mysql/helpers.rs), [`SqliteHelpers`](src/resources/sqlite/helpers.rs)), `ctx.redis()` ([`RedisHelpers`](src/resources/redis/helpers.rs)), `ctx.mongo()` ([`MongoHelpers`](src/resources/mongo/helpers.rs)), `ctx.nats()` / `ctx.neo4j()` / `ctx.duckdb()` / `ctx.elasticsearch()` / `ctx.opensearch()` / `ctx.qdrant()` / `ctx.weaviate()` / `ctx.influxdb()` / `ctx.timescaledb()` — see [Extra infrastructure features](#extra-infrastructure-features-extra-infra) and the [feature flags table](#feature-flags-helpers) below.

### Pattern 3: outbox rows + publication verifiers

**When to use this:** You are testing the **transactional outbox** flow — rows in `outbox_event` **and** proof that a message appeared on a supported bus. [`Outbox`](src/outbox.rs) plus [`PublicationVerifier`](src/publication/verifier.rs) (`assert_kafka_published`, `assert_rabbit_published`, `assert_nats_published`, `assert_redis_streams_published`) is the contract for that. Helpers **do not** duplicate verifier semantics; use them for direct bus/DB steps, and verifiers when you need publication proof tied to outbox semantics.

### Feature flags (helpers)

Helpers are gated behind the **same** resource features as the underlying providers — no separate `helpers` feature.

| Surface | Cargo feature | Module / types |
|---------|----------------|------------------|
| `ctx.kafka()` → [`KafkaHelpers`](src/resources/kafka/helpers.rs) | `kafka` | `rdkafka` |
| `ctx.rabbit()` → [`RabbitHelpers`](src/resources/rabbit/helpers.rs), [`ExchangeKind`](src/resources/rabbit/helpers.rs) | `rabbitmq` | `lapin` |
| `ctx.postgres()` → [`PostgresHelpers`](src/resources/postgres/helpers.rs) | `postgres` | `tokio-postgres` |
| `ctx.mariadb()` → [`MariaDbHelpers`](src/resources/mariadb/helpers.rs) | `mariadb` | `mysql_async` |
| `ctx.mysql()` → [`MysqlHelpers`](src/resources/mysql/helpers.rs) | `mysql` | `mysql_async` |
| `ctx.sqlite()` → [`SqliteHelpers`](src/resources/sqlite/helpers.rs) | `sqlite` | `rusqlite` (via `spawn_blocking` inside helpers) |
| `ctx.redis()` → [`RedisHelpers`](src/resources/redis/helpers.rs) | `redis` or `valkey` | `redis` crate (RESP) |
| `ctx.mongo()` → [`MongoHelpers`](src/resources/mongo/helpers.rs) | `mongodb` | `mongodb` official driver |
| `ctx.nats()` → [`NatsHelpers`](src/resources/nats/helpers.rs) | `nats` | `async-nats` |
| `ctx.neo4j()` → [`Neo4jHelpers`](src/resources/neo4j/helpers.rs) | `neo4j` | `neo4rs` |
| `ctx.elasticsearch()` → [`ElasticsearchHelpers`](src/resources/elasticsearch/helpers.rs) | `elasticsearch` | `reqwest` (HTTP) |
| `ctx.opensearch()` → [`OpenSearchHelpers`](src/resources/opensearch/helpers.rs) | `opensearch` | `reqwest` (HTTP) |
| `ctx.qdrant()` → [`QdrantHelpers`](src/resources/qdrant/helpers.rs) | `qdrant` | `qdrant-client` (gRPC) |
| `ctx.weaviate()` → [`WeaviateHelpers`](src/resources/weaviate/helpers.rs) | `weaviate` | `reqwest` (REST) |
| `ctx.influxdb()` → [`InfluxDbHelpers`](src/resources/influxdb/helpers.rs) | `influxdb` | `reqwest` (HTTP line protocol + Flux) |
| `ctx.timescaledb()` → [`TimescaledbHelpers`](src/resources/timescaledb/helpers.rs) | `timescaledb` | `tokio-postgres` (same bindings shape as Postgres) |

[`ConsumedMessage`](src/helper_prelude.rs) and [`DEFAULT_HELPER_CONSUME_TIMEOUT`](src/helper_prelude.rs) are re-exported from the crate root for Kafka, Rabbit, and NATS consume helpers.

### Example: Kafka (produce → consume)

Enable `kafka` in `Cargo.toml`. Logical topic names are prefixed automatically via [`KafkaBindings::topic_namespace`](src/resources.rs).

```rust,ignore
#[shakedown::test(resources = [Kafka])]
async fn kafka_helpers_example(ctx: &mut shakedown::Context) -> Result<(), shakedown::ShakedownError> {
    ctx.kafka()
        .produce("orders", "order-1", br#"{"sku":"A"}"#)
        .await?;

    let consumed = ctx.kafka().consume_one("orders", None).await?;
    assert_eq!(consumed.key_str(), Some("order-1"));

    // Or filter with `consume_until` and a predicate over `ConsumedMessage`:
    let _matched = ctx
        .kafka()
        .consume_until("orders", |message| message.key_str() == Some("order-1"), None)
        .await?;
    Ok(())
}
```

Runnable Docker-backed coverage: [`tests/helpers_kafka.rs`](tests/helpers_kafka.rs) (`cargo test -p shakedown --features kafka --test helpers_kafka -- --ignored`).

### Example: RabbitMQ (topology → publish → consume)

Enable `rabbitmq`. Use [`declare_topology`](src/resources/rabbit/helpers.rs) so exchange, queue, and bind are declared with scenario namespacing in one call.

```rust,ignore
use shakedown::ExchangeKind;

#[shakedown::test(resources = [Rabbit])]
async fn rabbit_helpers_example(ctx: &mut shakedown::Context) -> Result<(), shakedown::ShakedownError> {
    ctx.rabbit()
        .declare_topology("events", ExchangeKind::Direct, "events", "order.created")
        .await?;

    ctx.rabbit()
        .publish("events", "order.created", br#"{"order_id":"42"}"#)
        .await?;

    let consumed = ctx
        .rabbit()
        .consume_until("events", |_| true, None)
        .await?;
    let _: serde_json::Value = consumed.payload_json()?;
    Ok(())
}
```

Runnable Docker-backed coverage: [`tests/helpers_rabbit.rs`](tests/helpers_rabbit.rs) (`cargo test -p shakedown --features rabbitmq --test helpers_rabbit -- --ignored`).

### Example: Postgres (execute → assert)

Enable `postgres`. Parameters use `tokio_postgres::types::ToSql` ([`PostgresHelpers`](src/resources/postgres/helpers.rs)).

```rust,ignore
#[shakedown::test(resources = [Postgres])]
async fn postgres_helpers_example(ctx: &mut shakedown::Context) -> Result<(), shakedown::ShakedownError> {
    ctx.postgres()
        .execute(
            "INSERT INTO orders (id, total) VALUES ($1, $2)",
            &[&42_i32, &99.0_f64],
        )
        .await?;

    ctx.postgres()
        .assert_row_exists("SELECT 1 FROM orders WHERE id = $1", &[&42_i32])
        .await?;

    ctx.postgres()
        .assert_row_count("SELECT * FROM orders WHERE id = $1", &[&42_i32], 1)
        .await?;
    Ok(())
}
```

Runnable Docker-backed coverage: [`tests/helpers_postgres.rs`](tests/helpers_postgres.rs). For **SQLite** without Docker, see [`tests/helpers_sqlite.rs`](tests/helpers_sqlite.rs) (`cargo test -p shakedown --features sqlite --test helpers_sqlite`).

## Outbox helpers

The outbox API has **two layers**. You can use row assertions **without** any message bus; publication proof is **optional** and gated per transport (`kafka`, `rabbitmq`, `nats`, `redis` for Redis Streams, …).

1. **Row assertions** — Canonical `outbox_event` shape: SQL engines use a table; MongoDB uses a collection; same assertion API (`assert_pending`, `assert_sent`, `assert_failed`, `assert_retry`, payload checks). Requires `outbox` plus **one storage feature**: **`postgres`**, **`mariadb`**, **`mysql`**, **`sqlite`**, **`mongodb`**, or **`duckdb`**. They do **not** require any message-bus feature.
2. **Publication proof** — Observe a message for your aggregate on a bus via [`PublicationVerifier`](src/publication/verifier.rs). First-party implementations and [`Outbox`](src/outbox.rs) convenience methods:

| Publication verifier | Cargo feature | Types / convenience |
|---------------------|---------------|---------------------|
| **Kafka** | `kafka` | [`KafkaPublicationVerifier`](src/publication/kafka_verifier.rs), [`Outbox::assert_kafka_published`](src/outbox.rs) |
| **RabbitMQ** | `rabbitmq` | [`RabbitMqPublicationVerifier`](src/publication/rabbit_verifier.rs), [`Outbox::assert_rabbit_published`](src/outbox.rs) |
| **NATS** (JetStream) | `nats` | [`NatsPublicationVerifier`](src/publication/nats_verifier.rs), [`Outbox::assert_nats_published`](src/outbox.rs) |
| **Redis Streams** | `redis` (or `valkey` with the same verifier) | [`RedisStreamsPublicationVerifier`](src/publication/redis_streams_verifier.rs), [`Outbox::assert_redis_streams_published`](src/outbox.rs) |

Enable the matching Cargo feature; `outbox` alone does **not** pull in transports.

**Publication errors** are transport-neutral and distinct from row errors such as `OutboxRowNotFound`. First-party verifiers return [`OutboxPublicationNotFound`](crate::error::ShakedownError::OutboxPublicationNotFound) when the observation window expires without a matching message (`transport`: `"kafka"`, `"rabbitmq"`, `"nats"`, or `"redis_streams"`). [`OutboxPublicationTimeout`](crate::error::ShakedownError::OutboxPublicationTimeout) is available for **custom** [`PublicationVerifier`](#implementing-publicationverifier) implementations that need a separate “time-bounded infrastructure failure” classification (first-party code does not emit it today). Both variants carry `transport`, `channel`, and `aggregate_id`; prefer matching on these variants (or [`ShakedownError`](crate::error::ShakedownError) generally) for publication failures.

### Outbox storage engines (first-party)

| Storage | Cargo feature | Notes |
|---------|---------------|--------|
| **PostgreSQL** | `postgres` | Full SQL semantics for row locking in production-style outbox processors. |
| **MariaDB** | `mariadb` | Same. |
| **MySQL** | `mysql` | Same. |
| **SQLite** | `sqlite` | **Assertion-only** — no `SELECT FOR UPDATE SKIP LOCKED`; see [Assertion-only storage](#assertion-only-storage-sqlite-and-duckdb). |
| **MongoDB** | `mongodb` | Document `outbox_event` collection; same logical schema as SQL backends. |
| **DuckDB** | `duckdb` | **Assertion-only** — same limitation as SQLite; in-process, no Docker. |

**Postgres-compatible engines** such as **TimescaleDB** use the existing relational outbox path via `postgres` / `timescaledb` bindings (same `Outbox` row APIs).

### Full combination matrix (storage × publication)

Any first-party **storage** row can pair with any first-party **publication** verifier: enable `outbox`, exactly one storage feature, and one or more bus features. All cells below are supported.

| Storage \ Publication | Kafka | RabbitMQ | NATS | Redis Streams |
|----------------------|:-----:|:--------:|:----:|:-------------:|
| Postgres | ✓ | ✓ | ✓ | ✓ |
| MariaDB | ✓ | ✓ | ✓ | ✓ |
| MySQL | ✓ | ✓ | ✓ | ✓ |
| SQLite | ✓ | ✓ | ✓ | ✓ |
| MongoDB | ✓ | ✓ | ✓ | ✓ |
| DuckDB | ✓ | ✓ | ✓ | ✓ |

### NATS JetStream (publish-then-verify)

[`NatsPublicationVerifier`](src/publication/nats_verifier.rs) reads from a **JetStream** stream that captures the scenario-namespaced subject (ephemeral pull consumer, `DeliverPolicy::All`). Messages are persisted, so you can publish first — including from the service under test — and call [`Outbox::assert_nats_published`](src/outbox.rs) afterward. The suite [`NatsResource`](src/resources/nats/resource.rs) starts `nats-server` with `--jetstream`. Override the stream name with [`NatsPublicationVerifierOptions`](src/publication/nats_verifier.rs); when `stream_name` is empty, it defaults to `SD_` plus a sanitized form of the namespaced subject.

### Supported combinations (commands)

| What you need | Cargo features | Typical CI / local command (Docker where marked) |
|---------------|----------------|---------------------------------------------------|
| Outbox **rows only** (fast, no Docker) | `outbox`, `sqlite` or `outbox`, `duckdb` | `cargo test -p shakedown --features outbox,sqlite` / `… duckdb,outbox --test outbox_duckdb` |
| **MongoDB** outbox rows | `outbox`, `mongodb` | `cargo test -p shakedown --features outbox,mongodb --test outbox_mongo -- --ignored` |
| Postgres + **Kafka** publication proof | `outbox`, `postgres`, `kafka` | `cargo test -p shakedown --features outbox,postgres,kafka --test outbox_postgres -- --ignored` |
| Postgres + **RabbitMQ** publication proof | `outbox`, `postgres`, `rabbitmq` | `cargo test -p shakedown --features outbox,postgres,rabbitmq --test outbox_rabbit -- --ignored` |
| SQLite + **NATS** publication proof | `outbox`, `sqlite`, `nats` | `cargo test -p shakedown --features outbox,sqlite,nats --test outbox_nats_publication -- --ignored` |
| SQLite + **Redis Streams** publication proof | `outbox`, `sqlite`, `redis` | `cargo test -p shakedown --features outbox,sqlite,redis --test outbox_redis_streams_publication -- --ignored` |
| Rabbit verifier **unit tests** without Docker DB | `outbox`, `sqlite`, `rabbitmq` | `cargo test -p shakedown --features outbox,rabbitmq,sqlite` |
| **Custom bus** (e.g. SQS) | `outbox` (+ your client crates) | Implement [`PublicationVerifier`](#implementing-publicationverifier) in your crate or test module |

Full quality gates for this crate often include: `cargo check --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all -- --check`, `cargo test -p shakedown --features outbox,postgres,kafka`, and `cargo test -p shakedown --features outbox,postgres,rabbitmq -- --ignored`.

### Feature flags (examples)

**Postgres + Kafka** — row assertions and Kafka publication proof:

```toml
[dependencies]
shakedown = { version = "...", features = ["outbox", "postgres", "kafka"] }
```

**Postgres + RabbitMQ** — same row APIs; use `assert_rabbit_published` instead of Kafka. In seed, declare the exchange, queue, and binding (routing key) so the outbox relay and consumer topology match what the verifier expects.

```toml
[dependencies]
shakedown = { version = "...", features = ["outbox", "postgres", "rabbitmq"] }
```

**MongoDB + NATS** — document outbox rows + NATS publication proof:

```toml
[dependencies]
shakedown = { version = "...", features = ["outbox", "mongodb", "nats"] }
```

**DuckDB + Redis Streams** — in-process rows + stream observation (Redis still needs Docker for integration tests that start the resource):

```toml
[dependencies]
shakedown = { version = "...", features = ["outbox", "duckdb", "redis"] }
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

Use this when your team’s bus is **not** covered by a first-party verifier (first-party today: **Kafka**, **RabbitMQ**, **NATS** JetStream, **Redis Streams**). For example, **AWS SQS** is not implemented first-party, but the trait is shaped so you can add an impl without a breaking API change.

1. **Trait** — Implement [`PublicationVerifier`](src/publication/verifier.rs) for your type with an associated `Options` struct (exchange, queue URL, region, etc.).
2. **`assert_observed`** — Take [`Context`](src/lifecycle/mod.rs), a [`PublicationObserveSpec`](src/publication/spec.rs) (`aggregate_id`, `logical_channel`, optional `timeout`), and `&Self::Options`. Poll or consume until [`message_matches_aggregate_id`](src/publication/matching.rs) succeeds or the resolved deadline is exceeded ([`DEFAULT_PUBLICATION_OBSERVE_TIMEOUT`](src/publication/spec.rs): 20s when `timeout` is `None`).
3. **Bindings** — Read connection details from [`Context::kafka_bindings()`](src/resources.rs), [`Context::rabbit_bindings()`](src/resources.rs), [`Context::nats_bindings()`](src/resources.rs), [`Context::redis_bindings()`](src/resources.rs), or fields you attach via your own resources; keep scenario isolation consistent with how your publisher names topics, queues, subjects, or streams.
4. **Errors** — When the resolved deadline passes without a matching message, return [`OutboxPublicationNotFound`](crate::error::ShakedownError::OutboxPublicationNotFound) with `transport` set to a stable label (e.g. `"sqs"`), matching first-party verifiers. Optionally return [`OutboxPublicationTimeout`](crate::error::ShakedownError::OutboxPublicationTimeout) if your verifier distinguishes an infrastructure/connectivity time limit from “no matching publication observed.”

To use your verifier from tests, call `PublicationVerifier::assert_observed(&your_verifier, &ctx, &spec, &options)` (or wrap it in a small helper on `Outbox` in your codebase).

### Assertion-only storage (SQLite and DuckDB)

`SELECT FOR UPDATE SKIP LOCKED` is unavailable on **SQLite** and **DuckDB**, so claim-locking / publisher concurrency semantics are not validated on those paths. Use Postgres, MariaDB, or MySQL (or exercise your processor logic in tests that do not rely on SKIP LOCKED) when you need lock-semantics coverage.

## Mock services

**What this is:** The harness starts **real HTTP/1.1 and (optionally) unary gRPC servers on `127.0.0.1`** and records every inbound request/response on a [`MockServiceHandle`](src/mock_service/handle.rs). Your service-under-test keeps its **real HTTP or gRPC client stack** — serialization, timeouts, and status handling match production. This is **network-level** substitution (stub upstreams at URLs you inject into config), not in-process trait mocking.

**Lifecycle:** Register mock services on [`Context`](src/lifecycle/mod.rs) **before** [`run_scenario`](src/lifecycle/mod.rs). The harness starts servers after provisioning/readiness/seed and **before** your scenario body runs, so [`Context::mock_service_url`](src/lifecycle/mod.rs) and [`Context::mock_service`](src/lifecycle/mod.rs) return `Some(_)` inside the scenario closure for registered names. Events are logged with `source: "mock-service"` for failure timelines.

### Cargo features

| Feature | Default? | What it enables |
|---------|----------|-----------------|
| **`mock-services`** | Yes | HTTP mock services: [`HttpMockService`](src/mock_service/http/stub.rs) and [`HttpMockServiceTrait`](src/mock_service/http/mod.rs) served by hyper. |
| **`grpc-mock-services`** | No (opt-in) | Unary gRPC mock services: [`GrpcMockService`](src/mock_service/grpc/stub.rs) and [`GrpcMockServiceTrait`](src/mock_service/grpc/mod.rs) served by tonic. |

### Quick-start (HTTP)

Runnable reference: [`tests/mock_service_http.rs`](tests/mock_service_http.rs).

```rust,ignore
use shakedown::{run_scenario, Context, EventLog, HttpMockService};

#[tokio::test]
async fn http_mock_service_quickstart() {
    let stub = HttpMockService::new("api").on("GET", "/health").respond(200);
    let mut ctx = Context::new();
    ctx.register_http_mock_service("api", stub).expect("register");

    let mut event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "http_mock_service_quickstart",
        |context| {
            Box::pin(async move {
                let base = context.mock_service_url("api").expect("api url");
                let _ = reqwest::get(format!("{base}/health")).await.expect("GET /health");
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}
```

### Quick-start (gRPC)

Runnable reference: [`tests/mock_service_grpc.rs`](tests/mock_service_grpc.rs) (requires `grpc-mock-services`).

When you expect calls:

```rust
const GRPC_PATH: &str = "/echo.EchoService/SayHello";

handle.assert_called_times(1).expect("one unary call");
handle
    .assert_path_called_times(GRPC_PATH, GRPC_PATH, 1)
    .expect("path-specific count");
```

For gRPC, pass the full HTTP/2 path (leading `/`) for **both** the `method` and `path` arguments to [`assert_path_called_times`](src/mock_service/handle.rs) — the recorded [`RecordedCall`](src/mock_service/handle.rs) stores that path in both fields.

When the mock service must stay idle:

```rust
handle.assert_not_called().expect("no calls");
```

### Verification and tests

Automated references: [`tests/mock_service_http.rs`](tests/mock_service_http.rs) and [`tests/mock_service_grpc.rs`](tests/mock_service_grpc.rs).

## Service under test (`services` feature)

**What this is:** In addition to **infrastructure** resources (Kafka, Postgres, …) and **mock services** (stub upstreams), Shakedown can start **your service** (or a lightweight stand-in image) as a **Docker container** inside the same [`run_scenario`](src/lifecycle/mod.rs) lifecycle. After infrastructure is ready and mock services are listening, the harness **injects** connection strings and URLs into the container, waits for **readiness** using configurable probes, and exposes each service’s **HTTP base URL** on [`Context`](src/lifecycle/mod.rs) for assertions from the **test process** (loopback). Teardown stops service containers **before** mock services and infrastructure so graceful shutdown still sees live dependencies.

**Cargo feature:** Enable **`services`** (opt-in — not in default features). It pulls **testcontainers**, **reqwest** (HTTP health probes), and **`diagnostics`** (bollard) so service containers can participate in the same observability story as infra. Compose with **`kafka`**, **`postgres`**, **`mock-services`**, and other features when you want **convention-based** injection from those bindings.

```toml
[dev-dependencies]
shakedown = { version = "0.1.0", default-features = false, features = ["services", "mock-services"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
reqwest = { version = "0.11", default-features = false, features = ["json", "rustls-tls"] }
```

`default-features = false` is shown so you opt in explicitly; many teams keep default **`mock-services`** on and add **`services`**.

Public entry points (all behind `feature = "services"`): [`ServiceUnderTest`](src/service/builder.rs), [`ServiceSpec`](src/service/types.rs), [`HealthCheck`](src/service/health.rs), [`EnvMapping`](src/service/types.rs) / [`PullPolicy`](src/service/types.rs), [`ServiceHandle`](src/service/handle.rs), [`Context::register_service`](src/lifecycle/mod.rs), [`Context::service_url`](src/lifecycle/mod.rs), [`Context::service_handle`](src/lifecycle/mod.rs). Address rewriting for containers uses [`HOST_DOCKER_INTERNAL`](src/service/address.rs) (see §2).

Runnable references in this repo: [`tests/services_lifecycle.rs`](tests/services_lifecycle.rs) (single container), [`tests/services_kafka_mock_service_whoami.rs`](tests/services_kafka_mock_service_whoami.rs), [`tests/services_multi_postgres_whoami.rs`](tests/services_multi_postgres_whoami.rs).

### 1. Single-service quick-start (register, health check, assert)

1. Build a [`ServiceSpec`](src/service/types.rs) with [`ServiceUnderTest::docker`](src/service/builder.rs) — **`.name(...)`** and **at least one `.port(...)`** are required; if you omit [`.health_check`](src/service/builder.rs), the harness defaults to [`HealthCheck::tcp`](src/service/health.rs).
2. Call [`Context::register_service`](src/lifecycle/mod.rs) **before** [`run_scenario`](src/lifecycle/mod.rs). Dependencies must already be registered (see §3).
3. Inside the scenario closure, use [`Context::service_url`](src/lifecycle/mod.rs) for HTTP clients from the **host** (`http://127.0.0.1:…`). [`Context::service_handle`](src/lifecycle/mod.rs) exposes [`ServiceHandle::base_url`](src/service/handle.rs), [`container_id`](src/service/handle.rs), and [`is_healthy`](src/service/handle.rs).

The example below matches [`tests/services_lifecycle.rs`](tests/services_lifecycle.rs) and requires **Docker** when executed (`#[ignore]` in the crate’s integration test). Add **`tokio`** and **`reqwest`** to your crate as in the manifest snippet above.

```rust
use shakedown::{run_scenario, Context, EventLog, HealthCheck, ServiceUnderTest};

#[tokio::test]
async fn service_quickstart() {
    let specification = ServiceUnderTest::docker("traefik/whoami:v1.10")
        .name("web")
        .port(80)
        .health_check(HealthCheck::http("/"))
        .build()
        .expect("valid ServiceSpec");

    let mut context = Context::new();
    context.register_service(specification).expect("register_service");

    let mut event_log = EventLog::new();
    run_scenario(
        &mut context,
        "service_quickstart",
        |scenario_context| {
            Box::pin(async move {
                let base_url = scenario_context
                    .service_url("web")
                    .expect("URL after readiness");
                let client = reqwest::Client::new();
                let response = client.get(base_url).send().await.expect("GET /");
                assert!(response.status().is_success());
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}
```

### 2. Config injection and health checks

#### Convention-based env vars

When **[`auto_inject(true)`](src/service/builder.rs)** (the default), the harness adds variables derived from whatever is present on [`Context`](src/lifecycle/mod.rs) at container start time:

| Binding | Default env var | Notes |
|--------|-------------------|--------|
| Kafka | `KAFKA_BOOTSTRAP_SERVERS` | Requires `kafka` feature + registered Kafka resource |
| Postgres | `DATABASE_URL` | `postgres` feature; see §3 for **per-service** URLs when multiple services are registered |
| MariaDB | `MARIADB_URL` | `mariadb` feature |
| MySQL | `MYSQL_URL` | `mysql` feature |
| RabbitMQ | `RABBITMQ_URL` | `rabbitmq` feature |
| Redis / Valkey | `REDIS_URL` | `redis` or `valkey` feature (`EnvMapping::redis` renames the var) |
| MongoDB | `MONGODB_URI` | `mongodb` feature (`EnvMapping::mongo` renames the var) |
| NATS | `NATS_URL` | `nats` feature (`EnvMapping::nats` renames the var) |
| Neo4j | `NEO4J_URI` | `neo4j` feature (`EnvMapping::neo4j` renames the var) |
| DuckDB | `DUCKDB_URL` | `duckdb` feature |
| Elasticsearch | `ELASTICSEARCH_URL` | `elasticsearch` feature |
| OpenSearch | `OPENSEARCH_URL` | `opensearch` feature |
| Qdrant | `QDRANT_URL` | `qdrant` feature (gRPC URL) |
| Weaviate | `WEAVIATE_URL` | `weaviate` feature (REST) |
| InfluxDB 2.x | `INFLUXDB_URL` | `influxdb` feature; also sets `INFLUXDB_TOKEN`, `INFLUXDB_ORG`, `INFLUXDB_BUCKET` |
| TimescaleDB | `TIMESCALEDB_URL` | `timescaledb` feature (distinct from Postgres `DATABASE_URL`) |
| HTTP/gRPC mock service | `{UPPER_SNAKE(mock_service_name)}_URL` | e.g. mock service `"order-gateway"` → `ORDER_GATEWAY_URL` (`mock-services` + `services`) |
| `depends_on` peer | `{UPPER_SNAKE(service_name)}_URL` | Only for names listed in [`.depends_on`](src/service/builder.rs) (§3) |

**Overrides:** Pass [`.env_mapping`](src/service/builder.rs) with [`EnvMapping::kafka`](src/service/types.rs), [`postgres`](src/service/types.rs), [`mariadb`](src/service/types.rs), [`mysql`](src/service/types.rs), [`rabbitmq`](src/service/types.rs), [`redis`](src/service/types.rs), [`mongo`](src/service/types.rs), [`nats`](src/service/types.rs), [`neo4j`](src/service/types.rs), [`duckdb`](src/service/types.rs), [`elasticsearch`](src/service/types.rs), [`opensearch`](src/service/types.rs), [`qdrant`](src/service/types.rs), [`weaviate`](src/service/types.rs), [`influxdb`](src/service/types.rs), [`timescaledb`](src/service/types.rs), [`mock_service`](src/service/types.rs), or [`service`](src/service/types.rs) to rename the variable for that binding. **Explicit** [`.env(key, value)`](src/service/builder.rs) entries win over convention and mappings.

**Opt out:** [`.auto_inject(false)`](src/service/builder.rs) skips the table above; only `.env` and resolved `.env_mapping` entries are injected.

**Container vs test host:** Injected values rewrite loopback and `localhost` to **`host.docker.internal`** (and add Linux `host-gateway` mapping) so processes **inside** the service container reach Kafka, DBs, and mock services on the host. The test process keeps using **`127.0.0.1`** via [`service_url`](src/lifecycle/mod.rs) — see [`HOST_DOCKER_INTERNAL`](src/service/address.rs).

#### `PullPolicy`

[`.pull_policy`](src/service/builder.rs) controls image pull before start: [`PullPolicy::Always`](src/service/types.rs), [`IfNotPresent`](src/service/types.rs) (default, Docker-style), [`Never`](src/service/types.rs).

#### `HealthCheck` variants

Constructors live on [`HealthCheck`](src/service/health.rs): [`http`](src/service/health.rs), [`http_status`](src/service/health.rs), [`grpc`](src/service/health.rs), [`grpc_service`](src/service/health.rs), [`tcp`](src/service/health.rs), [`command`](src/service/health.rs), [`composite`](src/service/health.rs). Default poll budget is **30s** with **500ms** interval ([`DEFAULT_HEALTH_CHECK_TIMEOUT`](src/service/health.rs), [`DEFAULT_HEALTH_CHECK_INTERVAL`](src/service/health.rs)); tune with [`.with_timeout`](src/service/health.rs) / [`.with_interval`](src/service/health.rs).

**Implemented today for readiness:** **HTTP**, **TCP**, **Command** (container exec), and **Composite** (ordered nested checks). **`Grpc`** is accepted in configuration but **not yet implemented** as a probe — use **`http`** or **`tcp`** until gRPC health wiring lands.

```rust
use shakedown::HealthCheck;
use std::time::Duration;

let ready = HealthCheck::http("/health/ready")
    .with_timeout(Duration::from_secs(90));

let _tcp_fallback = HealthCheck::tcp().with_interval(Duration::from_millis(250));
```

### 3. Multi-service dependencies, inter-service URLs, and partitioned resources

Register **each** service with [`register_service`](src/lifecycle/mod.rs). Declare ordering with [`.depends_on("other_name")`](src/service/builder.rs); the harness performs a **topological sort** and rejects **cycles** or **missing** dependency names at registration time.

When **`auto_inject`** is on and more than one service is registered, **Postgres / MariaDB / MySQL** URLs use **per-service database path suffixes** so every container still receives the standard name (`DATABASE_URL` for Postgres, etc.) but points at an **isolated** database — same transparency model as other resources ([DECISIONS.md](../../DECISIONS.md) scenario isolation).

A downstream service that **depends on** an upstream receives **`UPSTREAM_URL`** (upper-snake of the dependency name) with the upstream’s base URL, rewritten for the container network. Services **not** listed in `depends_on` do **not** get each other’s URLs automatically.

```rust
use shakedown::{HealthCheck, ServiceUnderTest};

let upstream = ServiceUnderTest::docker("traefik/whoami:v1.10")
    .name("upstream")
    .port(80)
    .health_check(HealthCheck::http("/"))
    .build()
    .expect("upstream spec");

let downstream = ServiceUnderTest::docker("traefik/whoami:v1.10")
    .name("downstream")
    .port(80)
    .depends_on("upstream")
    .health_check(HealthCheck::http("/"))
    .build()
    .expect("downstream spec");

// Register `upstream` before `downstream` (or rely on validation errors if order is wrong).
```

Full Docker proof: [`tests/services_multi_postgres_whoami.rs`](tests/services_multi_postgres_whoami.rs) (`postgres`, `services`).

### 4. Diagnostics and resource assertions on service containers

Service containers are labeled like infrastructure: **`io.shakedown.framework=true`** plus **`io.shakedown.service={name}`** (see [`SHAKEDOWN_SERVICE_LABEL_KEY`](src/diagnostics/labels.rs) in [`docker_labels.rs`](src/docker_labels.rs)). Bollard **log streaming**, **Docker events**, and **resource stats** use the same filters as other Shakedown-managed containers when **`diagnostics`** is active (the `services` feature enables it unless you compile with **`no-diagnostics`**).

Use the **logical service name** you passed to [`.name`](src/service/builder.rs) with [`Context::assert_resource`](src/lifecycle/mod.rs) for **`peak_memory_under`** / **`avg_cpu_under`** — correlation prefers the `io.shakedown.service` label ([`correlation_name_from_container_summary`](src/diagnostics/labels.rs)). Deeper background: [Infrastructure diagnostics (bollard)](#infrastructure-diagnostics-bollard).

**End-to-end** exercise (Kafka + mock service + whoami + diagnostics): [`tests/services_kafka_mock_service_whoami.rs`](tests/services_kafka_mock_service_whoami.rs) (`kafka`, `services`, Docker + `--ignored`).

## See also

- **Where docs live** — workspace [`docs/README.md`](../../docs/README.md) explains how this repo’s notes relate to any separate product doc tree your team uses.
- **Pluggable outbox + queue** — background: treat the outbox as storage + dispatcher; Shakedown asserts both the row and the message observed on the bus (`PublicationVerifier`).
- **Verifier naming** — custom transports implement the same publication-verification trait shape as the built-in Kafka/Rabbit/NATS/Redis helpers.
