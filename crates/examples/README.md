# Shakedown examples (`shakedown-examples`)

Progressive **book-style** scenarios for learning Shakedown: order and fulfillment companion services, six parts (28 chapters), plus twelve resource appendices.

## Icon legend (table of contents)

| Icon | Meaning |
|------|---------|
| 🧱 | SQLite (no Docker) |
| 🦆 | DuckDB (no Docker) |
| 🐘 | Postgres |
| 🐬 | MariaDB / MySQL |
| ⏱️ | TimescaleDB |
| 💾 | Redis / Valkey |
| 📨 | Kafka |
| 🐰 | RabbitMQ |
| 🍃 | MongoDB |
| 🛰️ | NATS |
| 🔷 | Neo4j |
| 🔍 | Elasticsearch / OpenSearch |
| 📐 | Qdrant |
| 🔮 | Weaviate |
| 📈 | InfluxDB |
| 🧪 | HTTP / gRPC mock services |
| 🖥️ | `ServiceUnderTest` (example Docker images) |
| 💥 | Fault injection (Toxiproxy) |
| 📊 | Diagnostics (bollard stats) |
| 📡 | Cluster topology resource |
| ⚡ | Load framework (`load` feature) |

**Docker:** Chapter 1 and DuckDB appendices run without Docker. Every other chapter and appendix is `#[ignore]` by default — run with `just test-examples-*` and `--ignored` after `just build-example-services` where Parts II–VI need local images (`example-order-service:latest`, `example-fulfillment-service:latest`).

---

## Table of contents

### Part I — Foundations (infrastructure only)

| Ch | File | Summary | Resources |
|----|------|---------|-----------|
| 1 | `ch01_first_scenario` | Manual `run_scenario`, SQLite orders table, first assertions | 🧱 |
| 2 | `ch02_kafka` | Produce, `consume_one`, `consume_until`, payload checks | 📨 |
| 3 | `ch03_postgres` | Seed DDL, query, row assertions | 🐘 |
| 4 | `ch04_redis` | Cache keys: exists / equals / absent | 💾 |
| 5 | `ch05_messaging_rabbit` | Topology, publish, consume | 🐰 |
| 6 | `ch06_documents_mongo` | Documents: insert, find, assert count | 🍃 |
| 7 | `ch07_multi_resource` | Postgres + Redis + Kafka in one scenario | 🐘 💾 📨 |
| 8 | `ch08_mock_services` | HTTP + gRPC mocks, call assertions | 🧪 |

### Part II — One service under test (Order Service)

| Ch | File | Summary | Resources |
|----|------|---------|-----------|
| 9 | `ch09_service_basics` | `ServiceUnderTest`, health, Postgres after HTTP | 🖥️ 🐘 💾 📨 |
| 10 | `ch10_service_events` | Kafka events after `POST /orders` | 🖥️ 🐘 💾 📨 |
| 11 | `ch11_service_cache` | Redis cache + `GET` path | 🖥️ 🐘 💾 📨 |
| 12 | `ch12_service_outbox` | Outbox row + Kafka + Rabbit publication proof | 🖥️ 🐘 📨 🐰 + outbox |
| 13 | `ch13_service_mock_upstream` | Fulfillment URL mocked; path assertions | 🖥️ 🐘 💾 📨 🧪 |

### Part III — Two services (Order + Fulfillment)

| Ch | File | Summary | Resources |
|----|------|---------|-----------|
| 14 | `ch14_multi_service` | Inter-service flow, Postgres + Mongo | 🖥️ 🐘 🍃 📨 💾 |
| 15 | `ch15_cross_service_events` | Full chain: Postgres → Kafka → Mongo → Rabbit | 🖥️ 🐘 🍃 📨 🐰 💾 |
| 16 | `ch16_cross_service_mocks` | Payment mock + both databases | 🖥️ 🐘 🍃 📨 💾 🧪 |

### Part IV — Fault injection and diagnostics

| Ch | File | Summary | Resources |
|----|------|---------|-----------|
| 17 | `ch17_fault_injection` | `#[shakedown::faulty_test]`, Kafka latency toxic | 🖥️ 🐘 💾 📨 💥 |
| 18 | `ch18_diagnostics` | `peak_memory_under`, `avg_cpu_under`, failure timeline | 🖥️ 🐘 🍃 📨 💾 📊 |

### Part V — Cluster topologies

| Ch | File | Summary | Resources |
|----|------|---------|-----------|
| 19 | `ch19_mongo_replica_set` | Primary step-down, Mongo writes | 🖥️ 📡 🍃 📨 🐰 |
| 20 | `ch20_kafka_cluster` | Broker loss, leader election, consume proof | 🖥️ 📡 📨 🐘 💾 |
| 21 | `ch21_redis_sentinel` | Failover, cache reads | 🖥️ 📡 💾 🐘 📨 |
| 22 | `ch22_postgres_replica_set` | Promotion, reads on new primary | 🖥️ 📡 🐘 💾 📨 |
| 23 | `ch23_rabbit_cluster` | Quorum queue leader election | 🖥️ 📡 🐰 🍃 📨 |

### Part VI — Load, soak, resilience

| Ch | File | Summary | Resources |
|----|------|---------|-----------|
| 24 | `ch24_load_basics` | Warmup / ramp, `LoadMetricsAssertBuilder`, JSON report | 🖥️ 🐘 💾 📨 ⚡ |
| 25 | `ch25_load_windowed` | Phase windows, `p99_during`, `tail_ratio_under`, `drops_under` | 🖥️ 🐘 💾 📨 ⚡ |
| 26 | `ch26_soak` | Baselines, `no_regression_from`, drift, memory budget | 🖥️ 🐘 💾 📨 ⚡ 📊 |
| 27 | `ch27_disruption_under_load` | Mongo election under load, recovery SLOs | 🖥️ 📡 🍃 📨 🐰 ⚡ |
| 28 | `ch28_capstone` | Partition mesh, dual pass baseline, timeline | 🖥️ 📡 💥 🍃 📨 🐰 ⚡ 📊 |

### Appendices (reference cards)

| Appendix | File | Summary | Resources |
|----------|------|---------|-----------|
| A | `appendix_nats` | Pub/sub + outbox + `assert_nats_published` | 🛰️ 🧱 + outbox |
| B | `appendix_neo4j` | Cypher, graph assertions | 🔷 |
| C | `appendix_elasticsearch` | Index, search, hits | 🔍 |
| D | `appendix_opensearch` | Same pattern as ES | 🔍 |
| E | `appendix_qdrant` | Vectors, collection count | 📐 |
| F | `appendix_weaviate` | Class + object assertions | 🔮 |
| G | `appendix_influxdb` | Line protocol, `assert_point_exists` | 📈 |
| H | `appendix_timescaledb` | Hypertable + SQL assertions | ⏱️ |
| I | `appendix_duckdb` | Analytics SQL + full outbox row API | 🦆 + outbox |
| J | `appendix_mariadb` | Relational assertions | 🐬 |
| K | `appendix_mysql` | Relational assertions | 🐬 |
| L | `appendix_valkey` | Redis API + Redis Streams publication proof | 💾 🧱 + outbox |

---

## Assertion index

Each row lists the **crate API** surface and **where** it appears. Publication proof for buses is exposed as `Outbox::assert_*_published`, which uses the `PublicationVerifier` trait internally.

### 1. Data assertions (stores, documents, cache, outbox rows)

| API | Chapter(s) / appendix |
|-----|------------------------|
| `SqliteHelpers::assert_row_exists`, `assert_row_count` | ch01 |
| `PostgresHelpers::assert_row_exists`, `assert_row_count` | ch03, ch07, ch09, ch14–ch17, ch22, ch20 |
| `DuckDbHelpers::assert_row_exists`, `assert_row_count` | appendix_duckdb |
| `MariaDbHelpers::assert_row_exists`, `assert_row_count` | appendix_mariadb |
| `MysqlHelpers::assert_row_exists`, `assert_row_count` | appendix_mysql |
| `TimescaledbHelpers::assert_row_exists`, `assert_row_count`, `assert_hypertable_exists` | appendix_timescaledb |
| `RedisHelpers::assert_key_exists`, `assert_key_equals`, `assert_key_absent` | ch04, ch07, ch11, ch21 |
| `MongoHelpers::assert_document_exists`, `assert_document_count` | ch06, ch14–ch16, ch15, ch19, ch23 |
| `ElasticsearchHelpers` / `OpensearchHelpers`: `assert_document_indexed`, `assert_search_hits` | appendix_elasticsearch, appendix_opensearch |
| `QdrantHelpers::assert_point_exists`, `assert_collection_count` | appendix_qdrant |
| `WeaviateHelpers::assert_object_exists`, `assert_class_count` | appendix_weaviate |
| `InfluxDbHelpers::assert_point_exists` | appendix_influxdb |
| `Neo4jHelpers::assert_node_exists`, `assert_relationship_exists` | appendix_neo4j |
| `Outbox::setup` | ch12, appendices (duckdb, nats, valkey) |
| `Outbox::assert_pending`, `OutboxAssertBuilder::with_payload` | ch12, appendix_duckdb |
| `Outbox::assert_sent`, `assert_failed`, `assert_retry` | appendix_duckdb |
| `Outbox::assert_kafka_published` | ch12 |
| `Outbox::assert_rabbit_published` | ch12 |
| `Outbox::assert_nats_published` | appendix_nats |
| `Outbox::assert_redis_streams_published` | appendix_valkey |

### 2. Behavior assertions (mocks, messaging consumers, services)

| API | Chapter(s) / appendix |
|-----|------------------------|
| `MockServiceHandle::assert_called_times` | ch08 |
| `MockServiceHandle::assert_path_called_times` | ch08, ch13, ch16 |
| `MockServiceHandle::assert_called_with` | ch08, ch13 |
| `MockServiceHandle::assert_not_called` | ch08 |
| `MockServiceHandle::assert_path_not_called` | ch08 |
| `KafkaHelpers::consume_one`, `consume_until`, `produce` / `produce_json` (behavioral proof) | ch02, ch07, ch10, ch15, ch17, ch20 |
| `RabbitHelpers::consume_until`, `declare_topology`, `publish_json` | ch05, ch12, ch15, ch23, ch27, ch28 |
| `NatsHelpers::publish`, `subscribe_until` | appendix_nats |
| `ServiceHandle::is_healthy`, `Context::service_url` | ch09 |
| `run_scenario` + `EventLog` (orchestration for timeline) | ch01, ch18, ch28 |

### 3. Performance assertions (`LoadMetrics` + `LoadMetricsAssertBuilder`)

| API | Chapter(s) |
|-----|------------|
| `LoadMetrics::assert`, `assert_with_phase` | ch24–ch28 |
| `p99_under`, `p95_under`, `p50_under` | ch24, ch27, ch28 |
| `throughput_above` | ch25 |
| `error_rate_under` | ch24–ch28 |
| `no_drops` | ch24 |
| `drops_under` | ch25 |
| `recovery_under`, `error_burst_under` | ch27, ch28 |
| `tail_ratio_under` | ch25, ch28 |
| `no_regression_from` + `Tolerance` | ch26, ch28 |
| `LoadMetrics::save_baseline` | ch26, ch28 |
| `LoadMetricsAssertBuilder::store_for_report`, `print_summary_to_stderr` | ch24–ch28 |
| `LoadMetrics::write_report` | ch24–ch28 |
| `LoadHandle::metrics`, `await_phase`, `await_recovery_with_config` | ch24–ch28 |
| `LoadMetrics::p99_during`, `error_rate_during`, `throughput_during`, `p99_after` | ch25 |

### 4. Topology, load, and infrastructure assertions

| API | Chapter(s) |
|-----|------------|
| `MongoClusterOps::step_down_primary`, `await_new_primary`, `current_primary` | ch19, ch27, ch28 |
| `KafkaClusterOps::leader_for_partition`, `await_leader_election`, node stop/start (via cluster handle) | ch20 |
| `RedisSentinelOps::trigger_failover`, `await_failover_complete`, `current_master` | ch21 |
| `PostgresReplicaSetOps::promote_replica`, `current_primary`, binding helpers | ch22 |
| `RabbitClusterOps::quorum_queue_leader`, `await_queue_leader_election` | ch23 |
| `ClusterHandle::partition_nodes`, `reset_topology` | ch28 |
| `Context::assert_resource` → `ResourceAssertBuilder::peak_memory_under`, `avg_cpu_under` | ch18, ch26, ch28 |
| `FaultHandle::add_toxic`, `reset_proxy` (chaos on proxied Kafka) | ch17 |
| `render_failure_timeline` | ch18, ch28 |
| `RecoveryConfig` (rolling window + threshold) | ch27, ch28 |

---

## Workspace layout

| Path | Role |
|------|------|
| `src/lib.rs` | Shared DDL, sample payloads, domain types |
| `tests/ch*.rs` | Chapters (feature-gated integration tests) |
| `tests/appendix_*.rs` | Reference cards per engine |

## Quick start

```bash
# Non-Docker examples (ch01 + appendix_duckdb) — from repo root
just test-examples

# Build Order + Fulfillment images (Docker)
just build-example-services

# Ignored Docker-backed tests (all features)
just test-examples-ignored
```

## Just recipes (examples crate)

| Recipe | Purpose |
|--------|---------|
| `build-example-services` | Build both example service images |
| `test-examples` | ch01 + DuckDB appendices (no Docker) |
| `test-examples-ignored` | All `#[ignore]` targets with `--all-features` |
| `test-examples-part1` … `part6` | Parts I–VI with scoped features + filter |
| `test-examples-full` | Full examples suite (`--all-features -- --ignored`) |

## Related workspace crates

- **`example-order-service`** — Postgres, Redis, Kafka, optional outbox; HTTP API for orders.
- **`example-fulfillment-service`** — MongoDB, RabbitMQ, Kafka consumer; HTTP API for fulfillments.

Image names: `example-order-service:latest`, `example-fulfillment-service:latest`.

## Further reading

- Feature PRD / specs: `~/docs/shakedown/feature/mvp-examples/` (adopted docs tree).
- MVP row 9: `~/docs/shakedown/MVP.md`.
