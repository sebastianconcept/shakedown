//! Resource providers and registration for the Shakedown harness.
//!
//! What: [`Resource`], [`BuiltinResource`], [`RegisteredResource`], and helpers such as
//! [`crate::resources::ResourceRegistry`].
//!
//! Why: tests need pluggable databases, brokers, and mocks behind one async contract with scenario
//! isolation (`scenario_id` on provision).
//!
//! How: implement [`Resource`] for your system; register via [`crate::lifecycle::Context`] or
//! `#[shakedown::test(resources = [...])]` for builtins.

use crate::error::{InfrastructureError, ResourceHandle, ScenarioId};
use async_trait::async_trait;
use std::fmt;

/// Derive a safe, unique namespace from a scenario id for any resource type.
///
/// If you're implementing [`Resource`] for your own infrastructure (Redis, Elasticsearch,
/// RabbitMQ, etc.), call this in your `provision` method to get a scenario-scoped name you
/// can use as a key prefix, database name, virtual host, index name — whatever your system
/// uses for isolation. Every scenario gets its own namespace; teardown drops it.
///
/// The built-in resources (Kafka, RabbitMQ, Postgres, MariaDB, MySQL, SQLite, Redis, Valkey, MongoDB, NATS) use specialised variants
/// of this logic internally. This helper gives you the same safety guarantees without having
/// to reimplement the sanitisation rules:
///
/// - Prefixed with `shakedown_` so names are greppable and won't collide with real data.
/// - Only lowercase alphanumerics and `_` — safe for virtually any system identifier.
/// - Truncated to `max_len` (or 63 by default, matching PostgreSQL's limit).
/// - Falls back to `"scenario"` when the id is empty.
///
/// ```
/// # use shakedown::resources::sanitize_resource_namespace;
/// let ns = sanitize_resource_namespace("My Cool Scenario!", 63);
/// assert_eq!(ns, "shakedown_my_cool_scenario_");
/// assert!(ns.len() <= 63);
/// ```
pub fn sanitize_resource_namespace(scenario_id: &str, max_len: usize) -> String {
    const PREFIX: &str = "shakedown_";
    let mut rest = String::new();
    for ch in scenario_id.chars().take(200) {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            rest.push(ch.to_ascii_lowercase());
        } else {
            rest.push('_');
        }
    }
    if rest.is_empty() {
        rest.push_str("scenario");
    }
    let mut out = String::with_capacity(max_len.min(PREFIX.len() + rest.len()));
    out.push_str(PREFIX);
    out.push_str(&rest);
    if out.len() > max_len {
        out.truncate(max_len);
    }
    out
}

/// Turn a scenario id into a safe Kafka topic prefix segment.
///
/// Prefixes with `shakedown_`, replaces disallowed characters with `_`, and caps length so topic
/// names stay valid on typical brokers. Used by the Kafka builtin (`kafka` feature) when provisioning.
pub fn sanitize_kafka_topic_prefix(scenario_id: &str) -> String {
    let mut out = String::with_capacity(scenario_id.len().saturating_add(12));
    out.push_str("shakedown_");
    for ch in scenario_id.chars().take(200) {
        if ch.is_ascii_alphanumeric() || ch == '.' || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out.push('_');
    out
}

/// Turn a scenario id into a safe RabbitMQ exchange/queue name prefix segment.
///
/// Uses the same character rules as [`sanitize_kafka_topic_prefix`] so topic-style and AMQP
/// routing identifiers stay consistent across Kafka and Rabbit in one process.
pub fn sanitize_rabbit_resource_prefix(scenario_id: &str) -> String {
    sanitize_kafka_topic_prefix(scenario_id)
}

/// Turn a scenario id into a safe Redis / Valkey key prefix (`shakedown:<id>:`).
///
/// Uses `:` as the segment separator (RESP key convention). Only ASCII alphanumerics survive;
/// other characters become `_`. Empty ids map to `shakedown:scenario:`.
pub fn sanitize_redis_key_prefix(scenario_id: &str) -> String {
    let mut middle = String::new();
    for ch in scenario_id.chars().take(200) {
        if ch.is_ascii_alphanumeric() {
            middle.push(ch.to_ascii_lowercase());
        } else {
            middle.push('_');
        }
    }
    if middle.is_empty() {
        middle.push_str("scenario");
    }
    format!("shakedown:{middle}:")
}

/// Turn a scenario id into a safe NATS subject prefix (`shakedown.<id>.`).
///
/// Dots separate segments (NATS subject convention). Non-alphanumerics become `.`; empty ids map
/// to `shakedown.scenario.`.
pub fn sanitize_nats_subject_prefix(scenario_id: &str) -> String {
    let mut middle = String::new();
    for ch in scenario_id.chars().take(200) {
        if ch.is_ascii_alphanumeric() {
            middle.push(ch.to_ascii_lowercase());
        } else {
            middle.push('.');
        }
    }
    if middle.is_empty() {
        middle.push_str("scenario");
    }
    format!("shakedown.{middle}.")
}

/// Turn a scenario id into a safe PostgreSQL database name (`shakedown_…`, max 63 bytes).
///
/// Prefixes with `shakedown_`, lowercases alphanumerics, replaces other characters with `_`, and
/// truncates to PostgreSQL’s 63-byte identifier limit.
pub fn sanitize_sql_database_name(scenario_id: &str) -> String {
    const PREFIX: &str = "shakedown_";
    const MAX_LEN: usize = 63;
    let mut rest = String::new();
    for ch in scenario_id.chars().take(200) {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            rest.push(ch.to_ascii_lowercase());
        } else {
            rest.push('_');
        }
    }
    if rest.is_empty() {
        rest.push_str("scenario");
    }
    let mut out = String::with_capacity(MAX_LEN);
    out.push_str(PREFIX);
    out.push_str(&rest);
    if out.len() > MAX_LEN {
        out.truncate(MAX_LEN);
    }
    out
}

/// Scenario-scoped Elasticsearch / OpenSearch index name prefix (`shakedown_<id>_`, same
/// sanitization as [`sanitize_sql_database_name`] plus a trailing underscore for logical index names).
#[cfg(any(
    feature = "elasticsearch",
    feature = "opensearch",
    feature = "qdrant",
    feature = "weaviate"
))]
pub fn sanitize_elasticsearch_index_prefix(scenario_id: &str) -> String {
    format!("{}_", sanitize_sql_database_name(scenario_id))
}

/// Build a unique SQLite **named in-memory** URI for a scenario id.
///
/// Uses `file:<name>?mode=memory&cache=shared` so additional connections opened with the same URL
/// attach to one logical database; distinct scenario ids yield distinct names (isolation). The
/// harness keeps a primary [`rusqlite::Connection`] while [`crate::lifecycle::Context`] exposes the
/// same URI in [`SqliteBindings::database_url`] for user clients.
pub fn sqlite_named_memory_uri(scenario_id: &str) -> String {
    let mut safe = String::new();
    for ch in scenario_id.chars().take(120) {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            safe.push(ch.to_ascii_lowercase());
        } else {
            safe.push('_');
        }
    }
    if safe.is_empty() {
        safe.push_str("scenario");
    }
    format!("file:shakedown_{safe}?mode=memory&cache=shared")
}

/// Build a unique DuckDB database path for a scenario id.
///
/// DuckDB’s `:memory:` database is process-local and not reliably name-scoped across callers, so
/// Shakedown uses a **temp file path** to guarantee isolation between scenarios and tests.
///
/// The harness keeps a primary connection while [`crate::lifecycle::Context`] exposes the same
/// string in [`DuckDbBindings::database_url`] for helpers and service injection.
pub fn duckdb_named_memory_database_path(scenario_id: &str) -> String {
    let safe = sanitize_resource_namespace(scenario_id, 63);
    let pid = std::process::id();
    let mut path = std::env::temp_dir();
    path.push(format!("{safe}_{pid}.duckdb"));
    path.to_string_lossy().to_string()
}

/// Async resource contract — implement this trait to add a new infrastructure type.
///
/// Methods are async because provisioning (especially containers) involves I/O.
/// The `scenario_id` parameter enables logical isolation (e.g., topic prefix for Kafka).
///
/// `async_trait` is used here so third-party crates can use `Box<dyn Resource>` via
/// [`RegisteredResource::Extension`].
#[async_trait]
pub trait Resource: Send + Sync {
    /// Provision phase: create isolated state and resources for a scenario.
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError>;

    /// Readiness check: verify the resource is ready to use (beyond TCP).
    async fn check_ready(&self) -> Result<(), InfrastructureError>;

    /// Apply project default seed (migrations, reference data, baseline topics).
    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    /// Teardown: clean up scenario state, drop connections.
    async fn teardown(&mut self) -> Result<(), InfrastructureError>;
}

/// First-party resources as a closed enum for efficient internal dispatch.
#[allow(clippy::large_enum_variant)] // Kafka holds a container handle; boxing would diverge from SPECS §1a.
#[derive(Debug)]
pub enum BuiltinResource {
    /// Fake resource for non-Docker tests (unit test path)
    Fake(FakeResource),

    #[cfg(feature = "kafka")]
    /// Docker-backed Kafka (KRaft, apache image via testcontainers-modules).
    Kafka(KafkaResource),

    #[cfg(feature = "rabbitmq")]
    /// Docker-backed RabbitMQ (official image via testcontainers-modules).
    Rabbit(RabbitResource),

    #[cfg(feature = "postgres")]
    /// Docker-backed PostgreSQL (suite-shared container via testcontainers-modules).
    Postgres(PostgresResource),

    #[cfg(feature = "postgres-cluster")]
    /// Docker-backed PostgreSQL primary + streaming replica (per-scenario containers; US-014).
    PostgresReplicaSet(crate::PostgresReplicaSetResource),

    #[cfg(feature = "mariadb")]
    /// Docker-backed MariaDB (suite-shared container via testcontainers-modules).
    MariaDB(MariaDBResource),

    #[cfg(feature = "mysql")]
    /// Docker-backed Oracle MySQL (suite-shared container via testcontainers-modules).
    Mysql(MysqlResource),

    #[cfg(feature = "sqlite")]
    /// In-memory SQLite per scenario (no Docker; [`SqliteResource`]).
    Sqlite(SqliteResource),

    #[cfg(feature = "duckdb")]
    /// In-process DuckDB per scenario (no Docker; [`DuckDbResource`]).
    DuckDb(DuckDbResource),

    #[cfg(feature = "fault-injection")]
    /// Docker-backed Toxiproxy (suite-shared container via [`ToxiproxyResource`]).
    Toxiproxy(ToxiproxyResource),

    #[cfg(feature = "redis")]
    /// Docker-backed Redis (official image via testcontainers-modules; suite-shared container).
    Redis(RedisResource),

    #[cfg(feature = "valkey")]
    /// Docker-backed Valkey (official image via testcontainers-modules; suite-shared container).
    Valkey(ValkeyResource),

    #[cfg(feature = "mongodb")]
    /// Docker-backed MongoDB (official image via testcontainers-modules; suite-shared `mongod`).
    Mongo(MongoResource),

    #[cfg(feature = "mongodb-cluster")]
    /// Docker-backed MongoDB replica set (multiple `mongod` containers on a suite bridge network).
    MongoReplicaSet(crate::MongoReplicaSetResource),

    #[cfg(feature = "kafka-cluster")]
    /// Docker-backed multi-broker KRaft Kafka cluster (shared bridge network; US-010).
    KafkaCluster(crate::KafkaClusterResource),

    #[cfg(feature = "rabbitmq-cluster")]
    /// Docker-backed RabbitMQ Erlang cluster (shared bridge network; quorum default queue type; US-016).
    RabbitCluster(crate::RabbitClusterResource),

    #[cfg(feature = "redis-sentinel")]
    /// Docker-backed Redis primary/replicas + Sentinel quorum (shared bridge network; US-012).
    RedisSentinel(crate::RedisSentinelResource),

    #[cfg(feature = "nats")]
    /// Docker-backed NATS (official image via testcontainers-modules; suite-shared server; core pub/sub only).
    Nats(NatsResource),

    #[cfg(feature = "neo4j")]
    /// Docker-backed Neo4j 5.x (suite-shared server; per-scenario database on Enterprise).
    Neo4j(Neo4jResource),

    #[cfg(feature = "elasticsearch")]
    /// Docker-backed Elasticsearch (suite-shared node; per-scenario index prefix).
    Elasticsearch(ElasticsearchResource),

    #[cfg(feature = "opensearch")]
    /// Docker-backed OpenSearch (suite-shared node; per-scenario index prefix).
    OpenSearch(OpenSearchResource),

    #[cfg(feature = "qdrant")]
    /// Docker-backed Qdrant (suite-shared node; per-scenario collection name prefix).
    Qdrant(QdrantResource),

    #[cfg(feature = "weaviate")]
    /// Docker-backed Weaviate (suite-shared node; per-scenario class name prefix).
    Weaviate(WeaviateResource),

    #[cfg(feature = "influxdb")]
    /// Docker-backed InfluxDB 2.x (suite-shared node; per-scenario bucket).
    InfluxDb(InfluxDbResource),

    #[cfg(feature = "timescaledb")]
    /// Docker-backed TimescaleDB (suite-shared node; per-scenario database; Postgres wire protocol).
    Timescaledb(TimescaledbResource),
}

#[async_trait]
impl Resource for BuiltinResource {
    async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        match self {
            BuiltinResource::Fake(r) => r.provision(scenario_id).await,
            #[cfg(feature = "kafka")]
            BuiltinResource::Kafka(r) => r.provision(scenario_id).await,
            #[cfg(feature = "rabbitmq")]
            BuiltinResource::Rabbit(r) => r.provision(scenario_id).await,
            #[cfg(feature = "postgres")]
            BuiltinResource::Postgres(r) => r.provision(scenario_id).await,
            #[cfg(feature = "postgres-cluster")]
            BuiltinResource::PostgresReplicaSet(r) => r.provision(scenario_id).await,
            #[cfg(feature = "mariadb")]
            BuiltinResource::MariaDB(r) => r.provision(scenario_id).await,
            #[cfg(feature = "mysql")]
            BuiltinResource::Mysql(r) => r.provision(scenario_id).await,
            #[cfg(feature = "sqlite")]
            BuiltinResource::Sqlite(r) => r.provision(scenario_id).await,
            #[cfg(feature = "duckdb")]
            BuiltinResource::DuckDb(r) => r.provision(scenario_id).await,
            #[cfg(feature = "fault-injection")]
            BuiltinResource::Toxiproxy(r) => r.provision(scenario_id).await,
            #[cfg(feature = "redis")]
            BuiltinResource::Redis(r) => r.provision(scenario_id).await,
            #[cfg(feature = "valkey")]
            BuiltinResource::Valkey(r) => r.provision(scenario_id).await,
            #[cfg(feature = "mongodb")]
            BuiltinResource::Mongo(r) => r.provision(scenario_id).await,
            #[cfg(feature = "mongodb-cluster")]
            BuiltinResource::MongoReplicaSet(r) => r.provision(scenario_id).await,
            #[cfg(feature = "kafka-cluster")]
            BuiltinResource::KafkaCluster(r) => r.provision(scenario_id).await,
            #[cfg(feature = "rabbitmq-cluster")]
            BuiltinResource::RabbitCluster(r) => r.provision(scenario_id).await,
            #[cfg(feature = "redis-sentinel")]
            BuiltinResource::RedisSentinel(r) => r.provision(scenario_id).await,
            #[cfg(feature = "nats")]
            BuiltinResource::Nats(r) => r.provision(scenario_id).await,
            #[cfg(feature = "neo4j")]
            BuiltinResource::Neo4j(r) => r.provision(scenario_id).await,
            #[cfg(feature = "elasticsearch")]
            BuiltinResource::Elasticsearch(r) => r.provision(scenario_id).await,
            #[cfg(feature = "opensearch")]
            BuiltinResource::OpenSearch(r) => r.provision(scenario_id).await,
            #[cfg(feature = "qdrant")]
            BuiltinResource::Qdrant(r) => r.provision(scenario_id).await,
            #[cfg(feature = "weaviate")]
            BuiltinResource::Weaviate(r) => r.provision(scenario_id).await,
            #[cfg(feature = "influxdb")]
            BuiltinResource::InfluxDb(r) => r.provision(scenario_id).await,
            #[cfg(feature = "timescaledb")]
            BuiltinResource::Timescaledb(r) => r.provision(scenario_id).await,
        }
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        match self {
            BuiltinResource::Fake(r) => r.check_ready().await,
            #[cfg(feature = "kafka")]
            BuiltinResource::Kafka(r) => r.check_ready().await,
            #[cfg(feature = "rabbitmq")]
            BuiltinResource::Rabbit(r) => r.check_ready().await,
            #[cfg(feature = "postgres")]
            BuiltinResource::Postgres(r) => r.check_ready().await,
            #[cfg(feature = "postgres-cluster")]
            BuiltinResource::PostgresReplicaSet(r) => r.check_ready().await,
            #[cfg(feature = "mariadb")]
            BuiltinResource::MariaDB(r) => r.check_ready().await,
            #[cfg(feature = "mysql")]
            BuiltinResource::Mysql(r) => r.check_ready().await,
            #[cfg(feature = "sqlite")]
            BuiltinResource::Sqlite(r) => r.check_ready().await,
            #[cfg(feature = "duckdb")]
            BuiltinResource::DuckDb(r) => r.check_ready().await,
            #[cfg(feature = "fault-injection")]
            BuiltinResource::Toxiproxy(r) => r.check_ready().await,
            #[cfg(feature = "redis")]
            BuiltinResource::Redis(r) => r.check_ready().await,
            #[cfg(feature = "valkey")]
            BuiltinResource::Valkey(r) => r.check_ready().await,
            #[cfg(feature = "mongodb")]
            BuiltinResource::Mongo(r) => r.check_ready().await,
            #[cfg(feature = "mongodb-cluster")]
            BuiltinResource::MongoReplicaSet(r) => r.check_ready().await,
            #[cfg(feature = "kafka-cluster")]
            BuiltinResource::KafkaCluster(r) => r.check_ready().await,
            #[cfg(feature = "rabbitmq-cluster")]
            BuiltinResource::RabbitCluster(r) => r.check_ready().await,
            #[cfg(feature = "redis-sentinel")]
            BuiltinResource::RedisSentinel(r) => r.check_ready().await,
            #[cfg(feature = "nats")]
            BuiltinResource::Nats(r) => r.check_ready().await,
            #[cfg(feature = "neo4j")]
            BuiltinResource::Neo4j(r) => r.check_ready().await,
            #[cfg(feature = "elasticsearch")]
            BuiltinResource::Elasticsearch(r) => r.check_ready().await,
            #[cfg(feature = "opensearch")]
            BuiltinResource::OpenSearch(r) => r.check_ready().await,
            #[cfg(feature = "qdrant")]
            BuiltinResource::Qdrant(r) => r.check_ready().await,
            #[cfg(feature = "weaviate")]
            BuiltinResource::Weaviate(r) => r.check_ready().await,
            #[cfg(feature = "influxdb")]
            BuiltinResource::InfluxDb(r) => r.check_ready().await,
            #[cfg(feature = "timescaledb")]
            BuiltinResource::Timescaledb(r) => r.check_ready().await,
        }
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        match self {
            BuiltinResource::Fake(r) => r.apply_default_seed().await,
            #[cfg(feature = "kafka")]
            BuiltinResource::Kafka(r) => r.apply_default_seed().await,
            #[cfg(feature = "rabbitmq")]
            BuiltinResource::Rabbit(r) => r.apply_default_seed().await,
            #[cfg(feature = "postgres")]
            BuiltinResource::Postgres(r) => r.apply_default_seed().await,
            #[cfg(feature = "postgres-cluster")]
            BuiltinResource::PostgresReplicaSet(r) => r.apply_default_seed().await,
            #[cfg(feature = "mariadb")]
            BuiltinResource::MariaDB(r) => r.apply_default_seed().await,
            #[cfg(feature = "mysql")]
            BuiltinResource::Mysql(r) => r.apply_default_seed().await,
            #[cfg(feature = "sqlite")]
            BuiltinResource::Sqlite(r) => r.apply_default_seed().await,
            #[cfg(feature = "duckdb")]
            BuiltinResource::DuckDb(r) => r.apply_default_seed().await,
            #[cfg(feature = "fault-injection")]
            BuiltinResource::Toxiproxy(r) => r.apply_default_seed().await,
            #[cfg(feature = "redis")]
            BuiltinResource::Redis(r) => r.apply_default_seed().await,
            #[cfg(feature = "valkey")]
            BuiltinResource::Valkey(r) => r.apply_default_seed().await,
            #[cfg(feature = "mongodb")]
            BuiltinResource::Mongo(r) => r.apply_default_seed().await,
            #[cfg(feature = "mongodb-cluster")]
            BuiltinResource::MongoReplicaSet(r) => r.apply_default_seed().await,
            #[cfg(feature = "kafka-cluster")]
            BuiltinResource::KafkaCluster(r) => r.apply_default_seed().await,
            #[cfg(feature = "rabbitmq-cluster")]
            BuiltinResource::RabbitCluster(r) => r.apply_default_seed().await,
            #[cfg(feature = "redis-sentinel")]
            BuiltinResource::RedisSentinel(r) => r.apply_default_seed().await,
            #[cfg(feature = "nats")]
            BuiltinResource::Nats(r) => r.apply_default_seed().await,
            #[cfg(feature = "neo4j")]
            BuiltinResource::Neo4j(r) => r.apply_default_seed().await,
            #[cfg(feature = "elasticsearch")]
            BuiltinResource::Elasticsearch(r) => r.apply_default_seed().await,
            #[cfg(feature = "opensearch")]
            BuiltinResource::OpenSearch(r) => r.apply_default_seed().await,
            #[cfg(feature = "qdrant")]
            BuiltinResource::Qdrant(r) => r.apply_default_seed().await,
            #[cfg(feature = "weaviate")]
            BuiltinResource::Weaviate(r) => r.apply_default_seed().await,
            #[cfg(feature = "influxdb")]
            BuiltinResource::InfluxDb(r) => r.apply_default_seed().await,
            #[cfg(feature = "timescaledb")]
            BuiltinResource::Timescaledb(r) => r.apply_default_seed().await,
        }
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        match self {
            BuiltinResource::Fake(r) => r.teardown().await,
            #[cfg(feature = "kafka")]
            BuiltinResource::Kafka(r) => r.teardown().await,
            #[cfg(feature = "rabbitmq")]
            BuiltinResource::Rabbit(r) => r.teardown().await,
            #[cfg(feature = "postgres")]
            BuiltinResource::Postgres(r) => r.teardown().await,
            #[cfg(feature = "postgres-cluster")]
            BuiltinResource::PostgresReplicaSet(r) => r.teardown().await,
            #[cfg(feature = "mariadb")]
            BuiltinResource::MariaDB(r) => r.teardown().await,
            #[cfg(feature = "mysql")]
            BuiltinResource::Mysql(r) => r.teardown().await,
            #[cfg(feature = "sqlite")]
            BuiltinResource::Sqlite(r) => r.teardown().await,
            #[cfg(feature = "duckdb")]
            BuiltinResource::DuckDb(r) => r.teardown().await,
            #[cfg(feature = "fault-injection")]
            BuiltinResource::Toxiproxy(r) => r.teardown().await,
            #[cfg(feature = "redis")]
            BuiltinResource::Redis(r) => r.teardown().await,
            #[cfg(feature = "valkey")]
            BuiltinResource::Valkey(r) => r.teardown().await,
            #[cfg(feature = "mongodb")]
            BuiltinResource::Mongo(r) => r.teardown().await,
            #[cfg(feature = "mongodb-cluster")]
            BuiltinResource::MongoReplicaSet(r) => r.teardown().await,
            #[cfg(feature = "kafka-cluster")]
            BuiltinResource::KafkaCluster(r) => r.teardown().await,
            #[cfg(feature = "rabbitmq-cluster")]
            BuiltinResource::RabbitCluster(r) => r.teardown().await,
            #[cfg(feature = "redis-sentinel")]
            BuiltinResource::RedisSentinel(r) => r.teardown().await,
            #[cfg(feature = "nats")]
            BuiltinResource::Nats(r) => r.teardown().await,
            #[cfg(feature = "neo4j")]
            BuiltinResource::Neo4j(r) => r.teardown().await,
            #[cfg(feature = "elasticsearch")]
            BuiltinResource::Elasticsearch(r) => r.teardown().await,
            #[cfg(feature = "opensearch")]
            BuiltinResource::OpenSearch(r) => r.teardown().await,
            #[cfg(feature = "qdrant")]
            BuiltinResource::Qdrant(r) => r.teardown().await,
            #[cfg(feature = "weaviate")]
            BuiltinResource::Weaviate(r) => r.teardown().await,
            #[cfg(feature = "influxdb")]
            BuiltinResource::InfluxDb(r) => r.teardown().await,
            #[cfg(feature = "timescaledb")]
            BuiltinResource::Timescaledb(r) => r.teardown().await,
        }
    }
}

/// Stub [`Resource`] with no network or Docker I/O (always succeeds).
///
/// Exists so unit tests and CI jobs exercise the lifecycle without Testcontainers. Prefer
/// `#[shakedown::test(resources = [Fake])]` or [`crate::lifecycle::Context::register_builtin`] with
/// [`BuiltinResource::Fake`].
#[derive(Clone, Debug)]
pub struct FakeResource {
    _priv: std::marker::PhantomData<()>,
}

impl Default for FakeResource {
    fn default() -> Self {
        Self {
            _priv: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl Resource for FakeResource {
    async fn provision(
        &mut self,
        _scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        Ok(vec![ResourceHandle {
            id: "fake_resource".to_string(),
            connection_string: "://localhost/fake".to_string(),
            metadata: None,
        }])
    }

    async fn check_ready(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        Ok(())
    }
}

#[cfg(feature = "kafka")]
mod kafka_impl {
    use super::{sanitize_kafka_topic_prefix, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, DefaultConsumerContext};
    use rdkafka::types::RDKafkaErrorCode;
    use std::sync::OnceLock;
    use std::time::Duration;
    use testcontainers::core::Image;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};
    use tokio::sync::Mutex;

    /// Suite-wide broker: one Docker container per test process, shared by all [`KafkaResource`]
    /// instances until the last holder tears down (D-3).
    #[allow(clippy::large_enum_variant)] // Running holds ContainerAsync; boxing would complicate rm() handoff.
    enum SuiteBrokerState {
        Idle,
        Running {
            bootstrap: String,
            container: testcontainers::ContainerAsync<apache::Kafka>,
            refs: usize,
            /// Effective `image:tag` for mismatch detection across concurrent scenarios.
            image_key: String,
        },
    }

    static SUITE_BROKER: OnceLock<Mutex<SuiteBrokerState>> = OnceLock::new();

    fn suite_broker_mutex() -> &'static Mutex<SuiteBrokerState> {
        SUITE_BROKER.get_or_init(|| Mutex::new(SuiteBrokerState::Idle))
    }

    fn image_fingerprint(image: &apache::Kafka, tag_override: &Option<String>) -> String {
        let tag = tag_override.as_deref().unwrap_or_else(|| image.tag());
        format!("{}:{}", image.name(), tag)
    }

    /// Docker-backed Kafka using testcontainers-modules KRaft image (`apache/kafka-native` by default).
    ///
    /// **Suite lifecycle (D-3):** the first provision in the process starts a single KRaft broker;
    /// further [`KafkaResource`] instances reuse it. The last successful [`Resource::teardown`]
    /// removes the container. All instances in one process must use the same image name and tag
    /// (first configuration wins).
    #[derive(Debug)]
    pub struct KafkaResource {
        image: apache::Kafka,
        /// Optional image tag override (image name stays from [`apache::Kafka`]).
        image_tag: Option<String>,
        /// `true` after this instance acquired a reference to the suite broker.
        holds_suite_ref: bool,
        bootstrap_servers: Option<String>,
        topic_prefix: String,
    }

    impl Default for KafkaResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl KafkaResource {
        /// Create default Docker-backed Kafka settings (suite broker started on first provision).
        pub fn new() -> Self {
            Self {
                image: apache::Kafka::default(),
                image_tag: None,
                holds_suite_ref: false,
                bootstrap_servers: None,
                topic_prefix: String::new(),
            }
        }

        /// Override the image tag (e.g. pin `3.8.0` for production parity). Name remains `apache/kafka-native` / JVM variant from [`apache::Kafka`].
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Use JVM image (`apache/kafka`) instead of native.
        pub fn with_jvm_image(mut self) -> Self {
            self.image = apache::Kafka::default().with_jvm_image();
            self
        }

        /// Apply tag override from `SHAKEDOWN_KAFKA_IMAGE_TAG` when set (suite-level parity with CI/prod pins).
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_KAFKA_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(e: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }
    }

    #[async_trait]
    impl Resource for KafkaResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("kafka".into()));
            }

            self.topic_prefix = sanitize_kafka_topic_prefix(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_broker_mutex().lock().await;
            let bootstrap = match &mut *guard {
                SuiteBrokerState::Idle => {
                    use testcontainers::core::ImageExt;
                    let image = self.image.clone();
                    let node = if let Some(ref tag) = self.image_tag {
                        image
                            .with_tag(tag.clone())
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    };

                    let port = node
                        .get_host_port_ipv4(KAFKA_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let bootstrap = format!("127.0.0.1:{port}");
                    *guard = SuiteBrokerState::Running {
                        bootstrap: bootstrap.clone(),
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    bootstrap
                }
                SuiteBrokerState::Running {
                    bootstrap,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "Kafka suite broker already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    bootstrap.clone()
                }
            };
            drop(guard);

            self.bootstrap_servers = Some(bootstrap.clone());
            self.holds_suite_ref = true;

            Ok(vec![ResourceHandle {
                id: format!("kafka_{}", scenario_id),
                connection_string: bootstrap.clone(),
                metadata: Some(serde_json::json!({
                    "bootstrap_servers": bootstrap,
                    "topic_prefix": self.topic_prefix,
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let bootstrap = self
                .bootstrap_servers
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("kafka".into()))?;

            let consumer: rdkafka::consumer::BaseConsumer<DefaultConsumerContext> =
                ClientConfig::new()
                    .set("bootstrap.servers", bootstrap)
                    .set("client.id", "shakedown_ready_probe")
                    .set("socket.timeout.ms", "5000")
                    .create()
                    .map_err(|e| InfrastructureError::Readiness {
                        resource: "kafka".into(),
                        source: Box::new(std::io::Error::other(e.to_string())),
                    })?;

            const ATTEMPTS: u32 = 30;
            for i in 0..ATTEMPTS {
                match consumer.fetch_metadata(None, Duration::from_secs(3)) {
                    Ok(meta) if !meta.brokers().is_empty() => return Ok(()),
                    Ok(_) => {}
                    Err(e) => {
                        if i + 1 == ATTEMPTS {
                            return Err(InfrastructureError::Readiness {
                                resource: "kafka".into(),
                                source: Box::new(std::io::Error::other(e.to_string())),
                            });
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            Err(InfrastructureError::Readiness {
                resource: "kafka".into(),
                source: Box::new(std::io::Error::other(
                    "Kafka metadata fetch did not succeed in time",
                )),
            })
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            let bootstrap = self
                .bootstrap_servers
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("kafka".into()))?;

            let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
                .set("bootstrap.servers", bootstrap)
                .set("client.id", "shakedown_seed")
                .create()
                .map_err(|e| {
                    InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
                })?;

            let proof_topic = format!("{}shakedown.proof", self.topic_prefix);
            let new_topics = [NewTopic::new(&proof_topic, 1, TopicReplication::Fixed(1))];

            let results = admin
                .create_topics(&new_topics, &AdminOptions::new())
                .await
                .map_err(|e| {
                    InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
                })?;

            for r in results {
                if let Err((_, code)) = r {
                    if code != RDKafkaErrorCode::TopicAlreadyExists {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!("create topic: {code:?}")),
                        )));
                    }
                }
            }

            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.bootstrap_servers = None;
                return Ok(());
            }

            let mut guard = suite_broker_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<apache::Kafka>> = None;
            match &mut *guard {
                SuiteBrokerState::Idle => {}
                SuiteBrokerState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteBrokerState::Idle);
                        if let SuiteBrokerState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(c) = container_to_rm {
                c.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.bootstrap_servers = None;
            Ok(())
        }
    }
}

#[cfg(feature = "rabbitmq")]
mod rabbit_impl {
    use super::{sanitize_rabbit_resource_prefix, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use lapin::{types::ShortString, Connection, ConnectionProperties};
    use std::sync::OnceLock;
    use testcontainers::core::Image;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::rabbitmq::RabbitMq;
    use tokio::sync::Mutex;

    const RABBITMQ_AMQP_PORT: u16 = 5672;

    #[allow(clippy::large_enum_variant)]
    enum SuiteBrokerState {
        Idle,
        Running {
            host: String,
            port: u16,
            container: testcontainers::ContainerAsync<RabbitMq>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_BROKER: OnceLock<Mutex<SuiteBrokerState>> = OnceLock::new();

    fn suite_broker_mutex() -> &'static Mutex<SuiteBrokerState> {
        SUITE_BROKER.get_or_init(|| Mutex::new(SuiteBrokerState::Idle))
    }

    fn image_fingerprint(image: &RabbitMq, tag_override: &Option<String>) -> String {
        let tag = tag_override.as_deref().unwrap_or_else(|| image.tag());
        format!("{}:{}", image.name(), tag)
    }

    /// Docker-backed RabbitMQ using the official image via testcontainers-modules.
    ///
    /// **Suite lifecycle:** the first provision in the process starts a single broker; further
    /// [`RabbitResource`] instances reuse it. The last successful [`Resource::teardown`] removes the
    /// container when the refcount reaches zero.
    #[derive(Debug)]
    pub struct RabbitResource {
        image: RabbitMq,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        username: String,
        password: String,
        vhost: String,
        amqp_uri: Option<String>,
        resource_prefix: String,
    }

    impl Default for RabbitResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl RabbitResource {
        /// Create default Docker-backed RabbitMQ settings (suite broker started on first provision).
        pub fn new() -> Self {
            Self {
                image: RabbitMq::default(),
                image_tag: None,
                holds_suite_ref: false,
                host: None,
                port: None,
                username: "guest".to_string(),
                password: "guest".to_string(),
                vhost: "/".to_string(),
                amqp_uri: None,
                resource_prefix: String::new(),
            }
        }

        /// Override the image tag (e.g. pin a management image tag for parity). Name stays `rabbitmq`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_RABBITMQ_IMAGE_TAG` when set.
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_RABBITMQ_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(e: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }

        fn build_amqp_uri(host: &str, port: u16, username: &str, password: &str) -> String {
            format!("amqp://{username}:{password}@{host}:{port}")
        }
    }

    #[async_trait]
    impl Resource for RabbitResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("rabbitmq".into()));
            }

            self.resource_prefix = sanitize_rabbit_resource_prefix(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_broker_mutex().lock().await;
            let (host, port) = match &mut *guard {
                SuiteBrokerState::Idle => {
                    use testcontainers::core::ImageExt;
                    let image = self.image.clone();
                    let node = if let Some(ref tag) = self.image_tag {
                        image
                            .with_tag(tag.clone())
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    };

                    let mapped_port = node
                        .get_host_port_ipv4(RABBITMQ_AMQP_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();
                    *guard = SuiteBrokerState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port)
                }
                SuiteBrokerState::Running {
                    host,
                    port,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "RabbitMQ suite broker already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port)
                }
            };
            drop(guard);

            let amqp_uri = Self::build_amqp_uri(&host, port, &self.username, &self.password);
            self.host = Some(host.clone());
            self.port = Some(port);
            self.amqp_uri = Some(amqp_uri.clone());
            self.holds_suite_ref = true;

            Ok(vec![ResourceHandle {
                id: format!("rabbitmq_{scenario_id}"),
                connection_string: amqp_uri.clone(),
                metadata: Some(serde_json::json!({
                    "engine": "rabbitmq",
                    "amqp_uri": amqp_uri,
                    "host": host,
                    "port": port,
                    "username": self.username,
                    "password": self.password,
                    "vhost": self.vhost,
                    "resource_prefix": self.resource_prefix,
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let uri = self
                .amqp_uri
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("rabbitmq".into()))?;

            const ATTEMPTS: u32 = 30;
            for attempt in 0..ATTEMPTS {
                match Connection::connect(uri, ConnectionProperties::default()).await {
                    Ok(conn) => match conn.create_channel().await {
                        Ok(_) => {
                            let _ = conn.close(200, ShortString::from("shakedown_ready")).await;
                            return Ok(());
                        }
                        Err(error) if attempt + 1 == ATTEMPTS => {
                            return Err(InfrastructureError::Readiness {
                                resource: "rabbitmq".into(),
                                source: Box::new(std::io::Error::other(error.to_string())),
                            });
                        }
                        Err(_) => {}
                    },
                    Err(error) if attempt + 1 == ATTEMPTS => {
                        return Err(InfrastructureError::Readiness {
                            resource: "rabbitmq".into(),
                            source: Box::new(std::io::Error::other(error.to_string())),
                        });
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }

            Err(InfrastructureError::Readiness {
                resource: "rabbitmq".into(),
                source: Box::new(std::io::Error::other(
                    "RabbitMQ AMQP connection did not succeed in time",
                )),
            })
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.amqp_uri = None;
                self.resource_prefix.clear();
                return Ok(());
            }

            let mut guard = suite_broker_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<RabbitMq>> = None;
            match &mut *guard {
                SuiteBrokerState::Idle => {}
                SuiteBrokerState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteBrokerState::Idle);
                        if let SuiteBrokerState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(c) = container_to_rm {
                c.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.amqp_uri = None;
            self.resource_prefix.clear();
            Ok(())
        }
    }
}

#[cfg(any(feature = "redis", feature = "valkey"))]
mod resp_family {
    use super::{sanitize_redis_key_prefix, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use testcontainers::core::Image;
    use testcontainers::core::ImageExt;
    use testcontainers::runners::AsyncRunner;
    use tokio::sync::Mutex;

    const RESP_PORT: u16 = 6379;

    #[allow(clippy::large_enum_variant)]
    pub(crate) enum SuiteState<I: Image> {
        Idle,
        Running {
            host: String,
            port: u16,
            container: testcontainers::ContainerAsync<I>,
            refs: usize,
            image_key: String,
        },
    }

    /// Suite-shared RESP server (Redis or Valkey image) with per-scenario key prefixes.
    pub struct RespResource<I>
    where
        I: Image + Clone + Send + Sync + std::fmt::Debug + 'static,
    {
        pub image: I,
        pub image_tag: Option<String>,
        pub suite: &'static Mutex<SuiteState<I>>,
        pub engine: &'static str,
        pub env_tag_var: &'static str,
        /// When set and `image_tag` is unset, use this instead of [`Image::tag`] (pins Redis to `7`).
        pub hardcoded_default_tag: Option<&'static str>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        connection_url: Option<String>,
        key_prefix: String,
    }

    impl<I: Image + Clone + Send + Sync + std::fmt::Debug + 'static> std::fmt::Debug
        for RespResource<I>
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RespResource")
                .field("engine", &self.engine)
                .field("image_tag", &self.image_tag)
                .finish_non_exhaustive()
        }
    }

    impl<I: Image + Clone + Send + Sync + std::fmt::Debug + 'static> RespResource<I> {
        pub(crate) fn new(
            image: I,
            suite: &'static Mutex<SuiteState<I>>,
            engine: &'static str,
            env_tag_var: &'static str,
            hardcoded_default_tag: Option<&'static str>,
        ) -> Self {
            Self {
                image,
                image_tag: None,
                suite,
                engine,
                env_tag_var,
                hardcoded_default_tag,
                holds_suite_ref: false,
                host: None,
                port: None,
                connection_url: None,
                key_prefix: String::new(),
            }
        }

        /// Override the image tag (full `name:tag` is `Image::name` + this tag).
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Load tag override from `self.env_tag_var` when set (e.g. `SHAKEDOWN_REDIS_IMAGE_TAG`).
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var(self.env_tag_var) {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        }

        fn effective_tag(&self) -> String {
            if let Some(ref tag) = self.image_tag {
                return tag.clone();
            }
            if let Some(tag) = self.hardcoded_default_tag {
                return tag.to_string();
            }
            self.image.tag().to_string()
        }

        fn fingerprint(&self) -> String {
            format!("{}:{}", self.image.name(), self.effective_tag())
        }

        fn connection_url_display(&self) -> Result<&str, InfrastructureError> {
            self.connection_url
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(self.engine.into()))
        }
    }

    async fn delete_keys_with_prefix(
        connection_url: &str,
        key_prefix: &str,
        resource_label: &str,
    ) -> Result<(), InfrastructureError> {
        let client =
            redis::Client::open(connection_url).map_err(|error| InfrastructureError::Teardown {
                resource: resource_label.to_string(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| InfrastructureError::Teardown {
                resource: resource_label.to_string(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;

        let pattern = format!("{key_prefix}*");
        let mut cursor: u64 = 0;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(256)
                .query_async(&mut connection)
                .await
                .map_err(|error| InfrastructureError::Teardown {
                    resource: resource_label.to_string(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;

            if !keys.is_empty() {
                redis::cmd("DEL")
                    .arg(&keys)
                    .query_async::<i64>(&mut connection)
                    .await
                    .map_err(|error| InfrastructureError::Teardown {
                        resource: resource_label.to_string(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    })?;
            }

            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        Ok(())
    }

    #[async_trait]
    impl<I: Image + Clone + Send + Sync + std::fmt::Debug + 'static> Resource for RespResource<I> {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned(self.engine.into()));
            }

            self.key_prefix = sanitize_redis_key_prefix(scenario_id.as_str());
            let want_key = self.fingerprint();

            let mut guard = self.suite.lock().await;
            let (host, port) = match &mut *guard {
                SuiteState::Idle => {
                    let tag = self.effective_tag();
                    let node = self
                        .image
                        .clone()
                        .with_tag(tag)
                        .with_label(
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                        )
                        .start()
                        .await
                        .map_err(Self::map_tc_err)?;

                    let mapped_port = node
                        .get_host_port_ipv4(RESP_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();
                    *guard = SuiteState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port)
                }
                SuiteState::Running {
                    host,
                    port,
                    ref mut refs,
                    image_key,
                    ..
                } => {
                    if image_key != &want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "{} suite server already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process.",
                                self.engine
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port)
                }
            };
            drop(guard);

            let connection_url = format!("redis://{host}:{port}");
            self.host = Some(host.clone());
            self.port = Some(port);
            self.connection_url = Some(connection_url.clone());
            self.holds_suite_ref = true;

            Ok(vec![ResourceHandle {
                id: format!("{}_{scenario_id}", self.engine),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "engine": self.engine,
                    "host": host,
                    "port": port,
                    "connection_url": connection_url,
                    "key_prefix": self.key_prefix,
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let url = self.connection_url_display()?;
            let client =
                redis::Client::open(url).map_err(|error| InfrastructureError::Readiness {
                    resource: self.engine.into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;

            const ATTEMPTS: u32 = 30;
            for attempt in 0..ATTEMPTS {
                match client.get_multiplexed_async_connection().await {
                    Ok(mut connection) => match redis::cmd("PING")
                        .query_async::<String>(&mut connection)
                        .await
                    {
                        Ok(response) if response.eq_ignore_ascii_case("PONG") => return Ok(()),
                        Ok(_) | Err(_) if attempt + 1 == ATTEMPTS => {
                            return Err(InfrastructureError::Readiness {
                                resource: self.engine.into(),
                                source: Box::new(std::io::Error::other(
                                    "Redis PING did not return PONG",
                                )),
                            });
                        }
                        Ok(_) | Err(_) => {}
                    },
                    Err(error) if attempt + 1 == ATTEMPTS => {
                        return Err(InfrastructureError::Readiness {
                            resource: self.engine.into(),
                            source: Box::new(std::io::Error::other(error.to_string())),
                        });
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }

            Err(InfrastructureError::Readiness {
                resource: self.engine.into(),
                source: Box::new(std::io::Error::other(
                    "Redis readiness: connection attempts exhausted",
                )),
            })
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.connection_url = None;
                self.key_prefix.clear();
                return Ok(());
            }

            let url = self
                .connection_url
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(self.engine.into()))?
                .to_string();
            let prefix = std::mem::take(&mut self.key_prefix);
            let engine = self.engine.to_string();

            delete_keys_with_prefix(url.as_str(), prefix.as_str(), engine.as_str()).await?;

            let mut guard = self.suite.lock().await;
            let mut container_to_remove: Option<testcontainers::ContainerAsync<I>> = None;
            match &mut *guard {
                SuiteState::Idle => {}
                SuiteState::Running { ref mut refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteState::Idle);
                        if let SuiteState::Running { container, .. } = taken {
                            container_to_remove = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_remove {
                container.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.connection_url = None;
            Ok(())
        }
    }
}

#[cfg(feature = "redis")]
mod redis_impl {
    use super::resp_family::{RespResource, SuiteState};
    use super::{Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers_modules::redis::Redis;
    use tokio::sync::Mutex;

    static SUITE_REDIS: OnceLock<Mutex<SuiteState<Redis>>> = OnceLock::new();

    fn suite_redis_mutex() -> &'static Mutex<SuiteState<Redis>> {
        SUITE_REDIS.get_or_init(|| Mutex::new(SuiteState::Idle))
    }

    /// Docker-backed Redis using the official image via testcontainers-modules.
    ///
    /// Suite lifecycle matches Rabbit/Postgres: one container per process; refcounted teardown.
    /// Default image tag is **`7`** (`redis:7`). Override with [`Self::with_image_tag`] or
    /// `SHAKEDOWN_REDIS_IMAGE_TAG`.
    #[derive(Debug)]
    pub struct RedisResource(pub(super) RespResource<Redis>);

    impl Default for RedisResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl RedisResource {
        pub fn new() -> Self {
            Self(
                RespResource::new(
                    Redis::default(),
                    suite_redis_mutex(),
                    "redis",
                    "SHAKEDOWN_REDIS_IMAGE_TAG",
                    Some("7"),
                )
                .with_image_tag_from_env(),
            )
        }

        pub fn with_image_tag(self, tag: impl Into<String>) -> Self {
            Self(self.0.with_image_tag(tag))
        }

        pub fn with_image_tag_from_env(self) -> Self {
            Self(self.0.with_image_tag_from_env())
        }
    }

    #[async_trait]
    impl Resource for RedisResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            self.0.provision(scenario_id).await
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            self.0.check_ready().await
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            self.0.apply_default_seed().await
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            self.0.teardown().await
        }
    }
}

#[cfg(feature = "valkey")]
mod valkey_impl {
    use super::resp_family::{RespResource, SuiteState};
    use super::{Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers_modules::valkey::Valkey;
    use tokio::sync::Mutex;

    static SUITE_VALKEY: OnceLock<Mutex<SuiteState<Valkey>>> = OnceLock::new();

    fn suite_valkey_mutex() -> &'static Mutex<SuiteState<Valkey>> {
        SUITE_VALKEY.get_or_init(|| Mutex::new(SuiteState::Idle))
    }

    /// Docker-backed Valkey using the official image via testcontainers-modules.
    ///
    /// Default tag follows the module image (Valkey 8.x). Override with [`Self::with_image_tag`] or
    /// `SHAKEDOWN_VALKEY_IMAGE_TAG`.
    #[derive(Debug)]
    pub struct ValkeyResource(pub(super) RespResource<Valkey>);

    impl Default for ValkeyResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ValkeyResource {
        pub fn new() -> Self {
            Self(
                RespResource::new(
                    Valkey::default(),
                    suite_valkey_mutex(),
                    "valkey",
                    "SHAKEDOWN_VALKEY_IMAGE_TAG",
                    Some("8"),
                )
                .with_image_tag_from_env(),
            )
        }

        pub fn with_image_tag(self, tag: impl Into<String>) -> Self {
            Self(self.0.with_image_tag(tag))
        }

        pub fn with_image_tag_from_env(self) -> Self {
            Self(self.0.with_image_tag_from_env())
        }
    }

    #[async_trait]
    impl Resource for ValkeyResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            self.0.provision(scenario_id).await
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            self.0.check_ready().await
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            self.0.apply_default_seed().await
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            self.0.teardown().await
        }
    }
}

#[cfg(feature = "mongodb")]
mod mongo_impl {
    use super::{sanitize_sql_database_name, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use mongodb::bson::doc;
    use mongodb::Client;
    use std::sync::OnceLock;
    use testcontainers::core::{Image, ImageExt};
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::mongo::Mongo;
    use tokio::sync::Mutex;

    const MONGO_PORT: u16 = 27017;
    /// Default `mongo` image tag (official Docker Hub; extra-infra SPECS).
    pub const DEFAULT_MONGO_IMAGE_TAG: &str = "7";
    const SUITE_USER: &str = "root";
    const SUITE_PASSWORD: &str = "root";

    #[allow(clippy::large_enum_variant)]
    enum SuiteMongoState {
        Idle,
        Running {
            host: String,
            port: u16,
            container: testcontainers::ContainerAsync<Mongo>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_MONGO: OnceLock<Mutex<SuiteMongoState>> = OnceLock::new();

    fn suite_mongo_mutex() -> &'static Mutex<SuiteMongoState> {
        SUITE_MONGO.get_or_init(|| Mutex::new(SuiteMongoState::Idle))
    }

    fn image_fingerprint(image: &Mongo, tag_override: &Option<String>) -> String {
        let tag = tag_override.as_deref().unwrap_or(DEFAULT_MONGO_IMAGE_TAG);
        format!("{}:{}", image.name(), tag)
    }

    fn admin_connection_uri(host: &str, port: u16) -> String {
        format!("mongodb://{SUITE_USER}:{SUITE_PASSWORD}@{host}:{port}/admin?authSource=admin")
    }

    fn scenario_connection_uri(host: &str, port: u16, database: &str) -> String {
        format!("mongodb://{SUITE_USER}:{SUITE_PASSWORD}@{host}:{port}/{database}?authSource=admin")
    }

    async fn mongo_client(uri: &str) -> Result<Client, mongodb::error::Error> {
        Client::with_uri_str(uri).await
    }

    /// Docker-backed MongoDB using testcontainers-modules (`mongo` image).
    ///
    /// **Suite lifecycle:** one `mongod` per process; refcounted teardown drops the scenario database
    /// and removes the container when the last reference completes teardown.
    ///
    /// Default image tag is **`7`** (`mongo:7`). Override with [`Self::with_image_tag`] or
    /// `SHAKEDOWN_MONGO_IMAGE_TAG`. Root credentials are fixed (`root` / `root`) for the suite container.
    #[derive(Debug)]
    pub struct MongoResource {
        image: Mongo,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        username: String,
        password: String,
        database_name: String,
    }

    impl Default for MongoResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MongoResource {
        /// Create default Docker-backed MongoDB settings (suite server started on first provision).
        pub fn new() -> Self {
            Self {
                image: Mongo::default(),
                image_tag: Some(DEFAULT_MONGO_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                port: None,
                username: SUITE_USER.to_string(),
                password: SUITE_PASSWORD.to_string(),
                database_name: String::new(),
            }
        }

        /// Override the image tag (pin `7.0` or another tag for production parity). Image name stays `mongo`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_MONGO_IMAGE_TAG` when set.
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_MONGO_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        }
    }

    #[async_trait]
    impl Resource for MongoResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("mongodb".into()));
            }

            let database_name = sanitize_sql_database_name(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_mongo_mutex().lock().await;
            let (host, port) = match &mut *guard {
                SuiteMongoState::Idle => {
                    let tag = self.image_tag.as_deref().unwrap_or(DEFAULT_MONGO_IMAGE_TAG);
                    let node = self
                        .image
                        .clone()
                        .with_tag(tag.to_string())
                        .with_env_var("MONGO_INITDB_ROOT_USERNAME", SUITE_USER)
                        .with_env_var("MONGO_INITDB_ROOT_PASSWORD", SUITE_PASSWORD)
                        .with_label(
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                        )
                        .start()
                        .await
                        .map_err(Self::map_tc_err)?;

                    let mapped_port = node
                        .get_host_port_ipv4(MONGO_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();

                    *guard = SuiteMongoState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port)
                }
                SuiteMongoState::Running {
                    host,
                    port,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "MongoDB suite server already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port)
                }
            };
            drop(guard);

            let admin_uri = admin_connection_uri(host.as_str(), port);
            let client = mongo_client(admin_uri.as_str()).await.map_err(|error| {
                InfrastructureError::Provision {
                    resource: "mongodb".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                }
            })?;
            let scenario_db = client.database(database_name.as_str());
            let init_coll = scenario_db.collection::<mongodb::bson::Document>("_shakedown_init");
            init_coll
                .insert_one(doc! { "ok": true })
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: "mongodb".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.database_name = database_name.clone();

            let connection_url =
                scenario_connection_uri(host.as_str(), port, database_name.as_str());
            let username = self.username.clone();
            let password = self.password.clone();

            Ok(vec![ResourceHandle {
                id: format!("mongo_{scenario_id}"),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "database": database_name,
                    "username": username,
                    "password": password,
                    "connection_url": connection_url,
                    "engine": "mongodb",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let host = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("mongodb".into()))?;
            let port = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned("mongodb".into()))?;

            let admin_uri = admin_connection_uri(host, port);
            let client = mongo_client(admin_uri.as_str()).await.map_err(|error| {
                InfrastructureError::Readiness {
                    resource: "mongodb".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                }
            })?;
            let admin = client.database("admin");
            admin
                .run_command(doc! { "ping": 1 })
                .await
                .map_err(|error| InfrastructureError::Readiness {
                    resource: "mongodb".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.database_name.clear();
                return Ok(());
            }

            let database_name = self.database_name.clone();
            let host = self.host.clone().unwrap_or_default();
            let port = self.port.unwrap_or(MONGO_PORT);

            if !database_name.is_empty() {
                let admin_uri = admin_connection_uri(host.as_str(), port);
                if let Ok(client) = mongo_client(admin_uri.as_str()).await {
                    let db = client.database(database_name.as_str());
                    let _ = db.drop().await;
                }
            }

            let mut guard = suite_mongo_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<Mongo>> = None;
            match &mut *guard {
                SuiteMongoState::Idle => {}
                SuiteMongoState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteMongoState::Idle);
                        if let SuiteMongoState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_rm {
                container.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.database_name.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "nats")]
mod nats_impl {
    use super::{sanitize_nats_subject_prefix, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers::core::{Image, ImageExt};
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::nats::Nats;
    use tokio::sync::Mutex;

    const NATS_CLIENT_PORT: u16 = 4222;
    /// Default `nats` image tag (Docker Hub `nats:2`; extra-infra PRD).
    pub const DEFAULT_NATS_IMAGE_TAG: &str = "2";

    #[allow(clippy::large_enum_variant)]
    enum SuiteNatsState {
        Idle,
        Running {
            host: String,
            port: u16,
            container: testcontainers::ContainerAsync<Nats>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_NATS: OnceLock<Mutex<SuiteNatsState>> = OnceLock::new();

    fn suite_nats_mutex() -> &'static Mutex<SuiteNatsState> {
        SUITE_NATS.get_or_init(|| Mutex::new(SuiteNatsState::Idle))
    }

    fn image_fingerprint(image: &Nats, tag_override: &Option<String>) -> String {
        let tag = tag_override.as_deref().unwrap_or(DEFAULT_NATS_IMAGE_TAG);
        format!("{}:{}", image.name(), tag)
    }

    /// Docker-backed NATS using testcontainers-modules (`nats` image).
    ///
    /// **Suite lifecycle:** one server per process; refcounted teardown removes the container when
    /// the last scenario completes. Core pub/sub only — subjects are ephemeral; no per-scenario
    /// broker cleanup beyond releasing the suite reference.
    ///
    /// Default image tag is **`2`** (`nats:2`). Override with [`Self::with_image_tag`] or
    /// `SHAKEDOWN_NATS_IMAGE_TAG`.
    #[derive(Debug)]
    pub struct NatsResource {
        image: Nats,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        connection_url: Option<String>,
        subject_prefix: String,
    }

    impl Default for NatsResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl NatsResource {
        /// Create default Docker-backed NATS settings (suite broker started on first provision).
        pub fn new() -> Self {
            Self {
                image: Nats::default(),
                image_tag: Some(DEFAULT_NATS_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                port: None,
                connection_url: None,
                subject_prefix: String::new(),
            }
            .with_image_tag_from_env()
        }

        /// Override the image tag (pin a patch release for production parity). Image name stays `nats`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_NATS_IMAGE_TAG` when set (non-empty).
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_NATS_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        }

        fn connection_url_display(&self) -> Result<&str, InfrastructureError> {
            self.connection_url
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("nats".into()))
        }
    }

    #[async_trait]
    impl Resource for NatsResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("nats".into()));
            }

            self.subject_prefix = sanitize_nats_subject_prefix(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_nats_mutex().lock().await;
            let (host, port) = match &mut *guard {
                SuiteNatsState::Idle => {
                    let tag = self.image_tag.as_deref().unwrap_or(DEFAULT_NATS_IMAGE_TAG);
                    let node = self
                        .image
                        .clone()
                        .with_tag(tag.to_string())
                        .with_label(
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                        )
                        .start()
                        .await
                        .map_err(Self::map_tc_err)?;

                    let mapped_port = node
                        .get_host_port_ipv4(NATS_CLIENT_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();

                    *guard = SuiteNatsState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port)
                }
                SuiteNatsState::Running {
                    host,
                    port,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "NATS suite broker already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port)
                }
            };
            drop(guard);

            let connection_url = format!("nats://{host}:{port}");
            self.host = Some(host.clone());
            self.port = Some(port);
            self.connection_url = Some(connection_url.clone());
            self.holds_suite_ref = true;

            let subject_prefix = self.subject_prefix.clone();

            Ok(vec![ResourceHandle {
                id: format!("nats_{scenario_id}"),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "engine": "nats",
                    "host": host,
                    "port": port,
                    "connection_url": connection_url,
                    "subject_prefix": subject_prefix,
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let url = self.connection_url_display()?;
            const ATTEMPTS: u32 = 30;
            for attempt in 0..ATTEMPTS {
                match async_nats::connect(url).await {
                    Ok(client) => match client.flush().await {
                        Ok(()) => return Ok(()),
                        Err(error) if attempt + 1 == ATTEMPTS => {
                            return Err(InfrastructureError::Readiness {
                                resource: "nats".into(),
                                source: Box::new(std::io::Error::other(error.to_string())),
                            });
                        }
                        Err(_) => {}
                    },
                    Err(error) if attempt + 1 == ATTEMPTS => {
                        return Err(InfrastructureError::Readiness {
                            resource: "nats".into(),
                            source: Box::new(std::io::Error::other(error.to_string())),
                        });
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(InfrastructureError::Readiness {
                resource: "nats".into(),
                source: Box::new(std::io::Error::other(
                    "NATS readiness: connection attempts exhausted",
                )),
            })
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.connection_url = None;
                self.subject_prefix.clear();
                return Ok(());
            }

            let mut guard = suite_nats_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<Nats>> = None;
            match &mut *guard {
                SuiteNatsState::Idle => {}
                SuiteNatsState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteNatsState::Idle);
                        if let SuiteNatsState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_rm {
                container.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.connection_url = None;
            self.subject_prefix.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "neo4j")]
mod neo4j_impl {
    use super::{sanitize_sql_database_name, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use neo4rs::{query, ConfigBuilder, Graph};
    use std::sync::OnceLock;
    use testcontainers::core::{ImageExt, IntoContainerPort};
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::neo4j::{Neo4j, Neo4jImage};
    use tokio::sync::Mutex;

    const NEO4J_BOLT_PORT: u16 = 7687;
    const NEO4J_HTTP_PORT: u16 = 7474;
    /// Default image tag: Enterprise edition for multi-database isolation (extra-infra SPECS / OQ-1.2).
    pub const DEFAULT_NEO4J_IMAGE_TAG: &str = "5-enterprise";
    const SUITE_USER: &str = "neo4j";
    const SUITE_PASSWORD: &str = "shakedown";

    #[allow(clippy::large_enum_variant)]
    enum SuiteNeo4jState {
        Idle,
        Running {
            host: String,
            bolt_port: u16,
            http_port: u16,
            container: testcontainers::ContainerAsync<Neo4jImage>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_NEO4J: OnceLock<Mutex<SuiteNeo4jState>> = OnceLock::new();

    fn suite_neo4j_mutex() -> &'static Mutex<SuiteNeo4jState> {
        SUITE_NEO4J.get_or_init(|| Mutex::new(SuiteNeo4jState::Idle))
    }

    fn image_fingerprint(tag_override: &Option<String>) -> String {
        let tag = tag_override.as_deref().unwrap_or(DEFAULT_NEO4J_IMAGE_TAG);
        format!("neo4j:{tag}")
    }

    fn bolt_uri(host: &str, bolt_port: u16) -> String {
        format!("bolt://{host}:{bolt_port}")
    }

    fn map_neo_err(error: neo4rs::Error) -> InfrastructureError {
        InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
    }

    async fn graph_for_database(
        host: &str,
        bolt_port: u16,
        database: &str,
    ) -> Result<Graph, InfrastructureError> {
        let uri = bolt_uri(host, bolt_port);
        let configuration = ConfigBuilder::default()
            .uri(uri)
            .user(SUITE_USER)
            .password(SUITE_PASSWORD)
            .db(database)
            .build()
            .map_err(|error| InfrastructureError::Provision {
                resource: "neo4j".into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        Graph::connect(configuration).await.map_err(map_neo_err)
    }

    async fn graph_system(host: &str, bolt_port: u16) -> Result<Graph, InfrastructureError> {
        graph_for_database(host, bolt_port, "system").await
    }

    async fn run_return_one_system(host: &str, bolt_port: u16) -> Result<(), InfrastructureError> {
        let graph = graph_system(host, bolt_port).await?;
        let mut rows = graph
            .execute(query("RETURN 1 AS n"))
            .await
            .map_err(map_neo_err)?;
        let row_opt = rows.next().await.map_err(map_neo_err)?;
        let Some(row) = row_opt else {
            return Err(InfrastructureError::Readiness {
                resource: "neo4j".into(),
                source: Box::new(std::io::Error::other(
                    "Neo4j readiness: RETURN 1 produced no row",
                )),
            });
        };
        let _: i64 = row
            .get("n")
            .map_err(|error| InfrastructureError::Readiness {
                resource: "neo4j".into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        Ok(())
    }

    /// Docker-backed Neo4j using testcontainers-modules (`neo4j` image).
    ///
    /// **Suite lifecycle:** one server per process; per-scenario **database** via `CREATE DATABASE`
    /// (Neo4j 5 **Enterprise**). Default tag **`5-enterprise`** with license acceptance env.
    /// Override with [`Self::with_image_tag`] or `SHAKEDOWN_NEO4J_IMAGE_TAG`. Community Edition images
    /// do not support multiple databases — provisioning fails unless you use a single shared DB
    /// (not supported by this resource).
    #[derive(Debug)]
    pub struct Neo4jResource {
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        bolt_port: Option<u16>,
        http_port: Option<u16>,
        username: String,
        password: String,
        database_name: String,
    }

    impl Default for Neo4jResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Neo4jResource {
        /// Create default Docker-backed Neo4j settings (suite server started on first provision).
        pub fn new() -> Self {
            Self {
                image_tag: Some(DEFAULT_NEO4J_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                bolt_port: None,
                http_port: None,
                username: SUITE_USER.to_string(),
                password: SUITE_PASSWORD.to_string(),
                database_name: String::new(),
            }
            .with_image_tag_from_env()
        }

        /// Override the image tag (e.g. `5-enterprise` or a digest-pinned tag). Image name stays `neo4j`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_NEO4J_IMAGE_TAG` when set (non-empty).
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_NEO4J_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        }
    }

    #[async_trait]
    impl Resource for Neo4jResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("neo4j".into()));
            }

            let database_name = sanitize_sql_database_name(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image_tag);

            let mut guard = suite_neo4j_mutex().lock().await;
            let (host, bolt_port, http_port) = match &mut *guard {
                SuiteNeo4jState::Idle => {
                    let tag = self.image_tag.as_deref().unwrap_or(DEFAULT_NEO4J_IMAGE_TAG);
                    let neo4j_image: Neo4jImage = Neo4j::new()
                        .with_version(tag.to_string())
                        .with_user(SUITE_USER)
                        .with_password(SUITE_PASSWORD)
                        .into();

                    let node = neo4j_image
                        .with_env_var("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                        .with_label(
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                        )
                        .start()
                        .await
                        .map_err(Self::map_tc_err)?;

                    let mapped_bolt = node
                        .get_host_port_ipv4(NEO4J_BOLT_PORT.tcp())
                        .await
                        .map_err(Self::map_tc_err)?;
                    let mapped_http = node
                        .get_host_port_ipv4(NEO4J_HTTP_PORT.tcp())
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();

                    *guard = SuiteNeo4jState::Running {
                        host: host.clone(),
                        bolt_port: mapped_bolt,
                        http_port: mapped_http,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_bolt, mapped_http)
                }
                SuiteNeo4jState::Running {
                    host,
                    bolt_port,
                    http_port,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "Neo4j suite server already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *bolt_port, *http_port)
                }
            };
            drop(guard);

            let system_graph = graph_system(host.as_str(), bolt_port)
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: "neo4j".into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;

            let create_db_cypher = format!("CREATE DATABASE `{database_name}` WAIT");
            if let Err(error) = system_graph.run(query(&create_db_cypher)).await {
                return Err(InfrastructureError::Provision {
                    resource: "neo4j".into(),
                    source: Box::new(std::io::Error::other(format!(
                        "{error} (hint: per-scenario databases require Neo4j Enterprise; \
                         override image with SHAKEDOWN_NEO4J_IMAGE_TAG=5-enterprise or use a \
                         Community image only if you accept degraded single-database isolation)"
                    ))),
                });
            }

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.bolt_port = Some(bolt_port);
            self.http_port = Some(http_port);
            self.database_name = database_name.clone();

            let username = self.username.clone();
            let password = self.password.clone();
            let bolt_url = format!("bolt://{}:{}@{}:{}", username, password, host, bolt_port);
            let http_url = format!("http://{}:{}@{}:{}", username, password, host, http_port);

            Ok(vec![ResourceHandle {
                id: format!("neo4j_{scenario_id}"),
                connection_string: bolt_url.clone(),
                metadata: Some(serde_json::json!({
                    "engine": "neo4j",
                    "host": host,
                    "port": bolt_port,
                    "http_port": http_port,
                    "database": database_name,
                    "username": username,
                    "password": password,
                    "bolt_url": bolt_url,
                    "http_url": http_url,
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let host = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("neo4j".into()))?;
            let bolt_port = self
                .bolt_port
                .ok_or_else(|| InfrastructureError::NotProvisioned("neo4j".into()))?;

            const ATTEMPTS: u32 = 30;
            for attempt in 0..ATTEMPTS {
                match run_return_one_system(host, bolt_port).await {
                    Ok(()) => return Ok(()),
                    Err(error) if attempt + 1 == ATTEMPTS => return Err(error),
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                }
            }
            Err(InfrastructureError::Readiness {
                resource: "neo4j".into(),
                source: Box::new(std::io::Error::other("Neo4j readiness: attempts exhausted")),
            })
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.bolt_port = None;
                self.http_port = None;
                self.database_name.clear();
                return Ok(());
            }

            let database_name = self.database_name.clone();
            let host = self.host.clone().unwrap_or_default();
            let bolt_port = self.bolt_port.unwrap_or(NEO4J_BOLT_PORT);

            if !database_name.is_empty() {
                if let Ok(graph) = graph_system(host.as_str(), bolt_port).await {
                    let drop_cypher = format!("DROP DATABASE `{database_name}` IF EXISTS");
                    let _ = graph.run(query(&drop_cypher)).await;
                }
            }

            let mut guard = suite_neo4j_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<Neo4jImage>> = None;
            match &mut *guard {
                SuiteNeo4jState::Idle => {}
                SuiteNeo4jState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteNeo4jState::Idle);
                        if let SuiteNeo4jState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_rm {
                container.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.bolt_port = None;
            self.http_port = None;
            self.database_name.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "postgres")]
mod postgres_impl {
    use super::{sanitize_sql_database_name, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers::core::Image;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;
    use tokio::sync::Mutex;
    use tokio_postgres::NoTls;

    const POSTGRES_PORT: u16 = 5432;
    /// Default `postgres` image tag (Debian-based, non-alpine; see `mvp-postgres-mariadb` SPECS).
    pub const DEFAULT_POSTGRES_IMAGE_TAG: &str = "16";

    #[allow(clippy::large_enum_variant)]
    enum SuitePostgresState {
        Idle,
        Running {
            host: String,
            port: u16,
            username: String,
            password: String,
            container: testcontainers::ContainerAsync<Postgres>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_POSTGRES: OnceLock<Mutex<SuitePostgresState>> = OnceLock::new();

    fn suite_postgres_mutex() -> &'static Mutex<SuitePostgresState> {
        SUITE_POSTGRES.get_or_init(|| Mutex::new(SuitePostgresState::Idle))
    }

    fn image_fingerprint(image: &Postgres, tag_override: &Option<String>) -> String {
        let tag = tag_override
            .as_deref()
            .unwrap_or(DEFAULT_POSTGRES_IMAGE_TAG);
        format!("{}:{}", image.name(), tag)
    }

    fn admin_connection_url(host: &str, port: u16, username: &str, password: &str) -> String {
        format!("postgresql://{username}:{password}@{host}:{port}/postgres")
    }

    fn scenario_connection_url(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
    ) -> String {
        format!("postgresql://{username}:{password}@{host}:{port}/{database}")
    }

    async fn connect_with_url(url: &str) -> Result<tokio_postgres::Client, InfrastructureError> {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await.map_err(|e| {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        })?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    /// Docker-backed PostgreSQL using testcontainers-modules (`postgres` image).
    ///
    /// **Suite lifecycle:** the first provision in the process starts a single server; further
    /// [`PostgresResource`] instances reuse it. The last successful [`Resource::teardown`] drops the
    /// scenario database and may remove the container when the refcount reaches zero.
    #[derive(Debug)]
    pub struct PostgresResource {
        image: Postgres,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        username: String,
        password: String,
        database_name: String,
    }

    impl Default for PostgresResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl PostgresResource {
        /// Create default Docker-backed Postgres settings (suite server started on first provision).
        pub fn new() -> Self {
            Self {
                image: Postgres::default(),
                image_tag: Some(DEFAULT_POSTGRES_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                port: None,
                username: "postgres".to_string(),
                password: "postgres".to_string(),
                database_name: String::new(),
            }
        }

        /// Override the image tag (e.g. pin `16` or `16.4` for production parity). Name stays `postgres`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_POSTGRES_IMAGE_TAG` when set.
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_POSTGRES_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(e: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }
    }

    #[async_trait]
    impl Resource for PostgresResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("postgres".into()));
            }

            let database_name = sanitize_sql_database_name(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_postgres_mutex().lock().await;
            let (host, port, username, password) = match &mut *guard {
                SuitePostgresState::Idle => {
                    use testcontainers::core::ImageExt;
                    let image = self.image.clone();
                    let node = if let Some(ref tag) = self.image_tag {
                        image
                            .with_tag(tag.clone())
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    };

                    let mapped_port = node
                        .get_host_port_ipv4(POSTGRES_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();
                    let username = "postgres".to_string();
                    let password = "postgres".to_string();

                    *guard = SuitePostgresState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        username: username.clone(),
                        password: password.clone(),
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port, username, password)
                }
                SuitePostgresState::Running {
                    host,
                    port,
                    username,
                    password,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "Postgres suite server already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port, username.clone(), password.clone())
                }
            };
            drop(guard);

            let admin_url = admin_connection_url(&host, port, &username, &password);
            let client = connect_with_url(&admin_url).await?;
            let create_sql = format!("CREATE DATABASE {}", database_name);
            client.batch_execute(&create_sql).await.map_err(|e| {
                InfrastructureError::Provision {
                    resource: "postgres".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                }
            })?;

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.username = username.clone();
            self.password = password.clone();
            self.database_name = database_name.clone();

            let connection_url =
                scenario_connection_url(&host, port, &database_name, &username, &password);

            Ok(vec![ResourceHandle {
                id: format!("postgres_{}", scenario_id),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "database": database_name,
                    "username": username,
                    "password": password,
                    "connection_url": connection_url,
                    "engine": "postgres",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let host = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("postgres".into()))?;
            let port = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned("postgres".into()))?;
            let url = scenario_connection_url(
                host,
                port,
                &self.database_name,
                &self.username,
                &self.password,
            );
            let client =
                connect_with_url(&url)
                    .await
                    .map_err(|e| InfrastructureError::Readiness {
                        resource: "postgres".into(),
                        source: Box::new(std::io::Error::other(e.to_string())),
                    })?;
            client.query_one("SELECT 1", &[]).await.map_err(|e| {
                InfrastructureError::Readiness {
                    resource: "postgres".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                }
            })?;
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.database_name.clear();
                return Ok(());
            }

            let database_name = self.database_name.clone();
            let host = self.host.clone().unwrap_or_default();
            let port = self.port.unwrap_or(POSTGRES_PORT);
            let username = self.username.clone();
            let password = self.password.clone();

            if !database_name.is_empty() {
                let admin_url = admin_connection_url(&host, port, &username, &password);
                if let Ok(client) = connect_with_url(&admin_url).await {
                    let _ = client
                        .query(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
                            &[&database_name],
                        )
                        .await;
                    let drop_sql = format!("DROP DATABASE IF EXISTS {}", database_name);
                    let _ = client.batch_execute(&drop_sql).await;
                }
            }

            let mut guard = suite_postgres_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<Postgres>> = None;
            match &mut *guard {
                SuitePostgresState::Idle => {}
                SuitePostgresState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuitePostgresState::Idle);
                        if let SuitePostgresState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(c) = container_to_rm {
                c.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.database_name.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "mariadb")]
mod mariadb_impl {
    use super::{sanitize_sql_database_name, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use mysql_async::prelude::Queryable;
    use mysql_async::{OptsBuilder, Pool};
    use std::sync::OnceLock;
    use testcontainers::core::Image;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::mariadb::Mariadb;
    use tokio::sync::Mutex;

    const MARIADB_PORT: u16 = 3306;
    /// Default `mariadb` image tag (non-alpine; see `mvp-postgres-mariadb` SPECS).
    pub const DEFAULT_MARIADB_IMAGE_TAG: &str = "11";

    #[allow(clippy::large_enum_variant)]
    enum SuiteMariaDBState {
        Idle,
        Running {
            host: String,
            port: u16,
            username: String,
            password: String,
            container: testcontainers::ContainerAsync<Mariadb>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_MARIADB: OnceLock<Mutex<SuiteMariaDBState>> = OnceLock::new();

    fn suite_mariadb_mutex() -> &'static Mutex<SuiteMariaDBState> {
        SUITE_MARIADB.get_or_init(|| Mutex::new(SuiteMariaDBState::Idle))
    }

    fn image_fingerprint(image: &Mariadb, tag_override: &Option<String>) -> String {
        let tag = tag_override.as_deref().unwrap_or(DEFAULT_MARIADB_IMAGE_TAG);
        format!("{}:{}", image.name(), tag)
    }

    fn admin_pool(host: &str, port: u16, username: &str, password: &str) -> Pool {
        let mut builder = OptsBuilder::default()
            .user(Some(username))
            .ip_or_hostname(host)
            .tcp_port(port)
            .db_name(Some("mysql"));
        if password.is_empty() {
            builder = builder.pass::<String>(None);
        } else {
            builder = builder.pass(Some(password));
        }
        Pool::new(builder)
    }

    fn scenario_connection_url(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
    ) -> String {
        if password.is_empty() {
            format!("mysql://{username}@{host}:{port}/{database}")
        } else {
            format!("mysql://{username}:{password}@{host}:{port}/{database}")
        }
    }

    /// Docker-backed MariaDB using testcontainers-modules (`mariadb` image).
    ///
    /// **Suite lifecycle:** the first provision in the process starts a single server; further
    /// [`MariaDBResource`] instances reuse it. The last successful [`Resource::teardown`] drops the
    /// scenario database and may remove the container when the refcount reaches zero.
    #[derive(Debug)]
    pub struct MariaDBResource {
        image: Mariadb,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        username: String,
        password: String,
        database_name: String,
    }

    impl Default for MariaDBResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MariaDBResource {
        /// Create default Docker-backed MariaDB settings (suite server started on first provision).
        pub fn new() -> Self {
            Self {
                image: Mariadb::default(),
                image_tag: Some(DEFAULT_MARIADB_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                port: None,
                username: "root".to_string(),
                password: String::new(),
                database_name: String::new(),
            }
        }

        /// Override the image tag (e.g. pin `11` or `11.4` for production parity). Name stays `mariadb`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_MARIADB_IMAGE_TAG` when set.
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_MARIADB_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(e: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }

        fn map_mysql_err(e: mysql_async::Error) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }
    }

    #[async_trait]
    impl Resource for MariaDBResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("mariadb".into()));
            }

            let database_name = sanitize_sql_database_name(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_mariadb_mutex().lock().await;
            let (host, port, username, password) = match &mut *guard {
                SuiteMariaDBState::Idle => {
                    use testcontainers::core::ImageExt;
                    let image = self.image.clone();
                    let node = if let Some(ref tag) = self.image_tag {
                        image
                            .with_tag(tag.clone())
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    };

                    let mapped_port = node
                        .get_host_port_ipv4(MARIADB_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();
                    let username = "root".to_string();
                    let password = String::new();

                    *guard = SuiteMariaDBState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        username: username.clone(),
                        password: password.clone(),
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port, username, password)
                }
                SuiteMariaDBState::Running {
                    host,
                    port,
                    username,
                    password,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "MariaDB suite server already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port, username.clone(), password.clone())
                }
            };
            drop(guard);

            let pool = admin_pool(&host, port, &username, &password);
            let mut conn = pool.get_conn().await.map_err(Self::map_mysql_err)?;
            let create_sql = format!("CREATE DATABASE `{}`", database_name.replace('`', ""));
            conn.query_drop(create_sql.as_str()).await.map_err(|e| {
                InfrastructureError::Provision {
                    resource: "mariadb".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                }
            })?;

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.username = username.clone();
            self.password = password.clone();
            self.database_name = database_name.clone();

            let connection_url =
                scenario_connection_url(&host, port, &database_name, &username, &password);

            Ok(vec![ResourceHandle {
                id: format!("mariadb_{}", scenario_id),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "database": database_name,
                    "username": username,
                    "password": password,
                    "connection_url": connection_url,
                    "engine": "mariadb",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let host = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("mariadb".into()))?;
            let port = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned("mariadb".into()))?;
            let pool = admin_pool(host, port, &self.username, &self.password);
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|e| InfrastructureError::Readiness {
                    resource: "mariadb".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                })?;
            conn.query_drop(format!("USE `{}`", self.database_name.replace('`', "")).as_str())
                .await
                .map_err(|e| InfrastructureError::Readiness {
                    resource: "mariadb".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                })?;
            conn.query_drop("SELECT 1")
                .await
                .map_err(|e| InfrastructureError::Readiness {
                    resource: "mariadb".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                })?;
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.database_name.clear();
                return Ok(());
            }

            let database_name = self.database_name.clone();
            let host = self.host.clone().unwrap_or_default();
            let port = self.port.unwrap_or(MARIADB_PORT);
            let username = self.username.clone();
            let password = self.password.clone();

            if !database_name.is_empty() {
                let pool = admin_pool(&host, port, &username, &password);
                if let Ok(mut conn) = pool.get_conn().await {
                    let safe = database_name.replace('`', "");
                    let kill_sql = format!(
                        "SELECT CONCAT('KILL ', id, ';') AS stmt FROM information_schema.processlist \
                         WHERE db = '{}'",
                        safe.replace('\'', "''")
                    );
                    if let Ok(rows) = conn.query::<String, _>(kill_sql).await {
                        for stmt in rows {
                            let _ = conn.query_drop(stmt.as_str()).await;
                        }
                    }
                    let drop_sql = format!("DROP DATABASE IF EXISTS `{}`", safe);
                    let _ = conn.query_drop(drop_sql.as_str()).await;
                }
            }

            let mut guard = suite_mariadb_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<Mariadb>> = None;
            match &mut *guard {
                SuiteMariaDBState::Idle => {}
                SuiteMariaDBState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteMariaDBState::Idle);
                        if let SuiteMariaDBState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(c) = container_to_rm {
                c.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.database_name.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "mysql")]
mod mysql_impl {
    use super::{sanitize_sql_database_name, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use mysql_async::prelude::Queryable;
    use mysql_async::{OptsBuilder, Pool};
    use std::sync::OnceLock;
    use testcontainers::core::Image;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::mysql::Mysql as MysqlImage;
    use tokio::sync::Mutex;

    const MYSQL_PORT: u16 = 3306;
    /// Default `mysql` image tag (Oracle MySQL; non-alpine; see `mvp-postgres-mariadb` SPECS).
    pub const DEFAULT_MYSQL_IMAGE_TAG: &str = "8.4";

    #[allow(clippy::large_enum_variant)]
    enum SuiteMysqlState {
        Idle,
        Running {
            host: String,
            port: u16,
            username: String,
            password: String,
            container: testcontainers::ContainerAsync<MysqlImage>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_MYSQL: OnceLock<Mutex<SuiteMysqlState>> = OnceLock::new();

    fn suite_mysql_mutex() -> &'static Mutex<SuiteMysqlState> {
        SUITE_MYSQL.get_or_init(|| Mutex::new(SuiteMysqlState::Idle))
    }

    fn image_fingerprint(image: &MysqlImage, tag_override: &Option<String>) -> String {
        let tag = tag_override.as_deref().unwrap_or(DEFAULT_MYSQL_IMAGE_TAG);
        format!("{}:{}", image.name(), tag)
    }

    fn admin_pool(host: &str, port: u16, username: &str, password: &str) -> Pool {
        let mut builder = OptsBuilder::default()
            .user(Some(username))
            .ip_or_hostname(host)
            .tcp_port(port)
            .db_name(Some("mysql"));
        if password.is_empty() {
            builder = builder.pass::<String>(None);
        } else {
            builder = builder.pass(Some(password));
        }
        Pool::new(builder)
    }

    fn scenario_connection_url(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
    ) -> String {
        if password.is_empty() {
            format!("mysql://{username}@{host}:{port}/{database}")
        } else {
            format!("mysql://{username}:{password}@{host}:{port}/{database}")
        }
    }

    /// Docker-backed Oracle MySQL using testcontainers-modules (`mysql` image).
    ///
    /// **Suite lifecycle:** the first provision in the process starts a single server; further
    /// [`MysqlResource`] instances reuse it. The last successful [`Resource::teardown`] drops the
    /// scenario database and may remove the container when the refcount reaches zero.
    #[derive(Debug)]
    pub struct MysqlResource {
        image: MysqlImage,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        username: String,
        password: String,
        database_name: String,
    }

    impl Default for MysqlResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MysqlResource {
        /// Create default Docker-backed MySQL settings (suite server started on first provision).
        pub fn new() -> Self {
            Self {
                image: MysqlImage::default(),
                image_tag: Some(DEFAULT_MYSQL_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                port: None,
                username: "root".to_string(),
                password: String::new(),
                database_name: String::new(),
            }
        }

        /// Override the image tag (e.g. pin `8.4` or `8.4.2` for production parity). Name stays `mysql`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_MYSQL_IMAGE_TAG` when set.
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_MYSQL_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(e: impl std::fmt::Display + Send + Sync + 'static) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }

        fn map_mysql_err(e: mysql_async::Error) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        }
    }

    #[async_trait]
    impl Resource for MysqlResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned("mysql".into()));
            }

            let database_name = sanitize_sql_database_name(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_mysql_mutex().lock().await;
            let (host, port, username, password) = match &mut *guard {
                SuiteMysqlState::Idle => {
                    use testcontainers::core::ImageExt;
                    let image = self.image.clone();
                    let node = if let Some(ref tag) = self.image_tag {
                        image
                            .with_tag(tag.clone())
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    } else {
                        image
                            .with_label(
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                                crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                            )
                            .start()
                            .await
                            .map_err(Self::map_tc_err)?
                    };

                    let mapped_port = node
                        .get_host_port_ipv4(MYSQL_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();
                    let username = "root".to_string();
                    let password = String::new();

                    *guard = SuiteMysqlState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        username: username.clone(),
                        password: password.clone(),
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port, username, password)
                }
                SuiteMysqlState::Running {
                    host,
                    port,
                    username,
                    password,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "MySQL suite server already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port, username.clone(), password.clone())
                }
            };
            drop(guard);

            let pool = admin_pool(&host, port, &username, &password);
            let mut conn = pool.get_conn().await.map_err(Self::map_mysql_err)?;
            let create_sql = format!("CREATE DATABASE `{}`", database_name.replace('`', ""));
            conn.query_drop(create_sql.as_str()).await.map_err(|e| {
                InfrastructureError::Provision {
                    resource: "mysql".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                }
            })?;

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.username = username.clone();
            self.password = password.clone();
            self.database_name = database_name.clone();

            let connection_url =
                scenario_connection_url(&host, port, &database_name, &username, &password);

            Ok(vec![ResourceHandle {
                id: format!("mysql_{}", scenario_id),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "database": database_name,
                    "username": username,
                    "password": password,
                    "connection_url": connection_url,
                    "engine": "mysql",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let host = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned("mysql".into()))?;
            let port = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned("mysql".into()))?;
            let pool = admin_pool(host, port, &self.username, &self.password);
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|e| InfrastructureError::Readiness {
                    resource: "mysql".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                })?;
            conn.query_drop(format!("USE `{}`", self.database_name.replace('`', "")).as_str())
                .await
                .map_err(|e| InfrastructureError::Readiness {
                    resource: "mysql".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                })?;
            conn.query_drop("SELECT 1")
                .await
                .map_err(|e| InfrastructureError::Readiness {
                    resource: "mysql".into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                })?;
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.database_name.clear();
                return Ok(());
            }

            let database_name = self.database_name.clone();
            let host = self.host.clone().unwrap_or_default();
            let port = self.port.unwrap_or(MYSQL_PORT);
            let username = self.username.clone();
            let password = self.password.clone();

            if !database_name.is_empty() {
                let pool = admin_pool(&host, port, &username, &password);
                if let Ok(mut conn) = pool.get_conn().await {
                    let safe = database_name.replace('`', "");
                    let kill_sql = format!(
                        "SELECT CONCAT('KILL ', id, ';') AS stmt FROM information_schema.processlist \
                         WHERE db = '{}'",
                        safe.replace('\'', "''")
                    );
                    if let Ok(rows) = conn.query::<String, _>(kill_sql).await {
                        for stmt in rows {
                            let _ = conn.query_drop(stmt.as_str()).await;
                        }
                    }
                    let drop_sql = format!("DROP DATABASE IF EXISTS `{}`", safe);
                    let _ = conn.query_drop(drop_sql.as_str()).await;
                }
            }

            let mut guard = suite_mysql_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<MysqlImage>> = None;
            match &mut *guard {
                SuiteMysqlState::Idle => {}
                SuiteMysqlState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteMysqlState::Idle);
                        if let SuiteMysqlState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container_async) = container_to_rm {
                container_async.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.database_name.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "sqlite")]
mod sqlite_impl {
    use super::{sqlite_named_memory_uri, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use rusqlite::{Connection, OpenFlags};
    use std::sync::{Arc, Mutex};

    const RESOURCE_NAME: &str = "sqlite";

    /// In-memory SQLite for one scenario (no Docker).
    ///
    /// Opens a **named shared-cache in-memory** database (see [`super::sqlite_named_memory_uri`]) so
    /// [`SqliteBindings::database_url`] can be used with `rusqlite` or other drivers to open further
    /// connections to the same logical database.
    ///
    /// `rusqlite::Connection` is not [`Sync`]; it is wrapped in [`Arc`] [`Mutex`] so [`BuiltinResource`]
    /// remains [`Sync`] and readiness can use [`tokio::task::spawn_blocking`] (works on the default
    /// single-threaded Tokio test executor used by `#[tokio::test]`).
    #[derive(Debug, Default)]
    pub struct SqliteResource {
        connection: Option<Arc<Mutex<Connection>>>,
        /// Retained for debugging; matches metadata `database_url`.
        database_url: String,
    }

    impl SqliteResource {
        /// New SQLite resource (connection opened on [`Resource::provision`]).
        pub fn new() -> Self {
            Self::default()
        }
    }

    #[async_trait]
    impl Resource for SqliteResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.connection.is_some() {
                return Err(InfrastructureError::AlreadyProvisioned(
                    RESOURCE_NAME.into(),
                ));
            }

            let database_url = sqlite_named_memory_uri(scenario_id.as_str());
            let open_url = database_url.clone();
            let connection = tokio::task::spawn_blocking(move || {
                Connection::open_with_flags(
                    open_url.as_str(),
                    OpenFlags::SQLITE_OPEN_URI
                        | OpenFlags::SQLITE_OPEN_READ_WRITE
                        | OpenFlags::SQLITE_OPEN_CREATE,
                )
            })
            .await
            .map_err(|join_error| InfrastructureError::Provision {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(join_error.to_string())),
            })?
            .map_err(|source| InfrastructureError::Provision {
                resource: RESOURCE_NAME.into(),
                source: Box::new(source),
            })?;

            self.connection = Some(Arc::new(Mutex::new(connection)));
            self.database_url.clone_from(&database_url);

            Ok(vec![ResourceHandle {
                id: format!("sqlite_{scenario_id}"),
                connection_string: database_url.clone(),
                metadata: Some(serde_json::json!({
                    "database_url": database_url,
                    "engine": "sqlite",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let arc = self
                .connection
                .as_ref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            let arc = Arc::clone(arc);
            tokio::task::spawn_blocking(move || {
                let connection = arc
                    .lock()
                    .map_err(|poison| InfrastructureError::Readiness {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(poison.to_string())),
                    })?;
                connection
                    .query_row("SELECT 1", [], |row| row.get::<_, i32>(0))
                    .map_err(|source| InfrastructureError::Readiness {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(source),
                    })?;
                Ok::<(), InfrastructureError>(())
            })
            .await
            .map_err(|join_error| InfrastructureError::Readiness {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(join_error.to_string())),
            })?
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            self.connection.take();
            self.database_url.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "duckdb")]
mod duckdb_impl {
    use super::{duckdb_named_memory_database_path, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use duckdb::Connection;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    const RESOURCE_NAME: &str = "duckdb";

    /// In-process DuckDB for one scenario (no Docker).
    ///
    /// Opens a scenario-scoped database file (see [`super::duckdb_named_memory_database_path`]) so
    /// [`DuckDbBindings::database_url`] can be used to open further connections to the same logical
    /// database (helpers use [`tokio::task::spawn_blocking`]).
    ///
    /// `duckdb::Connection` is not [`Sync`]; it is wrapped in [`Arc`] [`Mutex`] so [`BuiltinResource`]
    /// remains [`Sync`].
    #[derive(Debug, Default)]
    pub struct DuckDbResource {
        connection: Option<Arc<Mutex<Connection>>>,
        database_url: String,
    }

    impl DuckDbResource {
        /// New DuckDB resource (connection opened on [`Resource::provision`]).
        pub fn new() -> Self {
            Self::default()
        }
    }

    #[async_trait]
    impl Resource for DuckDbResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.connection.is_some() {
                return Err(InfrastructureError::AlreadyProvisioned(
                    RESOURCE_NAME.into(),
                ));
            }

            let database_url = duckdb_named_memory_database_path(scenario_id.as_str());
            let open_path = database_url.clone();
            let connection = tokio::task::spawn_blocking(move || {
                Connection::open(Path::new(open_path.as_str()))
            })
            .await
            .map_err(|join_error| InfrastructureError::Provision {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(join_error.to_string())),
            })?
            .map_err(|source| InfrastructureError::Provision {
                resource: RESOURCE_NAME.into(),
                source: Box::new(source),
            })?;

            self.connection = Some(Arc::new(Mutex::new(connection)));
            self.database_url.clone_from(&database_url);

            Ok(vec![ResourceHandle {
                id: format!("duckdb_{scenario_id}"),
                connection_string: database_url.clone(),
                metadata: Some(serde_json::json!({
                    "database_url": database_url,
                    "engine": "duckdb",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let arc = self
                .connection
                .as_ref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            let arc = Arc::clone(arc);
            tokio::task::spawn_blocking(move || {
                let connection = arc
                    .lock()
                    .map_err(|poison| InfrastructureError::Readiness {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(poison.to_string())),
                    })?;
                connection
                    .query_row("SELECT 1", [], |row| row.get::<_, i32>(0))
                    .map_err(|source| InfrastructureError::Readiness {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(source),
                    })?;
                Ok::<(), InfrastructureError>(())
            })
            .await
            .map_err(|join_error| InfrastructureError::Readiness {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(join_error.to_string())),
            })?
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            self.connection.take();
            if !self.database_url.is_empty() {
                let _ = std::fs::remove_file(self.database_url.as_str());
            }
            self.database_url.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "fault-injection")]
mod toxiproxy_impl {
    //! Suite-shared Toxiproxy (`ghcr.io/shopify/toxiproxy`) for fault injection (SPECS §2).
    //!
    //! Container labels use [`crate::docker_labels`] (same strings as `diagnostics::labels`).

    use super::{Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use noxious_client::Client as ToxiproxyClient;
    use std::sync::OnceLock;
    use std::time::Duration;
    use testcontainers::core::IntoContainerPort;
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;
    use testcontainers::ImageExt;
    use tokio::sync::Mutex;

    use crate::docker_labels::{SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE};

    const RESOURCE_NAME: &str = "toxiproxy";
    const API_PORT: u16 = 8474;
    const DEFAULT_IMAGE_NAME: &str = "ghcr.io/shopify/toxiproxy";
    const DEFAULT_IMAGE_TAG: &str = "2.9.0";

    #[allow(clippy::large_enum_variant)]
    enum SuiteToxiproxyState {
        Idle,
        Running {
            api_base_url: String,
            /// Docker container id (hex) for [`bollard::Docker::connect_network`] (US-018).
            container_id: String,
            container: testcontainers::ContainerAsync<GenericImage>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_TOXIPROXY: OnceLock<Mutex<SuiteToxiproxyState>> = OnceLock::new();

    fn suite_toxiproxy_mutex() -> &'static Mutex<SuiteToxiproxyState> {
        SUITE_TOXIPROXY.get_or_init(|| Mutex::new(SuiteToxiproxyState::Idle))
    }

    fn image_fingerprint(image_name: &str, tag_override: &Option<String>) -> String {
        let tag = tag_override
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .unwrap_or(DEFAULT_IMAGE_TAG);
        format!("{image_name}:{tag}")
    }

    /// Docker-backed Toxiproxy using a suite-wide container (mirrors [`super::kafka_impl::KafkaResource`] / D-3).
    #[derive(Debug)]
    pub struct ToxiproxyResource {
        image_name: String,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        api_base_url: Option<String>,
    }

    impl Default for ToxiproxyResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ToxiproxyResource {
        /// Default image [`DEFAULT_IMAGE_NAME`] — suite broker starts on first provision.
        pub fn new() -> Self {
            Self {
                image_name: DEFAULT_IMAGE_NAME.to_string(),
                image_tag: None,
                holds_suite_ref: false,
                api_base_url: None,
            }
        }

        /// Override the image tag (e.g. pin a release). Image name stays [`DEFAULT_IMAGE_NAME`].
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag from `SHAKEDOWN_TOXIPROXY_IMAGE_TAG` when set (suite-level CI/prod pins).
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_TOXIPROXY_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Provision {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(error.to_string())),
            }
        }

        async fn get_version_ok(api_base_url: &str) -> Result<(), InfrastructureError> {
            let url = format!("{}/version", api_base_url.trim_end_matches('/'));
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(4))
                .build()
                .map_err(|source| InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(source),
                })?;
            const ATTEMPTS: u32 = 30;
            for attempt in 0..ATTEMPTS {
                match http.get(&url).send().await {
                    Ok(response) if response.status().is_success() => return Ok(()),
                    Ok(response) => {
                        if attempt + 1 == ATTEMPTS {
                            return Err(InfrastructureError::Readiness {
                                resource: RESOURCE_NAME.into(),
                                source: Box::new(std::io::Error::other(format!(
                                    "GET /version returned {}",
                                    response.status()
                                ))),
                            });
                        }
                    }
                    Err(err) => {
                        if attempt + 1 == ATTEMPTS {
                            return Err(InfrastructureError::Readiness {
                                resource: RESOURCE_NAME.into(),
                                source: Box::new(std::io::Error::other(err.to_string())),
                            });
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(InfrastructureError::Readiness {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(
                    "GET /version did not succeed in time",
                )),
            })
        }
    }

    #[async_trait]
    impl Resource for ToxiproxyResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned(
                    RESOURCE_NAME.into(),
                ));
            }

            let want_key = image_fingerprint(&self.image_name, &self.image_tag);

            let mut guard = suite_toxiproxy_mutex().lock().await;
            let api_base_url = match &mut *guard {
                SuiteToxiproxyState::Idle => {
                    let tag = self
                        .image_tag
                        .clone()
                        .unwrap_or_else(|| DEFAULT_IMAGE_TAG.to_string());
                    let image = GenericImage::new(self.image_name.clone(), tag)
                        .with_exposed_port(API_PORT.tcp())
                        .with_wait_for(WaitFor::seconds(2))
                        .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE);
                    let node = image.start().await.map_err(Self::map_tc_err)?;
                    let port = node
                        .get_host_port_ipv4(API_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let api_base_url = format!("http://127.0.0.1:{port}");
                    let container_id = node.id().to_string();
                    *guard = SuiteToxiproxyState::Running {
                        api_base_url: api_base_url.clone(),
                        container_id,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    api_base_url
                }
                SuiteToxiproxyState::Running {
                    api_base_url,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "Toxiproxy suite already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    api_base_url.clone()
                }
            };
            drop(guard);

            self.api_base_url = Some(api_base_url.clone());
            self.holds_suite_ref = true;

            Ok(vec![ResourceHandle {
                id: format!("toxiproxy_{scenario_id}"),
                connection_string: api_base_url.clone(),
                metadata: Some(serde_json::json!({
                    "engine": "toxiproxy",
                    "api_url": api_base_url,
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let api_base_url = self
                .api_base_url
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            Self::get_version_ok(api_base_url).await
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.api_base_url = None;
                return Ok(());
            }

            let api_base = self
                .api_base_url
                .take()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;

            let client = ToxiproxyClient::new(&api_base);
            let proxies =
                client
                    .proxies()
                    .await
                    .map_err(|source| InfrastructureError::Teardown {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(source),
                    })?;
            for (_name, proxy) in proxies {
                proxy
                    .delete()
                    .await
                    .map_err(|source| InfrastructureError::Teardown {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(source),
                    })?;
            }

            let mut guard = suite_toxiproxy_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
            match &mut *guard {
                SuiteToxiproxyState::Idle => {}
                SuiteToxiproxyState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteToxiproxyState::Idle);
                        if let SuiteToxiproxyState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_rm {
                container
                    .rm()
                    .await
                    .map_err(|remove_error| InfrastructureError::Teardown {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(remove_error.to_string())),
                    })?;
            }

            self.holds_suite_ref = false;
            self.api_base_url = None;
            Ok(())
        }
    }

    pub(crate) async fn toxiproxy_suite_container_id() -> Option<String> {
        let guard = suite_toxiproxy_mutex().lock().await;
        match &*guard {
            SuiteToxiproxyState::Idle => None,
            SuiteToxiproxyState::Running { container_id, .. } => Some(container_id.clone()),
        }
    }
}

#[cfg(feature = "fault-injection")]
pub(crate) async fn toxiproxy_suite_container_id() -> Option<String> {
    toxiproxy_impl::toxiproxy_suite_container_id().await
}

#[cfg(feature = "elasticsearch")]
mod elasticsearch_impl {
    use super::{sanitize_elasticsearch_index_prefix, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use crate::search_operations::SearchOperations;
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers::core::{Image, ImageExt};
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::elastic_search::{ElasticSearch, ELASTICSEARCH_API_PORT};
    use tokio::sync::Mutex;

    const RESOURCE_NAME: &str = "elasticsearch";
    /// Default Elasticsearch image tag (`docker.elastic.co/elasticsearch/elasticsearch:8`).
    pub const DEFAULT_ELASTICSEARCH_IMAGE_TAG: &str = "8";
    const SUITE_SECURITY_USER: &str = "elastic";
    const SUITE_SECURITY_PASSWORD: &str = "shakedown";

    #[allow(clippy::large_enum_variant)]
    enum SuiteElasticsearchState {
        Idle,
        Running {
            host: String,
            port: u16,
            container: testcontainers::ContainerAsync<ElasticSearch>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_ELASTICSEARCH: OnceLock<Mutex<SuiteElasticsearchState>> = OnceLock::new();

    fn suite_elasticsearch_mutex() -> &'static Mutex<SuiteElasticsearchState> {
        SUITE_ELASTICSEARCH.get_or_init(|| Mutex::new(SuiteElasticsearchState::Idle))
    }

    fn image_fingerprint(image: &ElasticSearch, tag_override: &Option<String>) -> String {
        let tag = tag_override
            .as_deref()
            .unwrap_or(DEFAULT_ELASTICSEARCH_IMAGE_TAG);
        format!("{}:{}", image.name(), tag)
    }

    /// Docker-backed Elasticsearch (`docker.elastic.co/elasticsearch/elasticsearch` via testcontainers-modules).
    ///
    /// **Suite lifecycle:** one node per process; per-scenario index prefix; refcounted teardown deletes
    /// matching indices then may remove the container.
    ///
    /// Default tag **`8`**. Override with [`Self::with_image_tag`] or `SHAKEDOWN_ELASTICSEARCH_IMAGE_TAG`.
    /// Security is **off** by default (`xpack.security.enabled=false`). Use [`Self::with_security`] for HTTP basic auth (`elastic` / `shakedown`).
    #[derive(Debug)]
    pub struct ElasticsearchResource {
        image: ElasticSearch,
        image_tag: Option<String>,
        security_enabled: bool,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        http_url: String,
        index_prefix: String,
        username: Option<String>,
        password: Option<String>,
    }

    impl Default for ElasticsearchResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ElasticsearchResource {
        /// Create default settings (suite container started on first provision; security off).
        pub fn new() -> Self {
            Self {
                image: ElasticSearch::default(),
                image_tag: Some(DEFAULT_ELASTICSEARCH_IMAGE_TAG.to_string()),
                security_enabled: false,
                holds_suite_ref: false,
                host: None,
                port: None,
                http_url: String::new(),
                index_prefix: String::new(),
                username: None,
                password: None,
            }
        }

        /// Enable X-Pack security with fixed `elastic` / `shakedown` credentials (D-13).
        pub fn with_security(mut self, enabled: bool) -> Self {
            self.security_enabled = enabled;
            self
        }

        /// Override the image tag (pin `8.11.0` or another tag). Image name stays the Elastic official registry path.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_ELASTICSEARCH_IMAGE_TAG` when set.
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_ELASTICSEARCH_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        }

        fn search_client(&self) -> SearchOperations {
            SearchOperations::new(
                self.http_url.clone(),
                self.username.clone(),
                self.password.clone(),
            )
        }
    }

    #[async_trait]
    impl Resource for ElasticsearchResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned(
                    RESOURCE_NAME.into(),
                ));
            }

            let index_prefix = sanitize_elasticsearch_index_prefix(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image, &self.image_tag);

            let mut guard = suite_elasticsearch_mutex().lock().await;
            let (host, port) = match &mut *guard {
                SuiteElasticsearchState::Idle => {
                    let tag = self
                        .image_tag
                        .as_deref()
                        .unwrap_or(DEFAULT_ELASTICSEARCH_IMAGE_TAG);
                    let mut start_image = self.image.clone().with_tag(tag.to_string());
                    start_image = start_image
                        .with_env_var("discovery.type", "single-node")
                        .with_env_var("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
                    if self.security_enabled {
                        start_image = start_image
                            .with_env_var("xpack.security.enabled", "true")
                            .with_env_var("ELASTIC_PASSWORD", SUITE_SECURITY_PASSWORD);
                    } else {
                        start_image = start_image.with_env_var("xpack.security.enabled", "false");
                    }
                    let node = start_image
                        .with_label(
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                        )
                        .start()
                        .await
                        .map_err(Self::map_tc_err)?;

                    let mapped_port = node
                        .get_host_port_ipv4(ELASTICSEARCH_API_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();

                    *guard = SuiteElasticsearchState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port)
                }
                SuiteElasticsearchState::Running {
                    host,
                    port,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "Elasticsearch suite already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port)
                }
            };
            drop(guard);

            let http_url = format!("http://{host}:{port}");
            let (username, password) = if self.security_enabled {
                (
                    Some(SUITE_SECURITY_USER.to_string()),
                    Some(SUITE_SECURITY_PASSWORD.to_string()),
                )
            } else {
                (None, None)
            };

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.http_url = http_url.clone();
            self.index_prefix = index_prefix.clone();
            self.username = username.clone();
            self.password = password.clone();

            Ok(vec![ResourceHandle {
                id: format!("elasticsearch_{scenario_id}"),
                connection_string: http_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "url": http_url,
                    "index_prefix": index_prefix,
                    "username": username,
                    "password": password,
                    "engine": "elasticsearch",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let _ = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            let _ = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;

            let client = self.search_client();
            let health =
                client
                    .cluster_health()
                    .await
                    .map_err(|error| InfrastructureError::Readiness {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    })?;
            let status = health
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            if status == "red" {
                return Err(InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(format!(
                        "cluster health status is red: {health}"
                    ))),
                });
            }
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.http_url.clear();
                self.index_prefix.clear();
                self.username = None;
                self.password = None;
                return Ok(());
            }

            let index_prefix = self.index_prefix.clone();
            let http_url = self.http_url.clone();
            let username = self.username.clone();
            let password = self.password.clone();

            if !index_prefix.is_empty() && !http_url.is_empty() {
                let client = SearchOperations::new(http_url, username, password);
                if let Err(error) = client
                    .delete_indices_with_prefix(index_prefix.as_str())
                    .await
                {
                    return Err(InfrastructureError::Teardown {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    });
                }
            }

            let mut guard = suite_elasticsearch_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<ElasticSearch>> = None;
            match &mut *guard {
                SuiteElasticsearchState::Idle => {}
                SuiteElasticsearchState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteElasticsearchState::Idle);
                        if let SuiteElasticsearchState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_rm {
                container.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.http_url.clear();
            self.index_prefix.clear();
            self.username = None;
            self.password = None;
            Ok(())
        }
    }
}

#[cfg(feature = "opensearch")]
mod opensearch_impl {
    use super::{sanitize_elasticsearch_index_prefix, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use crate::search_operations::SearchOperations;
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers::core::IntoContainerPort;
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;
    use testcontainers::ImageExt;
    use tokio::sync::Mutex;

    const RESOURCE_NAME: &str = "opensearch";
    /// Default image repository (`opensearchproject/opensearch`).
    pub const DEFAULT_OPENSEARCH_IMAGE_REPOSITORY: &str = "opensearchproject/opensearch";
    /// Default OpenSearch image tag (`opensearchproject/opensearch:2`).
    pub const DEFAULT_OPENSEARCH_IMAGE_TAG: &str = "2";
    const OPENSEARCH_HTTP_PORT: u16 = 9200;
    const SUITE_SECURITY_USER: &str = "admin";
    const SUITE_SECURITY_PASSWORD: &str = "shakedown";

    #[allow(clippy::large_enum_variant)]
    enum SuiteOpenSearchState {
        Idle,
        Running {
            host: String,
            port: u16,
            container: testcontainers::ContainerAsync<GenericImage>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_OPENSEARCH: OnceLock<Mutex<SuiteOpenSearchState>> = OnceLock::new();

    fn suite_opensearch_mutex() -> &'static Mutex<SuiteOpenSearchState> {
        SUITE_OPENSEARCH.get_or_init(|| Mutex::new(SuiteOpenSearchState::Idle))
    }

    fn image_fingerprint(repository: &str, tag_override: &Option<String>) -> String {
        let tag = tag_override
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .unwrap_or(DEFAULT_OPENSEARCH_IMAGE_TAG);
        format!("{repository}:{tag}")
    }

    /// Docker-backed OpenSearch (`opensearchproject/opensearch` via [`GenericImage`]).
    ///
    /// **Suite lifecycle:** one node per process; per-scenario index prefix; refcounted teardown deletes
    /// matching indices then may remove the container.
    ///
    /// Default tag **`2`**. Override with [`Self::with_image_tag`] or `SHAKEDOWN_OPENSEARCH_IMAGE_TAG`.
    /// Security is **off** by default (`plugins.security.disabled=true`). Use [`Self::with_security`] for HTTP basic auth (`admin` / `shakedown`).
    #[derive(Debug)]
    pub struct OpenSearchResource {
        image_repository: String,
        image_tag: Option<String>,
        security_enabled: bool,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        http_url: String,
        index_prefix: String,
        username: Option<String>,
        password: Option<String>,
    }

    impl Default for OpenSearchResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl OpenSearchResource {
        /// Create default settings (suite container started on first provision; security off).
        pub fn new() -> Self {
            Self {
                image_repository: DEFAULT_OPENSEARCH_IMAGE_REPOSITORY.to_string(),
                image_tag: Some(DEFAULT_OPENSEARCH_IMAGE_TAG.to_string()),
                security_enabled: false,
                holds_suite_ref: false,
                host: None,
                port: None,
                http_url: String::new(),
                index_prefix: String::new(),
                username: None,
                password: None,
            }
        }

        /// Enable the security plugin with fixed `admin` / `shakedown` credentials (D-13).
        pub fn with_security(mut self, enabled: bool) -> Self {
            self.security_enabled = enabled;
            self
        }

        /// Override the image tag (pin `2.11.0` or another tag). Repository stays [`DEFAULT_OPENSEARCH_IMAGE_REPOSITORY`].
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_OPENSEARCH_IMAGE_TAG` when set.
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_OPENSEARCH_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        }

        fn search_client(&self) -> SearchOperations {
            SearchOperations::new(
                self.http_url.clone(),
                self.username.clone(),
                self.password.clone(),
            )
        }
    }

    #[async_trait]
    impl Resource for OpenSearchResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned(
                    RESOURCE_NAME.into(),
                ));
            }

            let index_prefix = sanitize_elasticsearch_index_prefix(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image_repository, &self.image_tag);

            let mut guard = suite_opensearch_mutex().lock().await;
            let (host, port) = match &mut *guard {
                SuiteOpenSearchState::Idle => {
                    let repository = self.image_repository.as_str();
                    let tag = self
                        .image_tag
                        .as_deref()
                        .map(str::trim)
                        .filter(|t| !t.is_empty())
                        .unwrap_or(DEFAULT_OPENSEARCH_IMAGE_TAG);
                    let mut start_image = GenericImage::new(repository, tag)
                        .with_exposed_port(OPENSEARCH_HTTP_PORT.tcp())
                        .with_wait_for(WaitFor::healthcheck())
                        .with_env_var("discovery.type", "single-node")
                        .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
                        .with_label(
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                        );
                    if self.security_enabled {
                        start_image = start_image
                            .with_env_var("plugins.security.disabled", "false")
                            .with_env_var(
                                "OPENSEARCH_INITIAL_ADMIN_PASSWORD",
                                SUITE_SECURITY_PASSWORD,
                            );
                    } else {
                        start_image = start_image.with_env_var("plugins.security.disabled", "true");
                    }
                    let node = start_image.start().await.map_err(Self::map_tc_err)?;

                    let mapped_port = node
                        .get_host_port_ipv4(OPENSEARCH_HTTP_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();

                    *guard = SuiteOpenSearchState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port)
                }
                SuiteOpenSearchState::Running {
                    host,
                    port,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "OpenSearch suite already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port)
                }
            };
            drop(guard);

            let http_url = format!("http://{host}:{port}");
            let (username, password) = if self.security_enabled {
                (
                    Some(SUITE_SECURITY_USER.to_string()),
                    Some(SUITE_SECURITY_PASSWORD.to_string()),
                )
            } else {
                (None, None)
            };

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.http_url = http_url.clone();
            self.index_prefix = index_prefix.clone();
            self.username = username.clone();
            self.password = password.clone();

            Ok(vec![ResourceHandle {
                id: format!("opensearch_{scenario_id}"),
                connection_string: http_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "url": http_url,
                    "index_prefix": index_prefix,
                    "username": username,
                    "password": password,
                    "engine": "opensearch",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let _ = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            let _ = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;

            let client = self.search_client();
            let health =
                client
                    .cluster_health()
                    .await
                    .map_err(|error| InfrastructureError::Readiness {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    })?;
            let status = health
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            if status == "red" {
                return Err(InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(format!(
                        "cluster health status is red: {health}"
                    ))),
                });
            }
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.http_url.clear();
                self.index_prefix.clear();
                self.username = None;
                self.password = None;
                return Ok(());
            }

            let index_prefix = self.index_prefix.clone();
            let http_url = self.http_url.clone();
            let username = self.username.clone();
            let password = self.password.clone();

            if !index_prefix.is_empty() && !http_url.is_empty() {
                let client = SearchOperations::new(http_url, username, password);
                if let Err(error) = client
                    .delete_indices_with_prefix(index_prefix.as_str())
                    .await
                {
                    return Err(InfrastructureError::Teardown {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(error.to_string())),
                    });
                }
            }

            let mut guard = suite_opensearch_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
            match &mut *guard {
                SuiteOpenSearchState::Idle => {}
                SuiteOpenSearchState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteOpenSearchState::Idle);
                        if let SuiteOpenSearchState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_rm {
                container.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.http_url.clear();
            self.index_prefix.clear();
            self.username = None;
            self.password = None;
            Ok(())
        }
    }
}

#[cfg(feature = "qdrant")]
mod qdrant_impl {
    use super::{sanitize_elasticsearch_index_prefix, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers::core::IntoContainerPort;
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;
    use testcontainers::ImageExt;
    use tokio::sync::Mutex;

    const RESOURCE_NAME: &str = "qdrant";
    /// Default image repository (`qdrant/qdrant`).
    pub const DEFAULT_QDRANT_IMAGE_REPOSITORY: &str = "qdrant/qdrant";
    /// Default image tag (`qdrant/qdrant:1`).
    pub const DEFAULT_QDRANT_IMAGE_TAG: &str = "1";
    const QDRANT_REST_PORT: u16 = 6333;
    const QDRANT_GRPC_PORT: u16 = 6334;

    #[allow(clippy::large_enum_variant)]
    enum SuiteQdrantState {
        Idle,
        Running {
            host: String,
            rest_port: u16,
            grpc_port: u16,
            container: testcontainers::ContainerAsync<GenericImage>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_QDRANT: OnceLock<Mutex<SuiteQdrantState>> = OnceLock::new();

    fn suite_qdrant_mutex() -> &'static Mutex<SuiteQdrantState> {
        SUITE_QDRANT.get_or_init(|| Mutex::new(SuiteQdrantState::Idle))
    }

    fn image_fingerprint(repository: &str, tag_override: &Option<String>) -> String {
        let tag = tag_override
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .unwrap_or(DEFAULT_QDRANT_IMAGE_TAG);
        format!("{repository}:{tag}")
    }

    async fn delete_collections_with_prefix(rest_url: &str, prefix: &str) -> Result<(), String> {
        let client = reqwest::Client::new();
        let base = rest_url.trim_end_matches('/');
        let list_url = format!("{base}/collections");
        let response = client
            .get(&list_url)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if !response.status().is_success() {
            return Err(format!(
                "list collections failed: HTTP {}",
                response.status()
            ));
        }
        let body: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
        let names: Vec<String> = body
            .get("result")
            .and_then(|result| result.get("collections"))
            .and_then(|collections| collections.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.get("name").and_then(|name| name.as_str()))
                    .filter(|name| name.starts_with(prefix))
                    .map(std::string::ToString::to_string)
                    .collect()
            })
            .unwrap_or_default();
        for name in names {
            let delete_url = format!("{base}/collections/{}", name);
            let delete_response = client
                .delete(&delete_url)
                .send()
                .await
                .map_err(|error| error.to_string())?;
            if !delete_response.status().is_success() {
                return Err(format!(
                    "delete collection {name} failed: HTTP {}",
                    delete_response.status()
                ));
            }
        }
        Ok(())
    }

    /// Docker-backed Qdrant (`qdrant/qdrant` via [`GenericImage`]).
    ///
    /// **Suite lifecycle:** one node per process; per-scenario collection prefix; refcounted teardown
    /// deletes matching collections then may remove the container.
    ///
    /// Default tag **`1`**. Override with [`Self::with_image_tag`] or `SHAKEDOWN_QDRANT_IMAGE_TAG`.
    /// Readiness uses HTTP [`GET /readyz`](https://qdrant.tech/documentation/guides/installation/) on the REST port (D-12).
    #[derive(Debug)]
    pub struct QdrantResource {
        image_repository: String,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        rest_port: Option<u16>,
        grpc_port: Option<u16>,
        rest_url: String,
        grpc_url: String,
        collection_prefix: String,
    }

    impl Default for QdrantResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl QdrantResource {
        /// Create default settings (suite container started on first provision).
        pub fn new() -> Self {
            Self {
                image_repository: DEFAULT_QDRANT_IMAGE_REPOSITORY.to_string(),
                image_tag: Some(DEFAULT_QDRANT_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                rest_port: None,
                grpc_port: None,
                rest_url: String::new(),
                grpc_url: String::new(),
                collection_prefix: String::new(),
            }
        }

        /// Override the image tag. Repository stays [`DEFAULT_QDRANT_IMAGE_REPOSITORY`].
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_QDRANT_IMAGE_TAG` when set (may be `qdrant/qdrant:1` or `1`).
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_QDRANT_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        }
    }

    #[async_trait]
    impl Resource for QdrantResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned(
                    RESOURCE_NAME.into(),
                ));
            }

            let collection_prefix = sanitize_elasticsearch_index_prefix(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image_repository, &self.image_tag);

            let mut guard = suite_qdrant_mutex().lock().await;
            let (host, rest_port, grpc_port) = match &mut *guard {
                SuiteQdrantState::Idle => {
                    let repository = self.image_repository.as_str();
                    let tag = self
                        .image_tag
                        .as_deref()
                        .map(str::trim)
                        .filter(|t| !t.is_empty())
                        .unwrap_or(DEFAULT_QDRANT_IMAGE_TAG);
                    let start_image = GenericImage::new(repository, tag)
                        .with_exposed_port(QDRANT_REST_PORT.tcp())
                        .with_exposed_port(QDRANT_GRPC_PORT.tcp())
                        .with_wait_for(WaitFor::seconds(3))
                        .with_label(
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                        );
                    let node = start_image.start().await.map_err(Self::map_tc_err)?;

                    let mapped_rest = node
                        .get_host_port_ipv4(QDRANT_REST_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let mapped_grpc = node
                        .get_host_port_ipv4(QDRANT_GRPC_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();

                    *guard = SuiteQdrantState::Running {
                        host: host.clone(),
                        rest_port: mapped_rest,
                        grpc_port: mapped_grpc,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_rest, mapped_grpc)
                }
                SuiteQdrantState::Running {
                    host,
                    rest_port,
                    grpc_port,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "Qdrant suite already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *rest_port, *grpc_port)
                }
            };
            drop(guard);

            let rest_url = format!("http://{host}:{rest_port}");
            let grpc_url = format!("http://{host}:{grpc_port}");

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.rest_port = Some(rest_port);
            self.grpc_port = Some(grpc_port);
            self.rest_url = rest_url.clone();
            self.grpc_url = grpc_url.clone();
            self.collection_prefix = collection_prefix.clone();

            Ok(vec![ResourceHandle {
                id: format!("qdrant_{scenario_id}"),
                connection_string: grpc_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "rest_port": rest_port,
                    "grpc_port": grpc_port,
                    "rest_url": rest_url,
                    "grpc_url": grpc_url,
                    "collection_prefix": collection_prefix,
                    "engine": "qdrant",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let host = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            let _ = self
                .rest_port
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            let _ = host;

            let ready_url = format!("{}/readyz", self.rest_url.trim_end_matches('/'));
            let response = reqwest::Client::new()
                .get(&ready_url)
                .send()
                .await
                .map_err(|error| InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
            if !response.status().is_success() {
                return Err(InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(format!(
                        "GET /readyz returned HTTP {}",
                        response.status()
                    ))),
                });
            }
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.rest_port = None;
                self.grpc_port = None;
                self.rest_url.clear();
                self.grpc_url.clear();
                self.collection_prefix.clear();
                return Ok(());
            }

            let collection_prefix = self.collection_prefix.clone();
            let rest_url = self.rest_url.clone();

            if !collection_prefix.is_empty() && !rest_url.is_empty() {
                if let Err(error) =
                    delete_collections_with_prefix(rest_url.as_str(), collection_prefix.as_str())
                        .await
                {
                    return Err(InfrastructureError::Teardown {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(error)),
                    });
                }
            }

            let mut guard = suite_qdrant_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
            match &mut *guard {
                SuiteQdrantState::Idle => {}
                SuiteQdrantState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteQdrantState::Idle);
                        if let SuiteQdrantState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_rm {
                container.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.rest_port = None;
            self.grpc_port = None;
            self.rest_url.clear();
            self.grpc_url.clear();
            self.collection_prefix.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "weaviate")]
mod weaviate_impl {
    use super::{sanitize_elasticsearch_index_prefix, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers::core::IntoContainerPort;
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;
    use testcontainers::ImageExt;
    use tokio::sync::Mutex;

    const RESOURCE_NAME: &str = "weaviate";
    /// Default image repository (`semitechnologies/weaviate`, same as testcontainers-modules/weaviate).
    pub const DEFAULT_WEAVIATE_IMAGE_REPOSITORY: &str = "semitechnologies/weaviate";
    /// Default image tag (`1.28.2`, aligned with testcontainers-modules/weaviate).
    pub const DEFAULT_WEAVIATE_IMAGE_TAG: &str = "1.28.2";
    const WEAVIATE_HTTP_PORT: u16 = 8080;
    const WEAVIATE_GRPC_PORT: u16 = 50051;

    #[allow(clippy::large_enum_variant)]
    enum SuiteWeaviateState {
        Idle,
        Running {
            host: String,
            port: u16,
            container: testcontainers::ContainerAsync<GenericImage>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_WEAVIATE: OnceLock<Mutex<SuiteWeaviateState>> = OnceLock::new();

    fn suite_weaviate_mutex() -> &'static Mutex<SuiteWeaviateState> {
        SUITE_WEAVIATE.get_or_init(|| Mutex::new(SuiteWeaviateState::Idle))
    }

    fn image_fingerprint(tag_override: &Option<String>) -> String {
        let tag = tag_override
            .as_deref()
            .map(str::trim)
            .filter(|tag| !tag.is_empty())
            .unwrap_or(DEFAULT_WEAVIATE_IMAGE_TAG);
        format!("{}:{tag}", DEFAULT_WEAVIATE_IMAGE_REPOSITORY)
    }

    fn class_delete_prefix(class_prefix: &str) -> String {
        let mut chars = class_prefix.chars();
        match chars.next() {
            None => String::new(),
            Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
        }
    }

    async fn delete_classes_with_prefix(rest_url: &str, class_prefix: &str) -> Result<(), String> {
        let delete_prefix = class_delete_prefix(class_prefix);
        if delete_prefix.is_empty() {
            return Ok(());
        }
        let client = reqwest::Client::new();
        let base = rest_url.trim_end_matches('/');
        let schema_url = format!("{base}/v1/schema");
        let response = client
            .get(&schema_url)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if !response.status().is_success() {
            return Err(format!("list schema failed: HTTP {}", response.status()));
        }
        let body: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
        let names: Vec<String> = body
            .get("classes")
            .and_then(|classes| classes.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.get("class").and_then(|name| name.as_str()))
                    .filter(|name| name.starts_with(&delete_prefix))
                    .map(std::string::ToString::to_string)
                    .collect()
            })
            .unwrap_or_default();
        for class_name in names {
            let delete_url = format!("{base}/v1/schema/{}", class_name);
            let delete_response = client
                .delete(&delete_url)
                .send()
                .await
                .map_err(|error| error.to_string())?;
            if !delete_response.status().is_success() {
                return Err(format!(
                    "delete class {class_name} failed: HTTP {}",
                    delete_response.status()
                ));
            }
        }
        Ok(())
    }

    /// Docker-backed Weaviate (`semitechnologies/weaviate` via [`GenericImage`], tag aligned with
    /// `testcontainers-modules/weaviate`).
    ///
    /// **Suite lifecycle:** one node per process; per-scenario class name prefix; refcounted teardown
    /// deletes matching schema classes then may remove the container.
    ///
    /// Startup uses a short fixed wait (like Qdrant); [`Resource::check_ready`] performs HTTP
    /// `GET /v1/.well-known/ready` so flakes from container HTTP wait strategies are avoided.
    ///
    /// Default tag **`1.28.2`**. Override with [`Self::with_image_tag`] or `SHAKEDOWN_WEAVIATE_IMAGE_TAG`.
    /// The container enables anonymous access (`AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true`) per SPECS.
    #[derive(Debug)]
    pub struct WeaviateResource {
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        rest_url: String,
        class_prefix: String,
    }

    impl Default for WeaviateResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl WeaviateResource {
        /// Create default settings (suite container started on first provision).
        pub fn new() -> Self {
            Self {
                image_tag: Some(DEFAULT_WEAVIATE_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                port: None,
                rest_url: String::new(),
                class_prefix: String::new(),
            }
        }

        /// Override the image tag (e.g. `1.28.2`). Image name stays `semitechnologies/weaviate`.
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from `SHAKEDOWN_WEAVIATE_IMAGE_TAG` when set (tag only or `repo:tag`).
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_WEAVIATE_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        }
    }

    #[async_trait]
    impl Resource for WeaviateResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned(
                    RESOURCE_NAME.into(),
                ));
            }

            let class_prefix = sanitize_elasticsearch_index_prefix(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image_tag);

            let mut guard = suite_weaviate_mutex().lock().await;
            let (host, port) = match &mut *guard {
                SuiteWeaviateState::Idle => {
                    let tag = self
                        .image_tag
                        .as_deref()
                        .map(str::trim)
                        .filter(|tag| !tag.is_empty())
                        .unwrap_or(DEFAULT_WEAVIATE_IMAGE_TAG);
                    let start_image = GenericImage::new(DEFAULT_WEAVIATE_IMAGE_REPOSITORY, tag)
                        .with_exposed_port(WEAVIATE_HTTP_PORT.tcp())
                        .with_exposed_port(WEAVIATE_GRPC_PORT.tcp())
                        .with_wait_for(WaitFor::seconds(10))
                        .with_env_var("AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED", "true")
                        .with_env_var("PERSISTENCE_DATA_PATH", "/tmp/weaviate")
                        .with_label(
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                        );
                    let node = start_image.start().await.map_err(Self::map_tc_err)?;

                    let mapped_port = node
                        .get_host_port_ipv4(WEAVIATE_HTTP_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();

                    *guard = SuiteWeaviateState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port)
                }
                SuiteWeaviateState::Running {
                    host,
                    port,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "Weaviate suite already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port)
                }
            };
            drop(guard);

            let rest_url = format!("http://{host}:{port}");

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.rest_url = rest_url.clone();
            self.class_prefix = class_prefix.clone();

            Ok(vec![ResourceHandle {
                id: format!("weaviate_{scenario_id}"),
                connection_string: rest_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "rest_url": rest_url,
                    "class_prefix": class_prefix,
                    "engine": "weaviate",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let _ = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            let _ = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;

            let ready_url = format!(
                "{}/v1/.well-known/ready",
                self.rest_url.trim_end_matches('/')
            );
            let response = reqwest::Client::new()
                .get(&ready_url)
                .send()
                .await
                .map_err(|error| InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
            if !response.status().is_success() {
                return Err(InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(format!(
                        "GET /v1/.well-known/ready returned HTTP {}",
                        response.status()
                    ))),
                });
            }
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.rest_url.clear();
                self.class_prefix.clear();
                return Ok(());
            }

            let class_prefix = self.class_prefix.clone();
            let rest_url = self.rest_url.clone();

            if !class_prefix.is_empty() && !rest_url.is_empty() {
                if let Err(error) =
                    delete_classes_with_prefix(rest_url.as_str(), class_prefix.as_str()).await
                {
                    return Err(InfrastructureError::Teardown {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(error)),
                    });
                }
            }

            let mut guard = suite_weaviate_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
            match &mut *guard {
                SuiteWeaviateState::Idle => {}
                SuiteWeaviateState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteWeaviateState::Idle);
                        if let SuiteWeaviateState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_rm {
                container.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.rest_url.clear();
            self.class_prefix.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "influxdb")]
mod influxdb_impl {
    use super::{sanitize_sql_database_name, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers::core::IntoContainerPort;
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;
    use testcontainers::ImageExt;
    use tokio::sync::Mutex;

    const RESOURCE_NAME: &str = "influxdb";
    /// Default image repository (`influxdb`).
    pub const DEFAULT_INFLUXDB_IMAGE_REPOSITORY: &str = "influxdb";
    /// Default image tag (`2` — InfluxDB 2.x).
    pub const DEFAULT_INFLUXDB_IMAGE_TAG: &str = "2";
    const INFLUX_HTTP_PORT: u16 = 8086;
    /// Fixed org name from container init (OQ-4.1 / SPECS).
    pub const SUITE_ORGANIZATION: &str = "shakedown";
    /// Fixed admin token from `DOCKER_INFLUXDB_INIT_ADMIN_TOKEN` (OQ-4.1).
    pub const SUITE_ADMIN_TOKEN: &str = "shakedown-admin-token";

    #[allow(clippy::large_enum_variant)]
    enum SuiteInfluxDbState {
        Idle,
        Running {
            host: String,
            port: u16,
            container: testcontainers::ContainerAsync<GenericImage>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_INFLUXDB: OnceLock<Mutex<SuiteInfluxDbState>> = OnceLock::new();

    fn suite_influxdb_mutex() -> &'static Mutex<SuiteInfluxDbState> {
        SUITE_INFLUXDB.get_or_init(|| Mutex::new(SuiteInfluxDbState::Idle))
    }

    fn image_fingerprint(repository: &str, tag_override: &Option<String>) -> String {
        let tag = tag_override
            .as_deref()
            .map(str::trim)
            .filter(|tag| !tag.is_empty())
            .unwrap_or(DEFAULT_INFLUXDB_IMAGE_TAG);
        format!("{repository}:{tag}")
    }

    fn authorization_value(token: &str) -> String {
        format!("Token {token}")
    }

    async fn influx_health_passes(base_url: &str) -> Result<(), String> {
        let client = reqwest::Client::new();
        let url = format!("{}/health", base_url.trim_end_matches('/'));
        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if !response.status().is_success() {
            return Err(format!("GET /health returned HTTP {}", response.status()));
        }
        let body: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
        let status = body
            .get("status")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if status != "pass" {
            return Err(format!("influx health status is not pass: {body}"));
        }
        Ok(())
    }

    async fn fetch_organization_id(
        base_url: &str,
        admin_token: &str,
        org_name: &str,
    ) -> Result<String, String> {
        let client = reqwest::Client::new();
        let url = format!(
            "{}/api/v2/orgs?org={}",
            base_url.trim_end_matches('/'),
            org_name
        );
        let response = client
            .get(&url)
            .header(
                reqwest::header::AUTHORIZATION,
                authorization_value(admin_token),
            )
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if !response.status().is_success() {
            return Err(format!(
                "GET /api/v2/orgs returned HTTP {}",
                response.status()
            ));
        }
        let body: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
        let org_id = body
            .get("orgs")
            .and_then(|orgs| orgs.as_array())
            .and_then(|items| items.first())
            .and_then(|org| org.get("id"))
            .and_then(|id| id.as_str())
            .ok_or_else(|| format!("orgs response missing id: {body}"))?;
        Ok(org_id.to_string())
    }

    async fn create_bucket(
        base_url: &str,
        admin_token: &str,
        organization_id: &str,
        bucket_name: &str,
    ) -> Result<String, String> {
        let client = reqwest::Client::new();
        let url = format!("{}/api/v2/buckets", base_url.trim_end_matches('/'));
        let payload = serde_json::json!({
            "orgID": organization_id,
            "name": bucket_name,
            "retentionRules": []
        });
        let response = client
            .post(&url)
            .header(
                reqwest::header::AUTHORIZATION,
                authorization_value(admin_token),
            )
            .json(&payload)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(format!("POST /api/v2/buckets failed: HTTP {status} {text}",));
        }
        let body: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
        let bucket_id = body
            .get("id")
            .and_then(|id| id.as_str())
            .ok_or_else(|| format!("bucket create response missing id: {body}"))?;
        Ok(bucket_id.to_string())
    }

    async fn delete_bucket_by_id(
        base_url: &str,
        admin_token: &str,
        bucket_id: &str,
    ) -> Result<(), String> {
        let client = reqwest::Client::new();
        let url = format!(
            "{}/api/v2/buckets/{}",
            base_url.trim_end_matches('/'),
            bucket_id
        );
        let response = client
            .delete(&url)
            .header(
                reqwest::header::AUTHORIZATION,
                authorization_value(admin_token),
            )
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(format!(
                "DELETE /api/v2/buckets failed: HTTP {status} {text}",
            ));
        }
        Ok(())
    }

    /// Docker-backed InfluxDB 2.x (`influxdb:2` via [`GenericImage`]).
    ///
    /// **Suite lifecycle:** one node per process; per-scenario bucket via HTTP API; refcounted teardown
    /// deletes the bucket then may remove the container.
    ///
    /// Default tag **`2`**. Override with [`Self::with_image_tag`] or **`SHAKEDOWN_INFLUXDB_IMAGE_TAG`**
    /// via [`Self::with_image_tag_from_env`]. Container init uses fixed org **`shakedown`** and admin
    /// token **`shakedown-admin-token`** (see `DOCKER_INFLUXDB_INIT_*` env in [`Resource::provision`]).
    #[derive(Debug)]
    pub struct InfluxDbResource {
        image_repository: String,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        /// API base URL, e.g. `http://127.0.0.1:8086`.
        url: String,
        organization: String,
        token: String,
        bucket_name: String,
        bucket_id: String,
    }

    impl Default for InfluxDbResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl InfluxDbResource {
        /// Create default settings (suite container started on first provision).
        pub fn new() -> Self {
            Self {
                image_repository: DEFAULT_INFLUXDB_IMAGE_REPOSITORY.to_string(),
                image_tag: Some(DEFAULT_INFLUXDB_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                port: None,
                url: String::new(),
                organization: SUITE_ORGANIZATION.to_string(),
                token: SUITE_ADMIN_TOKEN.to_string(),
                bucket_name: String::new(),
                bucket_id: String::new(),
            }
        }

        /// Override the image tag. Repository stays [`DEFAULT_INFLUXDB_IMAGE_REPOSITORY`].
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Apply tag override from **`SHAKEDOWN_INFLUXDB_IMAGE_TAG`** when set (e.g. `2` or `2.7`).
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_INFLUXDB_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        }
    }

    #[async_trait]
    impl Resource for InfluxDbResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned(
                    RESOURCE_NAME.into(),
                ));
            }

            let bucket_name = sanitize_sql_database_name(scenario_id.as_str());
            let want_key = image_fingerprint(&self.image_repository, &self.image_tag);

            let mut guard = suite_influxdb_mutex().lock().await;
            let (host, port) = match &mut *guard {
                SuiteInfluxDbState::Idle => {
                    let repository = self.image_repository.as_str();
                    let tag = self
                        .image_tag
                        .as_deref()
                        .map(str::trim)
                        .filter(|tag| !tag.is_empty())
                        .unwrap_or(DEFAULT_INFLUXDB_IMAGE_TAG);
                    let start_image = GenericImage::new(repository, tag)
                        .with_exposed_port(INFLUX_HTTP_PORT.tcp())
                        .with_wait_for(WaitFor::seconds(20))
                        .with_env_var("DOCKER_INFLUXDB_INIT_MODE", "setup")
                        .with_env_var("DOCKER_INFLUXDB_INIT_ORG", SUITE_ORGANIZATION)
                        .with_env_var("DOCKER_INFLUXDB_INIT_BUCKET", "default")
                        .with_env_var("DOCKER_INFLUXDB_INIT_USERNAME", "admin")
                        .with_env_var("DOCKER_INFLUXDB_INIT_PASSWORD", "shakedown123")
                        .with_env_var("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", SUITE_ADMIN_TOKEN)
                        .with_label(
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                        );
                    let node = start_image.start().await.map_err(Self::map_tc_err)?;

                    let mapped_port = node
                        .get_host_port_ipv4(INFLUX_HTTP_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();

                    *guard = SuiteInfluxDbState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port)
                }
                SuiteInfluxDbState::Running {
                    host,
                    port,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "InfluxDB suite already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port)
                }
            };
            drop(guard);

            let base_url = format!("http://{host}:{port}");
            let organization_id =
                fetch_organization_id(base_url.as_str(), SUITE_ADMIN_TOKEN, SUITE_ORGANIZATION)
                    .await
                    .map_err(|message| InfrastructureError::Provision {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(message)),
                    })?;

            let bucket_id = create_bucket(
                base_url.as_str(),
                SUITE_ADMIN_TOKEN,
                organization_id.as_str(),
                bucket_name.as_str(),
            )
            .await
            .map_err(|message| InfrastructureError::Provision {
                resource: RESOURCE_NAME.into(),
                source: Box::new(std::io::Error::other(message)),
            })?;

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.url = base_url.clone();
            self.organization = SUITE_ORGANIZATION.to_string();
            self.token = SUITE_ADMIN_TOKEN.to_string();
            self.bucket_name = bucket_name.clone();
            self.bucket_id = bucket_id.clone();

            Ok(vec![ResourceHandle {
                id: format!("influxdb_{scenario_id}"),
                connection_string: base_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "url": base_url,
                    "token": SUITE_ADMIN_TOKEN,
                    "organization": SUITE_ORGANIZATION,
                    "bucket": bucket_name,
                    "engine": "influxdb",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let _ = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            let _ = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;

            influx_health_passes(self.url.as_str())
                .await
                .map_err(|message| InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(message)),
                })
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.url.clear();
                self.organization.clear();
                self.token.clear();
                self.bucket_name.clear();
                self.bucket_id.clear();
                return Ok(());
            }

            let base_url = self.url.clone();
            let bucket_id = self.bucket_id.clone();
            let token = self.token.clone();

            if !bucket_id.is_empty() && !base_url.is_empty() {
                if let Err(message) =
                    delete_bucket_by_id(base_url.as_str(), token.as_str(), bucket_id.as_str()).await
                {
                    return Err(InfrastructureError::Teardown {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(message)),
                    });
                }
            }

            let mut guard = suite_influxdb_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
            match &mut *guard {
                SuiteInfluxDbState::Idle => {}
                SuiteInfluxDbState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteInfluxDbState::Idle);
                        if let SuiteInfluxDbState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_rm {
                container.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.url.clear();
            self.organization.clear();
            self.token.clear();
            self.bucket_name.clear();
            self.bucket_id.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "timescaledb")]
mod timescaledb_impl {
    use super::{sanitize_sql_database_name, Resource, ResourceHandle};
    use crate::error::{InfrastructureError, ScenarioId};
    use async_trait::async_trait;
    use std::sync::OnceLock;
    use testcontainers::core::IntoContainerPort;
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;
    use testcontainers::ImageExt;
    use tokio::sync::Mutex;
    use tokio_postgres::NoTls;

    const RESOURCE_NAME: &str = "timescaledb";
    const TIMESCALEDB_PORT: u16 = 5432;
    /// Default image repository (`timescale/timescaledb`).
    pub const DEFAULT_TIMESCALEDB_IMAGE_REPOSITORY: &str = "timescale/timescaledb";
    /// Default image tag (`latest-pg16` — pinned major Postgres line per SPECS).
    pub const DEFAULT_TIMESCALEDB_IMAGE_TAG: &str = "latest-pg16";

    #[allow(clippy::large_enum_variant)]
    enum SuiteTimescaledbState {
        Idle,
        Running {
            host: String,
            port: u16,
            username: String,
            password: String,
            container: testcontainers::ContainerAsync<GenericImage>,
            refs: usize,
            image_key: String,
        },
    }

    static SUITE_TIMESCALEDB: OnceLock<Mutex<SuiteTimescaledbState>> = OnceLock::new();

    fn suite_timescaledb_mutex() -> &'static Mutex<SuiteTimescaledbState> {
        SUITE_TIMESCALEDB.get_or_init(|| Mutex::new(SuiteTimescaledbState::Idle))
    }

    fn image_fingerprint(repository: &str, tag_override: &Option<String>) -> String {
        let tag = tag_override
            .as_deref()
            .map(str::trim)
            .filter(|tag| !tag.is_empty())
            .unwrap_or(DEFAULT_TIMESCALEDB_IMAGE_TAG);
        format!("{repository}:{tag}")
    }

    fn admin_connection_url(host: &str, port: u16, username: &str, password: &str) -> String {
        format!("postgresql://{username}:{password}@{host}:{port}/postgres")
    }

    fn scenario_connection_url(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: &str,
    ) -> String {
        format!("postgresql://{username}:{password}@{host}:{port}/{database}")
    }

    async fn connect_with_url(url: &str) -> Result<tokio_postgres::Client, InfrastructureError> {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await.map_err(|e| {
            InfrastructureError::Backend(Box::new(std::io::Error::other(e.to_string())))
        })?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    /// Docker-backed TimescaleDB (`timescale/timescaledb` via [`GenericImage`]).
    ///
    /// **Suite lifecycle:** one server per process; per-scenario `CREATE DATABASE` / `DROP DATABASE`;
    /// refcounted teardown. Uses the same [`super::PostgresBindings`] shape as PostgreSQL.
    ///
    /// Default tag **`latest-pg16`**. Override with [`Self::with_image_tag`] or
    /// `SHAKEDOWN_TIMESCALEDB_IMAGE_TAG`.
    #[derive(Debug)]
    pub struct TimescaledbResource {
        image_repository: String,
        image_tag: Option<String>,
        holds_suite_ref: bool,
        host: Option<String>,
        port: Option<u16>,
        username: String,
        password: String,
        database_name: String,
    }

    impl Default for TimescaledbResource {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TimescaledbResource {
        /// Create default Docker-backed TimescaleDB settings (suite server started on first provision).
        pub fn new() -> Self {
            Self {
                image_repository: DEFAULT_TIMESCALEDB_IMAGE_REPOSITORY.to_string(),
                image_tag: Some(DEFAULT_TIMESCALEDB_IMAGE_TAG.to_string()),
                holds_suite_ref: false,
                host: None,
                port: None,
                username: "postgres".to_string(),
                password: "postgres".to_string(),
                database_name: String::new(),
            }
        }

        /// Override the image tag (pin another `timescale/timescaledb` tag). Repository stays
        /// [`DEFAULT_TIMESCALEDB_IMAGE_REPOSITORY`] unless changed via [`Self::with_image_repository`].
        pub fn with_image_tag(mut self, tag: impl Into<String>) -> Self {
            self.image_tag = Some(tag.into());
            self
        }

        /// Override the image repository (advanced; default is `timescale/timescaledb`).
        pub fn with_image_repository(mut self, repository: impl Into<String>) -> Self {
            self.image_repository = repository.into();
            self
        }

        /// Apply tag override from `SHAKEDOWN_TIMESCALEDB_IMAGE_TAG` when set (non-empty).
        pub fn with_image_tag_from_env(mut self) -> Self {
            if let Ok(tag) = std::env::var("SHAKEDOWN_TIMESCALEDB_IMAGE_TAG") {
                let tag = tag.trim();
                if !tag.is_empty() {
                    self.image_tag = Some(tag.to_string());
                }
            }
            self
        }

        fn map_tc_err(
            error: impl std::fmt::Display + Send + Sync + 'static,
        ) -> InfrastructureError {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        }
    }

    #[async_trait]
    impl Resource for TimescaledbResource {
        async fn provision(
            &mut self,
            scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            if self.holds_suite_ref {
                return Err(InfrastructureError::AlreadyProvisioned(
                    RESOURCE_NAME.into(),
                ));
            }

            let database_name = sanitize_sql_database_name(scenario_id.as_str());
            let want_key = image_fingerprint(self.image_repository.as_str(), &self.image_tag);

            let mut guard = suite_timescaledb_mutex().lock().await;
            let (host, port, username, password) = match &mut *guard {
                SuiteTimescaledbState::Idle => {
                    let repository = self.image_repository.as_str();
                    let tag = self
                        .image_tag
                        .as_deref()
                        .map(str::trim)
                        .filter(|t| !t.is_empty())
                        .unwrap_or(DEFAULT_TIMESCALEDB_IMAGE_TAG);
                    let start_image = GenericImage::new(repository, tag)
                        .with_exposed_port(TIMESCALEDB_PORT.tcp())
                        .with_wait_for(WaitFor::message_on_stderr(
                            "database system is ready to accept connections",
                        ))
                        .with_env_var("POSTGRES_USER", "postgres")
                        .with_env_var("POSTGRES_PASSWORD", "postgres")
                        .with_env_var("POSTGRES_DB", "postgres")
                        .with_label(
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_KEY,
                            crate::docker_labels::SHAKEDOWN_DOCKER_LABEL_VALUE,
                        );
                    let node = start_image.start().await.map_err(Self::map_tc_err)?;

                    let mapped_port = node
                        .get_host_port_ipv4(TIMESCALEDB_PORT)
                        .await
                        .map_err(Self::map_tc_err)?;
                    let host = "127.0.0.1".to_string();
                    let username = "postgres".to_string();
                    let password = "postgres".to_string();

                    *guard = SuiteTimescaledbState::Running {
                        host: host.clone(),
                        port: mapped_port,
                        username: username.clone(),
                        password: password.clone(),
                        container: node,
                        refs: 1,
                        image_key: want_key.clone(),
                    };
                    (host, mapped_port, username, password)
                }
                SuiteTimescaledbState::Running {
                    host,
                    port,
                    username,
                    password,
                    refs,
                    image_key,
                    ..
                } => {
                    if *image_key != want_key {
                        return Err(InfrastructureError::Backend(Box::new(
                            std::io::Error::other(format!(
                                "TimescaleDB suite server already running as {image_key}; \
                                 cannot start or join as {want_key}. Use one image:tag per process."
                            )),
                        )));
                    }
                    *refs += 1;
                    (host.clone(), *port, username.clone(), password.clone())
                }
            };
            drop(guard);

            let admin_url = admin_connection_url(&host, port, &username, &password);
            let client = connect_with_url(&admin_url).await?;
            let create_sql = format!("CREATE DATABASE {}", database_name);
            client.batch_execute(&create_sql).await.map_err(|e| {
                InfrastructureError::Provision {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                }
            })?;

            self.holds_suite_ref = true;
            self.host = Some(host.clone());
            self.port = Some(port);
            self.username = username.clone();
            self.password = password.clone();
            self.database_name = database_name.clone();

            let connection_url =
                scenario_connection_url(&host, port, &database_name, &username, &password);

            Ok(vec![ResourceHandle {
                id: format!("timescaledb_{scenario_id}"),
                connection_string: connection_url.clone(),
                metadata: Some(serde_json::json!({
                    "host": host,
                    "port": port,
                    "database": database_name,
                    "username": username,
                    "password": password,
                    "connection_url": connection_url,
                    "engine": "timescaledb",
                })),
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            let host = self
                .host
                .as_deref()
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            let port = self
                .port
                .ok_or_else(|| InfrastructureError::NotProvisioned(RESOURCE_NAME.into()))?;
            let url = scenario_connection_url(
                host,
                port,
                &self.database_name,
                &self.username,
                &self.password,
            );
            let client =
                connect_with_url(&url)
                    .await
                    .map_err(|e| InfrastructureError::Readiness {
                        resource: RESOURCE_NAME.into(),
                        source: Box::new(std::io::Error::other(e.to_string())),
                    })?;
            client.query_one("SELECT 1", &[]).await.map_err(|e| {
                InfrastructureError::Readiness {
                    resource: RESOURCE_NAME.into(),
                    source: Box::new(std::io::Error::other(e.to_string())),
                }
            })?;
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            if !self.holds_suite_ref {
                self.host = None;
                self.port = None;
                self.database_name.clear();
                return Ok(());
            }

            let database_name = self.database_name.clone();
            let host = self.host.clone().unwrap_or_default();
            let port = self.port.unwrap_or(TIMESCALEDB_PORT);
            let username = self.username.clone();
            let password = self.password.clone();

            if !database_name.is_empty() {
                let admin_url = admin_connection_url(&host, port, &username, &password);
                if let Ok(client) = connect_with_url(&admin_url).await {
                    let _ = client
                        .query(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
                            &[&database_name],
                        )
                        .await;
                    let drop_sql = format!("DROP DATABASE IF EXISTS {}", database_name);
                    let _ = client.batch_execute(&drop_sql).await;
                }
            }

            let mut guard = suite_timescaledb_mutex().lock().await;
            let mut container_to_rm: Option<testcontainers::ContainerAsync<GenericImage>> = None;
            match &mut *guard {
                SuiteTimescaledbState::Idle => {}
                SuiteTimescaledbState::Running { refs, .. } => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        let taken = std::mem::replace(&mut *guard, SuiteTimescaledbState::Idle);
                        if let SuiteTimescaledbState::Running { container, .. } = taken {
                            container_to_rm = Some(container);
                        }
                    }
                }
            }
            drop(guard);

            if let Some(container) = container_to_rm {
                container.rm().await.map_err(Self::map_tc_err)?;
            }

            self.holds_suite_ref = false;
            self.host = None;
            self.port = None;
            self.database_name.clear();
            Ok(())
        }
    }
}

#[cfg(feature = "fault-injection")]
pub use toxiproxy_impl::ToxiproxyResource;

#[cfg(feature = "kafka")]
pub use kafka_impl::KafkaResource;

#[cfg(feature = "rabbitmq")]
pub use rabbit_impl::RabbitResource;

#[cfg(feature = "postgres")]
pub use postgres_impl::PostgresResource;

#[cfg(feature = "mariadb")]
pub use mariadb_impl::MariaDBResource;

#[cfg(feature = "mysql")]
pub use mysql_impl::MysqlResource;

#[cfg(feature = "sqlite")]
pub use sqlite_impl::SqliteResource;

#[cfg(feature = "duckdb")]
pub use duckdb_impl::DuckDbResource;

#[cfg(feature = "redis")]
pub use redis_impl::RedisResource;

#[cfg(feature = "valkey")]
pub use valkey_impl::ValkeyResource;

#[cfg(feature = "mongodb")]
pub use mongo_impl::MongoResource;

#[cfg(feature = "nats")]
pub use nats_impl::NatsResource;

#[cfg(feature = "neo4j")]
pub use neo4j_impl::Neo4jResource;

#[cfg(feature = "elasticsearch")]
pub use elasticsearch_impl::ElasticsearchResource;

#[cfg(feature = "opensearch")]
pub use opensearch_impl::OpenSearchResource;

#[cfg(feature = "qdrant")]
pub use qdrant_impl::QdrantResource;

#[cfg(feature = "weaviate")]
pub use weaviate_impl::WeaviateResource;

#[cfg(feature = "influxdb")]
pub use influxdb_impl::InfluxDbResource;

#[cfg(feature = "timescaledb")]
pub use timescaledb_impl::TimescaledbResource;

/// Connection and naming details extracted after Kafka provision for a scenario.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from [`crate::lifecycle::Context::kafka_bindings`]
/// inside scenarios that produce/consume messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaBindings {
    /// Bootstrap servers URL.
    pub bootstrap_servers: String,
    /// Topic prefix for this scenario's namespace.
    pub topic_prefix: String,
    /// Optional client ID prefix for isolation.
    pub client_id_prefix: Option<String>,
}

impl KafkaBindings {
    /// Create bindings with the given prefix.
    pub fn new(bootstrap_servers: impl Into<String>, topic_prefix: impl Into<String>) -> Self {
        let bootstrap = bootstrap_servers.into();
        let prefix = topic_prefix.into();
        KafkaBindings {
            bootstrap_servers: bootstrap.clone(),
            topic_prefix: prefix.clone(),
            client_id_prefix: Some(format!("shakedown_{prefix}")),
        }
    }

    /// Create bindings without a client ID prefix.
    pub fn with_bootstrap_only(
        bootstrap_servers: impl Into<String>,
        topic_prefix: impl Into<String>,
    ) -> Self {
        KafkaBindings {
            bootstrap_servers: bootstrap_servers.into(),
            topic_prefix: topic_prefix.into(),
            client_id_prefix: None,
        }
    }

    /// Return the topic name under this scenario's prefix.
    pub fn topic_namespace(&self, topic_name: &str) -> String {
        format!("{}{}", self.topic_prefix, topic_name)
    }
}

/// AMQP connection details and scenario-scoped naming for RabbitMQ.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::rabbit_bindings`] after provision (`rabbitmq` feature).
#[cfg(feature = "rabbitmq")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RabbitBindings {
    /// Full AMQP URI (includes credentials; test images only — do not use in production).
    pub amqp_uri: String,
    /// Same connection string as [`Self::amqp_uri`]; multi-host comma form for [`BuiltinResource::RabbitCluster`].
    pub connection_url: String,
    /// Broker hostname or IP (typically `127.0.0.1`).
    pub host: String,
    /// Mapped host port for AMQP (`5672` in the default image).
    pub port: u16,
    /// Username (default `guest` for the official test image).
    pub username: String,
    /// Password (default `guest`).
    pub password: String,
    /// Virtual host (default `/`).
    pub vhost: String,
    /// Prefix for exchange and queue names for this scenario (isolation).
    pub resource_prefix: String,
    /// Optional client connection name prefix (mirrors [`KafkaBindings::client_id_prefix`]).
    pub client_id_prefix: Option<String>,
}

#[cfg(feature = "rabbitmq")]
impl RabbitBindings {
    /// Build bindings from provision metadata fields (`connection_url` matches `amqp_uri`).
    pub fn new(
        amqp_uri: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
        vhost: impl Into<String>,
        resource_prefix: impl Into<String>,
    ) -> Self {
        let amqp_uri = amqp_uri.into();
        Self::with_connection_url(
            amqp_uri.clone(),
            amqp_uri,
            host,
            port,
            username,
            password,
            vhost,
            resource_prefix,
        )
    }

    /// Build bindings when the canonical multi-host URI differs from the lapin-friendly `amqp_uri` (e.g. [`BuiltinResource::RabbitCluster`]).
    #[allow(clippy::too_many_arguments)]
    pub fn with_connection_url(
        amqp_uri: impl Into<String>,
        connection_url: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        username: impl Into<String>,
        password: impl Into<String>,
        vhost: impl Into<String>,
        resource_prefix: impl Into<String>,
    ) -> Self {
        let resource_prefix = resource_prefix.into();
        let prefix_trimmed = resource_prefix.trim_end_matches('_').to_string();
        RabbitBindings {
            amqp_uri: amqp_uri.into(),
            connection_url: connection_url.into(),
            host: host.into(),
            port,
            username: username.into(),
            password: password.into(),
            vhost: vhost.into(),
            resource_prefix,
            client_id_prefix: Some(format!("shakedown_{prefix_trimmed}")),
        }
    }

    /// Compose a queue or exchange name under this scenario's prefix (same composition as [`KafkaBindings::topic_namespace`]).
    pub fn namespaced_name(&self, logical_name: &str) -> String {
        format!("{}{}", self.resource_prefix, logical_name)
    }
}

/// Connection details for a provisioned PostgreSQL or TimescaleDB scenario database.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::postgres_bindings`] or [`crate::lifecycle::Context::timescaledb_bindings`]
/// after provision.
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresBindings {
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for PostgreSQL.
    pub port: u16,
    /// Scenario-isolated database name.
    pub database: String,
    /// Superuser or application user for the scenario database.
    pub username: String,
    /// Password for `username`.
    pub password: String,
    /// JDBC/libpq-style URL for this scenario database.
    pub connection_url: String,
    /// When using [`crate::PostgresReplicaSetResource`], the streaming replica’s mapped host port (`postgres-cluster`).
    pub replica_port: Option<u16>,
}

/// Connection details for a provisioned MariaDB scenario database.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::mariadb_bindings`] after provision.
#[cfg(feature = "mariadb")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MariaDBBindings {
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for MariaDB.
    pub port: u16,
    /// Scenario-isolated database name.
    pub database: String,
    /// User for the scenario database (suite default is often `root`).
    pub username: String,
    /// Password for `username` (may be empty for test images).
    pub password: String,
    /// `mysql://` URL for this scenario database.
    pub connection_url: String,
}

/// Connection details for a provisioned Oracle MySQL scenario database.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::mysql_bindings`] after provision.
#[cfg(feature = "mysql")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MysqlBindings {
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for MySQL.
    pub port: u16,
    /// Scenario-isolated database name.
    pub database: String,
    /// User for the scenario database (suite default is often `root`).
    pub username: String,
    /// Password for `username` (may be empty for test images).
    pub password: String,
    /// `mysql://` URL for this scenario database.
    pub connection_url: String,
}

/// Connection details for a provisioned in-memory SQLite database.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::sqlite_bindings`] after provision.
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteBindings {
    /// URI string for this scenario's database (named in-memory with shared cache; see feature SPECS).
    pub database_url: String,
}

/// Connection details for a provisioned in-process DuckDB database (`duckdb` feature).
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::duckdb_bindings`] after provision.
#[cfg(feature = "duckdb")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDbBindings {
    /// Path string for this scenario's database (named in-memory; see [`super::duckdb_named_memory_database_path`]).
    pub database_url: String,
}

/// Redis or Valkey connection details and scenario-scoped key prefix.
///
/// Populated from provision metadata for both `redis` and `valkey` engine handles; use
/// [`crate::lifecycle::Context::redis_bindings`] after provision.
#[cfg(any(feature = "redis", feature = "valkey"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisBindings {
    /// `redis://` URL without authentication (default suite images).
    pub connection_url: String,
    pub host: String,
    pub port: u16,
    /// Prefix prepended to logical keys for isolation (`shakedown:<scenario>:`).
    pub key_prefix: String,
}

/// Redis Sentinel discovery endpoints plus primary URL (`redis-sentinel` feature).
///
/// Populated when [`BuiltinResource::RedisSentinel`] provisions the suite. [`connection_url`] is the
/// current primary (same wire URL [`RedisHelpers`](crate::helpers::RedisHelpers) use via
/// [`crate::lifecycle::Context::redis_bindings`]).
#[cfg(feature = "redis-sentinel")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisSentinelBindings {
    /// Comma-separated `host:port` Sentinel endpoints (harness host + mapped ports).
    pub sentinel_hosts: String,
    /// Logical master name (`SENTINEL MONITOR` / `get-master-addr-by-name`).
    pub master_name: String,
    /// `redis://` URL to the current primary (writable).
    pub connection_url: String,
    /// Host-mapped `redis://` URLs for replica nodes (read-only), in replica order.
    pub replica_connection_urls: Vec<String>,
}

/// Connection details for a provisioned MongoDB scenario database (`mongodb` feature).
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::mongo_bindings`] after provision.
/// Connection details and scenario-scoped NATS subject prefix (`nats` feature).
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::nats_bindings`] after provision.
#[cfg(feature = "nats")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsBindings {
    /// `nats://` URL to the suite broker (no auth for default image).
    pub connection_url: String,
    pub host: String,
    pub port: u16,
    /// Prefix prepended to logical subjects (`shakedown.<scenario>.`).
    pub subject_prefix: String,
}

#[cfg(feature = "nats")]
impl NatsBindings {
    /// Full subject for a logical name under this scenario's prefix.
    pub fn subject_namespace(&self, logical_subject: &str) -> String {
        format!("{}{}", self.subject_prefix, logical_subject)
    }
}

/// Bolt/HTTP connection details and scenario-isolated Neo4j database name (`neo4j` feature).
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::neo4j_bindings`] after provision.
#[cfg(feature = "neo4j")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Neo4jBindings {
    /// Bolt URL with credentials (typical **`NEO4J_URI`** for services).
    pub bolt_url: String,
    /// HTTP URL (browser/REST; includes credentials for default anonymous HTTP off).
    pub http_url: String,
    pub host: String,
    /// Mapped host port for Bolt (`7687` in the container).
    pub port: u16,
    /// Mapped host port for HTTP (`7474` in the container).
    pub http_port: u16,
    pub username: String,
    pub password: String,
    /// Scenario-isolated database name (multi-database Enterprise).
    pub database: String,
}

/// HTTP connection details and scenario-scoped Elasticsearch index prefix (`elasticsearch` feature).
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::elasticsearch_bindings`] after provision.
#[cfg(feature = "elasticsearch")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElasticsearchBindings {
    /// Base URL for the HTTP API (e.g. `http://127.0.0.1:9200`).
    pub url: String,
    pub host: String,
    pub port: u16,
    /// Prefix for logical index names (`shakedown_<scenario>_`).
    pub index_prefix: String,
    /// Set when [`ElasticsearchResource::with_security`] was used (`elastic` user by default).
    pub username: Option<String>,
    pub password: Option<String>,
}

#[cfg(feature = "elasticsearch")]
impl ElasticsearchBindings {
    /// Full index name for a logical index name under this scenario's prefix.
    pub fn index_namespace(&self, logical_index: &str) -> String {
        format!("{}{}", self.index_prefix, logical_index)
    }
}

/// HTTP connection details and scenario-scoped OpenSearch index prefix (`opensearch` feature).
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::opensearch_bindings`] after provision.
#[cfg(feature = "opensearch")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenSearchBindings {
    /// Base URL for the HTTP API (e.g. `http://127.0.0.1:9200`).
    pub url: String,
    pub host: String,
    pub port: u16,
    /// Prefix for logical index names (`shakedown_<scenario>_`).
    pub index_prefix: String,
    /// Set when [`OpenSearchResource::with_security`] was used (`admin` user by default).
    pub username: Option<String>,
    pub password: Option<String>,
}

#[cfg(feature = "opensearch")]
impl OpenSearchBindings {
    /// Full index name for a logical index name under this scenario's prefix.
    pub fn index_namespace(&self, logical_index: &str) -> String {
        format!("{}{}", self.index_prefix, logical_index)
    }
}

/// gRPC and REST endpoints plus scenario-scoped Qdrant collection name prefix (`qdrant` feature).
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::qdrant_bindings`] after provision.
#[cfg(feature = "qdrant")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QdrantBindings {
    /// gRPC URL for [`qdrant_client::Qdrant`] (e.g. `http://127.0.0.1:6334`).
    pub grpc_url: String,
    /// REST URL for HTTP probes and optional direct API use (e.g. `http://127.0.0.1:6333`).
    pub rest_url: String,
    pub host: String,
    pub grpc_port: u16,
    pub rest_port: u16,
    /// Prefix for logical collection names (`shakedown_<scenario>_`).
    pub collection_prefix: String,
}

#[cfg(feature = "qdrant")]
impl QdrantBindings {
    /// Full collection name for a logical name under this scenario's prefix.
    pub fn collection_namespace(&self, logical_collection: &str) -> String {
        format!("{}{}", self.collection_prefix, logical_collection)
    }
}

/// REST endpoint and scenario-scoped Weaviate class name prefix (`weaviate` feature).
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::weaviate_bindings`] after provision.
#[cfg(feature = "weaviate")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeaviateBindings {
    /// Base URL for the HTTP API (e.g. `http://127.0.0.1:8080`).
    pub rest_url: String,
    pub host: String,
    pub port: u16,
    /// Prefix for logical class names (`shakedown_<scenario>_`, same sanitizer as index/collection prefixes).
    pub class_prefix: String,
}

#[cfg(feature = "weaviate")]
impl WeaviateBindings {
    /// Full class name for a logical name under this scenario's prefix (Weaviate requires an initial capital letter).
    pub fn class_namespace(&self, logical_class: &str) -> String {
        let raw = format!("{}{}", self.class_prefix, logical_class);
        let mut chars = raw.chars();
        match chars.next() {
            None => String::from("ShakedownEmpty"),
            Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
        }
    }
}

/// InfluxDB 2.x HTTP API base URL and scenario-isolated bucket (`influxdb` feature).
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::influxdb_bindings`] after provision.
#[cfg(feature = "influxdb")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InfluxDbBindings {
    /// Base URL for the HTTP API (e.g. `http://127.0.0.1:8086`).
    pub url: String,
    pub host: String,
    pub port: u16,
    /// Admin token (fixed in the default container init — matches `DOCKER_INFLUXDB_INIT_ADMIN_TOKEN`).
    pub token: String,
    pub organization: String,
    /// Per-scenario bucket name.
    pub bucket: String,
}

#[cfg(feature = "mongodb")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MongoBindings {
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for `mongod` (`27017` in the default image).
    pub port: u16,
    /// Scenario-isolated database name.
    pub database: String,
    /// Root user for the suite container (default `root`).
    pub username: String,
    /// Password for `username` (default `root` in the suite image).
    pub password: String,
    /// Full MongoDB connection URI for this scenario database (`authSource=admin`).
    pub connection_url: String,
    /// Published host ports for each replica-set member index (`0..N`), suite order (empty for standalone).
    pub replica_set_host_ports: Vec<u16>,
}

/// A marker trait for resources that can be cloned.
pub trait CloneResource: Resource + Clone {}

impl<T: Resource + Clone> CloneResource for T {}

/// Pairs a [`Resource`] implementation with declarative [`ResourceConfig`] (for tooling / future CLI).
#[derive(Debug)]
pub struct ResourceBinding<T: Resource> {
    /// Live resource implementation under test.
    pub resource: T,
    /// Declared type label and connection string (documentation or driver hints).
    pub config: ResourceConfig,
}

/// Static description of a resource binding (type name and connection string).
#[derive(Debug, Clone)]
pub struct ResourceConfig {
    /// Logical type key (e.g. `"kafka"`, `"postgres"`).
    pub resource_type: String,
    /// Human-readable or driver connection string.
    pub connection_string: String,
}

impl<T: Resource> ResourceBinding<T> {
    /// Wrap `resource` with `config`.
    pub fn new(resource: T, config: ResourceConfig) -> Self {
        Self { resource, config }
    }

    /// Borrow the inner resource.
    pub fn resource(&self) -> &T {
        &self.resource
    }

    /// Borrow the configuration.
    pub fn config(&self) -> &ResourceConfig {
        &self.config
    }
}

/// Either a closed [`BuiltinResource`] or a boxed user [`Resource`], as stored on [`crate::lifecycle::Context`].
#[allow(clippy::large_enum_variant)]
pub enum RegisteredResource {
    /// Shipped-with-the-crate implementation ([`FakeResource`], Kafka, TCP SQL engines, SQLite when features are on).
    Builtin(BuiltinResource),
    /// Third-party or test-local [`Resource`] behind dynamic dispatch.
    Extension(Box<dyn Resource>),
}

impl RegisteredResource {
    /// Forward to the builtin or extension [`Resource::provision`].
    pub async fn provision(
        &mut self,
        scenario_id: &ScenarioId,
    ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
        match self {
            RegisteredResource::Builtin(b) => b.provision(scenario_id).await,
            RegisteredResource::Extension(e) => e.provision(scenario_id).await,
        }
    }

    /// Forward to [`Resource::check_ready`].
    pub async fn check_ready(&self) -> Result<(), InfrastructureError> {
        match self {
            RegisteredResource::Builtin(b) => b.check_ready().await,
            RegisteredResource::Extension(e) => e.check_ready().await,
        }
    }

    /// Forward to [`Resource::apply_default_seed`].
    pub async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
        match self {
            RegisteredResource::Builtin(b) => b.apply_default_seed().await,
            RegisteredResource::Extension(e) => e.apply_default_seed().await,
        }
    }

    /// Forward to [`Resource::teardown`].
    pub async fn teardown(&mut self) -> Result<(), InfrastructureError> {
        match self {
            RegisteredResource::Builtin(b) => b.teardown().await,
            RegisteredResource::Extension(e) => e.teardown().await,
        }
    }

    /// Dynamic downcast hook for tests and future reflection (builtin vs extension).
    pub fn as_any(&self) -> &dyn std::any::Any {
        match self {
            RegisteredResource::Builtin(b) => b,
            RegisteredResource::Extension(e) => e,
        }
    }

    /// Returns `true` if this slot holds a [`BuiltinResource`].
    pub fn is_builtin(&self) -> bool {
        matches!(self, RegisteredResource::Builtin(_))
    }

    /// Returns `true` if this slot holds a boxed extension [`Resource`].
    pub fn is_extension(&self) -> bool {
        matches!(self, RegisteredResource::Extension(_))
    }
}

impl fmt::Debug for RegisteredResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegisteredResource::Builtin(r) => f
                .debug_tuple("RegisteredResource::Builtin")
                .field(r)
                .finish(),
            RegisteredResource::Extension(_) => f
                .debug_tuple("RegisteredResource::Extension")
                .field(&"<dyn Resource>")
                .finish(),
        }
    }
}

impl PartialEq for RegisteredResource {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (
                RegisteredResource::Builtin(_),
                RegisteredResource::Builtin(_)
            ) | (
                RegisteredResource::Extension(_),
                RegisteredResource::Extension(_)
            )
        )
    }
}

/// Standalone list of [`RegisteredResource`] entries (alternative to using [`crate::lifecycle::Context`] alone).
#[derive(Debug, Default)]
pub struct ResourceRegistry {
    resources: Vec<RegisteredResource>,
}

impl ResourceRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a [`BuiltinResource`].
    pub fn register_builtin(&mut self, resource: BuiltinResource) {
        self.resources.push(RegisteredResource::Builtin(resource));
    }

    /// Append a boxed extension resource.
    pub fn register_extension<R: Resource + 'static>(&mut self, resource: R) {
        self.resources
            .push(RegisteredResource::Extension(Box::new(resource)));
    }

    /// Slice of all registered resources in order.
    pub fn resources(&self) -> &[RegisteredResource] {
        &self.resources
    }

    /// Remove every registered resource.
    pub fn clear(&mut self) {
        self.resources.clear();
    }

    /// `true` when there are no resources.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_resource_provision() {
        // Given a default fake resource and a scenario id
        let mut fake = FakeResource::default();
        // When we provision it
        let handles = fake
            .provision(&ScenarioId::new("test"))
            .await
            .expect("provision should succeed");
        // Then we get a single handle with the stable fake id
        assert_eq!(handles.len(), 1);
        assert_eq!(handles[0].id, "fake_resource");
    }

    #[tokio::test]
    async fn fake_resource_check_ready() {
        // Given a fake resource after provision semantics are not required here
        let fake = FakeResource::default();
        // When we check readiness
        let ready = fake.check_ready().await;
        // Then it always reports ok
        assert!(ready.is_ok());
    }

    #[tokio::test]
    async fn fake_resource_apply_default_seed() {
        // Given a mutable fake resource
        let mut fake = FakeResource::default();
        // When we apply the default seed
        let seeded = fake.apply_default_seed().await;
        // Then seeding succeeds
        assert!(seeded.is_ok());
    }

    #[tokio::test]
    async fn fake_resource_teardown() {
        // Given a fake resource that was "used" in a scenario
        let mut fake = FakeResource::default();
        // When we tear it down
        let torn_down = fake.teardown().await;
        // Then teardown succeeds
        assert!(torn_down.is_ok());
    }

    #[test]
    fn registered_resource_debug() {
        // Given a builtin registered resource wrapping a fake
        let builtin = RegisteredResource::Builtin(BuiltinResource::Fake(FakeResource::default()));
        // When we format it with Debug
        let debug_text = format!("{builtin:?}");
        // Then we get a non-empty debug representation
        assert!(!debug_text.is_empty());
    }

    #[test]
    fn registered_resource_partial_eq() {
        // Given two identical builtin fakes, a different extension, and another extension equal to the first
        let r1 = RegisteredResource::Builtin(BuiltinResource::Fake(FakeResource::default()));
        let r2 = RegisteredResource::Builtin(BuiltinResource::Fake(FakeResource::default()));
        let r3 = RegisteredResource::Extension(Box::new(FakeResource::default()));
        let r4 = RegisteredResource::Extension(Box::new(FakeResource::default()));
        // When we compare them with PartialEq
        // Then builtins match each other, builtin differs from extension, and extensions match
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
        assert_eq!(r3, r4);
    }

    #[test]
    fn resource_registry() {
        // Given an empty registry
        let mut registry = ResourceRegistry::new();
        // When we add a builtin, then an extension, then clear
        registry.register_builtin(BuiltinResource::Fake(FakeResource::default()));
        assert!(!registry.is_empty());
        assert_eq!(registry.resources().len(), 1);
        registry.register_extension(FakeResource::default());
        assert_eq!(registry.resources().len(), 2);
        registry.clear();
        // Then the registry is empty again
        assert!(registry.is_empty());
    }

    #[test]
    fn sanitize_resource_namespace_general() {
        assert_eq!(
            sanitize_resource_namespace("My Cool Scenario!", 63),
            "shakedown_my_cool_scenario_"
        );
        assert_eq!(sanitize_resource_namespace("", 63), "shakedown_scenario");
        let long = "x".repeat(200);
        let ns = sanitize_resource_namespace(&long, 40);
        assert!(ns.len() <= 40);
        assert!(ns.starts_with("shakedown_"));
    }

    #[test]
    fn sanitize_redis_key_prefix_colon_segments() {
        assert_eq!(
            sanitize_redis_key_prefix("MyScenario_1"),
            "shakedown:myscenario_1:"
        );
        assert_eq!(sanitize_redis_key_prefix(""), "shakedown:scenario:");
    }

    #[test]
    fn sanitize_sql_database_name_postgres_rules() {
        assert_eq!(
            sanitize_sql_database_name("MyScenario_1"),
            "shakedown_myscenario_1"
        );
        let long = "a".repeat(100);
        let sanitized = sanitize_sql_database_name(&long);
        assert!(sanitized.len() <= 63);
        assert!(sanitized.starts_with("shakedown_"));
        assert_eq!(sanitize_sql_database_name(""), "shakedown_scenario");
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_named_memory_uri_is_unique_per_scenario() {
        let a = super::sqlite_named_memory_uri("test_a");
        let b = super::sqlite_named_memory_uri("test_b");
        assert_ne!(a, b);
        assert!(a.starts_with("file:shakedown_"));
        assert!(a.contains("mode=memory"));
        assert!(a.contains("cache=shared"));
    }

    #[cfg(feature = "duckdb")]
    #[test]
    fn duckdb_named_memory_database_path_is_unique_per_scenario() {
        let a = super::duckdb_named_memory_database_path("test_a");
        let b = super::duckdb_named_memory_database_path("test_b");
        assert_ne!(a, b);
        assert!(a.ends_with(".duckdb"));
    }

    #[test]
    fn resource_binding() {
        // Given a fake resource and a typed connection config
        let resource = FakeResource::default();
        let config = ResourceConfig {
            resource_type: "fake".to_string(),
            connection_string: "localhost:1234".to_string(),
        };
        // When we bind them
        let binding = ResourceBinding::new(resource, config);
        // Then the binding exposes the config we passed in
        assert_eq!(binding.config.resource_type, "fake");
        assert_eq!(binding.config.connection_string, "localhost:1234");
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_bindings_topic_namespace() {
        // Given Kafka bindings for a bootstrap URL and topic prefix
        let bindings = KafkaBindings::new("localhost:9092", "shakedown_test");
        // When we read fields and derive a topic namespace suffix
        // Then servers, prefix, client id, and composed namespace match expectations
        assert_eq!(bindings.bootstrap_servers, "localhost:9092");
        assert_eq!(bindings.topic_prefix, "shakedown_test");
        assert!(bindings.client_id_prefix.is_some());
        assert_eq!(bindings.topic_namespace("proof"), "shakedown_testproof");
    }

    #[cfg(feature = "rabbitmq")]
    #[test]
    fn rabbit_bindings_namespaced_name() {
        let bindings = RabbitBindings::new(
            "amqp://guest:guest@127.0.0.1:5672",
            "127.0.0.1",
            5672,
            "guest",
            "guest",
            "/",
            "shakedown_test_",
        );
        assert_eq!(bindings.amqp_uri, "amqp://guest:guest@127.0.0.1:5672");
        assert_eq!(bindings.connection_url, bindings.amqp_uri);
        assert_eq!(bindings.port, 5672);
        assert_eq!(bindings.vhost, "/");
        assert_eq!(bindings.namespaced_name("proof"), "shakedown_test_proof");
    }
}
