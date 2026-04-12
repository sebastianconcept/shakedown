//! Resource providers and registration for Shakedown.
//!
//! Integration tests need real infrastructure — databases, brokers, caches — provisioned
//! fresh and fully isolated for each scenario. The [`Resource`] trait is the contract every engine
//! implements so Shakedown's lifecycle can provision, health-check, seed, and tear down anything
//! behind one async interface.
//!
//! Register resources on [`crate::lifecycle::Context`] before calling
//! [`crate::lifecycle::run_scenario`]. For built-in engines (Kafka, Postgres, Redis, …) use
//! [`BuiltinResource`] variants. For custom infrastructure implement [`Resource`] directly and call
//! `ctx.register_resource(your_resource)`. Use [`sanitize_resource_namespace`] inside `provision`
//! to derive a scenario-scoped name that is safe as a database name, topic prefix, index name, or
//! key namespace.
//!
//! Shared contract, sanitizers, [`BuiltinResource`], and bindings live in this module;
//! each engine lives under `resources/<engine>/` with `resource.rs`, `helpers.rs`, and `tests.rs`.
//!
//! # Book reference
//!
//! Part I, *Chapters 2–8: Foundations* — each chapter introduces one engine family (relational,
//! Kafka, Redis, MongoDB, …), showing how to register resources, configure isolation, and seed
//! scenario-specific data.

use crate::error::{InfrastructureError, ResourceHandle, ScenarioId};
use async_trait::async_trait;
use std::fmt;

/// Uniform sub-context creation for all scenario-scoped Bindings.
///
/// Both prefix-based resources (Kafka, Redis, NATS, …) and entity-based resources (Postgres,
/// MariaDB, Neo4j, …) implement this trait. The call site is always the same:
///
/// ```rust,no_run
/// # use shakedown::{Isolatable, KafkaBindings, InfrastructureError};
/// # async fn example(kafka: &KafkaBindings) -> Result<(), InfrastructureError> {
/// let sub = kafka.isolate("analytics").await?;
/// # Ok(())
/// # }
/// ```
///
/// Prefix-based resources return new Bindings with a derived sub-prefix — **no I/O**.
/// Entity-based resources provision a real database or bucket (I/O).
pub trait Isolatable: Sized {
    /// Create a named sub-context of this Bindings.
    ///
    /// For prefix-based resources this is a pure in-memory O(1) operation — the new Bindings
    /// derive a deeper prefix from `name`. For entity-based resources (Postgres, MariaDB, …)
    /// this provisions a real secondary database and returns Bindings pointing to it.
    fn isolate(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<Self, InfrastructureError>> + Send;
}

/// Uniform namespacing contract for all scenario-scoped Bindings that use a prefix.
///
/// Implement this trait to write generic test helpers that construct isolation-safe entity names
/// without coupling to a specific resource type.
///
/// Every built-in prefix-based Bindings type implements this trait. The `namespace` method
/// prepends the scenario-scoped isolation prefix to the given logical name using the separator
/// convention of the underlying resource (e.g. `_` for Kafka, `:` for Redis, `.` for NATS).
///
/// # Example
///
/// ```rust,no_run
/// use shakedown::Namespaced;
///
/// fn make_name<B: Namespaced>(bindings: &B, logical: &str) -> String {
///     bindings.namespace(logical)
/// }
/// ```
pub trait Namespaced {
    /// Return `name` prefixed with this scenario's isolation prefix.
    fn namespace(&self, name: &str) -> String;
}

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

/// Turn a scenario id into a safe Neo4j 5 **database** name (`shakedown-…`, max 63 bytes).
///
/// Neo4j allows only ASCII letters, digits, dots, and dashes in database names—no underscores.
/// This prefixes `shakedown-` and maps non-alphanumerics to `-`, collapsing runs of `-`.
#[cfg(feature = "neo4j")]
pub fn sanitize_neo4j_database_name(scenario_id: &str) -> String {
    const PREFIX: &str = "shakedown";
    const MAX_LEN: usize = 63;
    let mut rest = String::new();
    for ch in scenario_id.chars().take(200) {
        if ch.is_ascii_alphanumeric() {
            rest.push(ch.to_ascii_lowercase());
        } else {
            rest.push('-');
        }
    }
    if rest.is_empty() {
        rest.push_str("scenario");
    }
    let mut collapsed = String::new();
    for ch in rest.chars() {
        if ch == '-' {
            if collapsed.ends_with('-') {
                continue;
            }
            collapsed.push('-');
        } else {
            collapsed.push(ch);
        }
    }
    let mut rest = collapsed.trim_matches('-').to_string();
    if rest.is_empty() {
        rest.push_str("scenario");
    }
    let mut out = format!("{PREFIX}-{rest}");
    if out.len() > MAX_LEN {
        out.truncate(MAX_LEN);
        while out.ends_with('-') && out.len() > PREFIX.len() + 1 {
            out.pop();
        }
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
#[allow(clippy::large_enum_variant)] // Kafka holds a container handle; boxing would diverge from the public BuiltinResource shape.
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
    /// Docker-backed PostgreSQL primary + streaming replica (per-scenario containers).
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
    /// Docker-backed multi-broker KRaft Kafka cluster (shared bridge network).
    KafkaCluster(crate::KafkaClusterResource),

    #[cfg(feature = "rabbitmq-cluster")]
    /// Docker-backed RabbitMQ Erlang cluster (shared bridge network; quorum default queue type).
    RabbitCluster(crate::RabbitClusterResource),

    #[cfg(feature = "redis-sentinel")]
    /// Docker-backed Redis primary/replicas + Sentinel quorum (shared bridge network).
    RedisSentinel(crate::RedisSentinelResource),

    #[cfg(feature = "nats")]
    /// Docker-backed NATS (official image via testcontainers-modules; suite-shared server; JetStream enabled for publication verification).
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

// --- Engine resource implementations (`resources/<engine>/`; shared Redis/Valkey in `resp_family.rs`) ---

mod migration_runner;
mod sql_bindings_migrations;

pub use migration_runner::MigrationRunner;

/// Shared reqwest client for Elasticsearch/OpenSearch HTTP APIs (`elasticsearch` / `opensearch` features).
#[cfg(any(feature = "elasticsearch", feature = "opensearch"))]
pub(crate) mod search_operations;

#[cfg(any(feature = "redis", feature = "valkey"))]
pub(crate) mod resp_family;

#[cfg(any(feature = "mariadb", feature = "mysql"))]
pub(crate) mod mysql_family;

#[cfg(feature = "fault-injection")]
pub mod toxiproxy;

#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;
#[cfg(feature = "influxdb")]
pub mod influxdb;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "mariadb")]
pub mod mariadb;
#[cfg(feature = "mongodb")]
pub mod mongo;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "nats")]
pub mod nats;
#[cfg(feature = "neo4j")]
pub mod neo4j;
#[cfg(feature = "opensearch")]
pub mod opensearch;
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
pub mod postgres;
#[cfg(feature = "qdrant")]
pub mod qdrant;
#[cfg(feature = "rabbitmq")]
pub mod rabbit;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "timescaledb")]
pub mod timescaledb;
#[cfg(feature = "valkey")]
pub mod valkey;
#[cfg(feature = "weaviate")]
pub mod weaviate;

#[cfg(feature = "duckdb")]
pub use duckdb::DuckDbResource;
#[cfg(feature = "elasticsearch")]
pub use elasticsearch::ElasticsearchResource;
#[cfg(feature = "influxdb")]
pub use influxdb::InfluxDbResource;
#[cfg(feature = "kafka")]
pub use kafka::KafkaResource;
#[cfg(feature = "mariadb")]
pub use mariadb::MariaDBResource;
#[cfg(feature = "mongodb")]
pub use mongo::MongoResource;
#[cfg(feature = "mysql")]
pub use mysql::MysqlResource;
#[cfg(feature = "nats")]
pub use nats::NatsResource;
#[cfg(feature = "neo4j")]
pub use neo4j::Neo4jResource;
#[cfg(feature = "opensearch")]
pub use opensearch::OpenSearchResource;
#[cfg(feature = "postgres")]
pub use postgres::PostgresResource;
#[cfg(feature = "qdrant")]
pub use qdrant::QdrantResource;
#[cfg(feature = "rabbitmq")]
pub use rabbit::RabbitResource;
#[cfg(feature = "redis")]
pub use redis::RedisResource;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteResource;
#[cfg(feature = "timescaledb")]
pub use timescaledb::TimescaledbResource;
#[cfg(feature = "fault-injection")]
pub(crate) use toxiproxy::toxiproxy_suite_container_id;
#[cfg(feature = "fault-injection")]
pub use toxiproxy::ToxiproxyResource;
#[cfg(feature = "valkey")]
pub use valkey::ValkeyResource;
#[cfg(feature = "weaviate")]
pub use weaviate::WeaviateResource;

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
    ///
    /// See also [`consumer_group_namespace`](Self::consumer_group_namespace) — consumer groups
    /// must stay on the same scenario boundary as topics when parallel tests share a broker.
    pub fn topic_namespace(&self, topic_name: &str) -> String {
        format!("{}{}", self.topic_prefix, topic_name)
    }

    /// Return `group_id` prefixed with this scenario's topic prefix.
    ///
    /// Use this when configuring application code that has a hardcoded consumer group ID.
    /// Parallel scenarios share the same Kafka broker and topic namespace — without this
    /// prefix, two parallel scenarios with the same logical group ID will join the *same*
    /// consumer group and steal each other's messages, causing non-deterministic test
    /// failures.
    ///
    /// Uses the same prefix as [`topic_namespace`](Self::topic_namespace) so topics and
    /// their consumer groups always share the scenario boundary.
    pub fn consumer_group_namespace(&self, group_id: &str) -> String {
        format!("{}{}", self.topic_prefix, group_id)
    }
}

#[cfg(feature = "kafka")]
impl KafkaBindings {
    /// Returns an [`rdkafka::ClientConfig`] pre-filled with `bootstrap.servers` and a
    /// `message.timeout.ms` of `5000` suitable for tests.
    ///
    /// Use this as a starting point when you need a custom producer (for example for the
    /// transactional outbox pattern):
    ///
    /// ```rust,no_run
    /// use shakedown::rdkafka::producer::FutureProducer;
    /// # fn example(bindings: &shakedown::KafkaBindings) {
    /// let producer: FutureProducer = bindings
    ///     .client_config()
    ///     .set("acks", "all")
    ///     .create()
    ///     .expect("producer");
    /// # }
    /// ```
    pub fn client_config(&self) -> rdkafka::config::ClientConfig {
        let mut configuration = rdkafka::config::ClientConfig::new();
        configuration.set("bootstrap.servers", self.bootstrap_servers.as_str());
        configuration.set("message.timeout.ms", "5000");
        configuration
    }

    /// Convenience: creates a [`rdkafka::producer::FutureProducer`] from [`Self::client_config`].
    ///
    /// Equivalent to `bindings.client_config().create()`. For custom settings, use
    /// [`Self::client_config`] directly and call `.set(...)` before `.create()`.
    pub fn future_producer(
        &self,
    ) -> Result<rdkafka::producer::FutureProducer, InfrastructureError> {
        Self::future_producer_from_client_config(self.client_config())
    }

    /// Builds a [`rdkafka::producer::FutureProducer`] from a fully configured [`rdkafka::ClientConfig`].
    ///
    /// Used by [`Self::future_producer`] and unit tests that need to exercise error paths without
    /// mutating [`KafkaBindings`] invariants.
    pub(crate) fn future_producer_from_client_config(
        configuration: rdkafka::config::ClientConfig,
    ) -> Result<rdkafka::producer::FutureProducer, InfrastructureError> {
        configuration.create().map_err(|error| {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        })
    }
}

impl Namespaced for KafkaBindings {
    /// Return `name` prefixed with this scenario's Kafka topic prefix.
    fn namespace(&self, name: &str) -> String {
        format!("{}{}", self.topic_prefix, name)
    }
}

impl Isolatable for KafkaBindings {
    /// Return new Bindings with a deeper topic prefix (`{current}{name}_`) — no I/O.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        Ok(KafkaBindings {
            bootstrap_servers: self.bootstrap_servers.clone(),
            topic_prefix: format!("{}{}_", self.topic_prefix, name),
            client_id_prefix: self.client_id_prefix.clone(),
        })
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
    ///
    /// See also [`queue_namespace`](Self::queue_namespace) and [`exchange_namespace`](Self::exchange_namespace).
    pub fn namespaced_name(&self, logical_name: &str) -> String {
        format!("{}{}", self.resource_prefix, logical_name)
    }

    /// Return `queue` prefixed with this scenario's resource prefix.
    ///
    /// Use this when declaring queues so parallel scenarios don't share the same queue name and
    /// accidentally consume each other's messages.
    ///
    /// See also [`exchange_namespace`](Self::exchange_namespace) and [`namespaced_name`](Self::namespaced_name).
    pub fn queue_namespace(&self, queue: &str) -> String {
        format!("{}{}", self.resource_prefix, queue)
    }

    /// Return `exchange` prefixed with this scenario's resource prefix.
    ///
    /// Use this when declaring exchanges so parallel scenarios don't route messages to shared
    /// exchanges unintentionally.
    ///
    /// See also [`queue_namespace`](Self::queue_namespace) and [`namespaced_name`](Self::namespaced_name).
    pub fn exchange_namespace(&self, exchange: &str) -> String {
        format!("{}{}", self.resource_prefix, exchange)
    }
}

#[cfg(feature = "rabbitmq")]
impl Namespaced for RabbitBindings {
    /// Return `name` prefixed with this scenario's RabbitMQ resource prefix.
    fn namespace(&self, name: &str) -> String {
        format!("{}{}", self.resource_prefix, name)
    }
}

#[cfg(feature = "rabbitmq")]
impl Isolatable for RabbitBindings {
    /// Return new Bindings with a deeper resource prefix (`{current}{name}.`) — no I/O.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        Ok(RabbitBindings {
            amqp_uri: self.amqp_uri.clone(),
            connection_url: self.connection_url.clone(),
            host: self.host.clone(),
            port: self.port,
            username: self.username.clone(),
            password: self.password.clone(),
            vhost: self.vhost.clone(),
            resource_prefix: format!("{}{}_", self.resource_prefix, name),
            client_id_prefix: self.client_id_prefix.clone(),
        })
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

/// Implementation of [`Isolatable`] for [`PostgresBindings`] (provisions a new database on the
/// suite container — I/O).
#[cfg(feature = "postgres")]
impl Isolatable for PostgresBindings {
    /// Provision a new Postgres database named `name` on the suite container.
    ///
    /// The returned Bindings point to the new database; connection credentials are the same as
    /// the parent. The database is cleaned up when the suite container tears down.
    ///
    /// Returns an error for invalid names (longer than 63 chars or containing invalid characters).
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        let safe_name = sanitize_sql_database_name(name);
        if safe_name.len() > 63 {
            return Err(InfrastructureError::Provision {
                resource: format!("postgres:isolate:{name}"),
                source: Box::new(std::io::Error::other(
                    "database name exceeds 63 characters after sanitization",
                )),
            });
        }
        let admin_url =
            postgres::admin_connection_url(&self.host, self.port, &self.username, &self.password);
        let client = postgres::connect_with_url(&admin_url).await?;
        let create_sql = format!("CREATE DATABASE {safe_name}");
        client.batch_execute(&create_sql).await.map_err(|error| {
            InfrastructureError::Provision {
                resource: format!("postgres:isolate:{name}"),
                source: Box::new(std::io::Error::other(error.to_string())),
            }
        })?;
        let connection_url = postgres::scenario_connection_url(
            &self.host,
            self.port,
            &safe_name,
            &self.username,
            &self.password,
        );
        Ok(PostgresBindings {
            host: self.host.clone(),
            port: self.port,
            database: safe_name,
            username: self.username.clone(),
            password: self.password.clone(),
            connection_url,
            replica_port: None,
        })
    }
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

/// Implementation of [`Isolatable`] for [`MariaDBBindings`] (provisions a new database — I/O).
#[cfg(feature = "mariadb")]
impl Isolatable for MariaDBBindings {
    /// Provision a new MariaDB database named `name` on the suite container.
    ///
    /// The returned Bindings point to the new database. The database is cleaned up when the
    /// suite container tears down.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        use mysql_async::prelude::Queryable;
        let safe_name = sanitize_sql_database_name(name);
        if safe_name.len() > 63 {
            return Err(InfrastructureError::Provision {
                resource: format!("mariadb:isolate:{name}"),
                source: Box::new(std::io::Error::other(
                    "database name exceeds 63 characters after sanitization",
                )),
            });
        }
        let pool = mariadb::admin_pool(&self.host, self.port, &self.username, &self.password);
        let mut connection =
            pool.get_conn()
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: format!("mariadb:isolate:{name}"),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
        connection
            .query_drop(format!("CREATE DATABASE `{safe_name}`").as_str())
            .await
            .map_err(|error| InfrastructureError::Provision {
                resource: format!("mariadb:isolate:{name}"),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        let connection_url = mariadb::scenario_connection_url(
            &self.host,
            self.port,
            &safe_name,
            &self.username,
            &self.password,
        );
        Ok(MariaDBBindings {
            host: self.host.clone(),
            port: self.port,
            database: safe_name,
            username: self.username.clone(),
            password: self.password.clone(),
            connection_url,
        })
    }
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

/// Implementation of [`Isolatable`] for [`MysqlBindings`] (provisions a new database — I/O).
#[cfg(feature = "mysql")]
impl Isolatable for MysqlBindings {
    /// Provision a new MySQL database named `name` on the suite container.
    ///
    /// The returned Bindings point to the new database. The database is cleaned up when the
    /// suite container tears down.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        use mysql_async::prelude::Queryable;
        let safe_name = sanitize_sql_database_name(name);
        if safe_name.len() > 63 {
            return Err(InfrastructureError::Provision {
                resource: format!("mysql:isolate:{name}"),
                source: Box::new(std::io::Error::other(
                    "database name exceeds 63 characters after sanitization",
                )),
            });
        }
        let pool = mysql::admin_pool(&self.host, self.port, &self.username, &self.password);
        let mut connection =
            pool.get_conn()
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: format!("mysql:isolate:{name}"),
                    source: Box::new(std::io::Error::other(error.to_string())),
                })?;
        connection
            .query_drop(format!("CREATE DATABASE `{safe_name}`").as_str())
            .await
            .map_err(|error| InfrastructureError::Provision {
                resource: format!("mysql:isolate:{name}"),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        let connection_url = mysql::scenario_connection_url(
            &self.host,
            self.port,
            &safe_name,
            &self.username,
            &self.password,
        );
        Ok(MysqlBindings {
            host: self.host.clone(),
            port: self.port,
            database: safe_name,
            username: self.username.clone(),
            password: self.password.clone(),
            connection_url,
        })
    }
}

/// Connection details for a provisioned in-memory SQLite database.
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::sqlite_bindings`] after provision.
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteBindings {
    /// URI string for this scenario's database (named in-memory with shared cache; matches the `sqlite` feature wiring).
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

#[cfg(feature = "sqlite")]
impl Isolatable for SqliteBindings {
    /// Return new in-memory SQLite Bindings scoped to `name` — no I/O.
    ///
    /// Each unique `name` yields an independent named in-memory database.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        Ok(SqliteBindings {
            database_url: sqlite_named_memory_uri(name),
        })
    }
}

#[cfg(feature = "duckdb")]
impl Isolatable for DuckDbBindings {
    /// Return new DuckDB Bindings scoped to `name` — no I/O.
    ///
    /// Each unique `name` yields an independent named database path.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        Ok(DuckDbBindings {
            database_url: duckdb_named_memory_database_path(name),
        })
    }
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
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for Redis.
    pub port: u16,
    /// Prefix prepended to logical keys for isolation (`shakedown:<scenario>:`).
    pub key_prefix: String,
}

#[cfg(any(feature = "redis", feature = "valkey"))]
impl RedisBindings {
    /// Return `key` prefixed with this scenario's key prefix.
    ///
    /// Use this when storing or looking up keys so parallel scenarios don't share the same
    /// key namespace and overwrite each other's data.
    pub fn key_namespace(&self, key: &str) -> String {
        format!("{}{}", self.key_prefix, key)
    }
}

#[cfg(any(feature = "redis", feature = "valkey"))]
impl Namespaced for RedisBindings {
    /// Return `name` prefixed with this scenario's Redis key prefix.
    fn namespace(&self, name: &str) -> String {
        format!("{}{}", self.key_prefix, name)
    }
}

#[cfg(any(feature = "redis", feature = "valkey"))]
impl Isolatable for RedisBindings {
    /// Return new Bindings with a deeper key prefix (`{current}{name}:`) — no I/O.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        Ok(RedisBindings {
            connection_url: self.connection_url.clone(),
            host: self.host.clone(),
            port: self.port,
            key_prefix: format!("{}{}:", self.key_prefix, name),
        })
    }
}

/// Redis Sentinel discovery endpoints plus primary URL (`redis-sentinel` feature).
///
/// Populated when [`BuiltinResource::RedisSentinel`] provisions the suite. [`connection_url`] is the
/// current primary (same wire URL [`RedisHelpers`](crate::resources::redis::RedisHelpers) use via
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
    /// Prefix prepended to logical keys for isolation (`shakedown:<scenario>:`).
    pub key_prefix: String,
}

#[cfg(feature = "redis-sentinel")]
impl RedisSentinelBindings {
    /// Return `key` prefixed with this scenario's key prefix.
    ///
    /// Use this when storing or looking up keys through the Sentinel-connected primary so
    /// parallel scenarios don't share the same key namespace and overwrite each other's data.
    pub fn key_namespace(&self, key: &str) -> String {
        format!("{}{}", self.key_prefix, key)
    }

    /// Build a `redis::sentinel::SentinelClient` for the logical master ([`Self::master_name`]),
    /// using [`Self::sentinel_hosts`] as Sentinel discovery endpoints.
    ///
    /// This is the high-level sentinel type that resolves the **current** primary after failovers
    /// (via `SentinelClient::async_get_client` / sync APIs), unlike a plain `redis::Client` opened
    /// only to [`Self::connection_url`], which does not track topology.
    ///
    /// Requires the **`redis-sentinel`** Cargo feature (redis crate `sentinel` + `tokio-comp`).
    pub fn sentinel_client(
        &self,
    ) -> Result<::redis::sentinel::SentinelClient, crate::error::ShakedownError> {
        use ::redis::sentinel::SentinelServerType;

        let sentinel_urls: Vec<String> = self
            .sentinel_hosts
            .split(',')
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
            .map(|host_port| format!("redis://{host_port}/"))
            .collect();

        if sentinel_urls.is_empty() {
            return Err(crate::error::ShakedownError::Infrastructure(
                InfrastructureError::Backend(Box::new(std::io::Error::other(
                    "RedisSentinelBindings.sentinel_client: sentinel_hosts has no endpoints",
                ))),
            ));
        }

        ::redis::sentinel::SentinelClient::build(
            sentinel_urls,
            self.master_name.clone(),
            None,
            SentinelServerType::Master,
        )
        .map_err(crate::helper_prelude::map_backend_error)
    }
}

#[cfg(feature = "redis-sentinel")]
impl Namespaced for RedisSentinelBindings {
    /// Return `name` prefixed with this scenario's Redis Sentinel key prefix.
    fn namespace(&self, name: &str) -> String {
        format!("{}{}", self.key_prefix, name)
    }
}

#[cfg(feature = "redis-sentinel")]
impl Isolatable for RedisSentinelBindings {
    /// Return new Bindings with a deeper key prefix (`{current}{name}:`) — no I/O.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        Ok(RedisSentinelBindings {
            sentinel_hosts: self.sentinel_hosts.clone(),
            master_name: self.master_name.clone(),
            connection_url: self.connection_url.clone(),
            replica_connection_urls: self.replica_connection_urls.clone(),
            key_prefix: format!("{}{}:", self.key_prefix, name),
        })
    }
}

/// Connection details and scenario-scoped NATS subject prefix (`nats` feature).
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::nats_bindings`] after provision.
#[cfg(feature = "nats")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsBindings {
    /// `nats://` URL to the suite broker (no auth for default image).
    pub connection_url: String,
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for NATS.
    pub port: u16,
    /// Prefix prepended to logical subjects (`shakedown.<scenario>.`).
    ///
    /// See also [`subject_namespace`](Self::subject_namespace) for the full subject string.
    pub subject_prefix: String,
}

#[cfg(feature = "nats")]
impl NatsBindings {
    /// Full subject for a logical name under this scenario's prefix.
    ///
    /// See also [`subject_prefix`](Self::subject_prefix) — JetStream stream and subject wiring
    /// should stay consistent with this namespace.
    pub fn subject_namespace(&self, logical_subject: &str) -> String {
        format!("{}{}", self.subject_prefix, logical_subject)
    }
}

#[cfg(feature = "nats")]
impl Namespaced for NatsBindings {
    /// Return `name` prefixed with this scenario's NATS subject prefix.
    fn namespace(&self, name: &str) -> String {
        format!("{}{}", self.subject_prefix, name)
    }
}

#[cfg(feature = "nats")]
impl Isolatable for NatsBindings {
    /// Return new Bindings with a deeper subject prefix (`{current}{name}.`) — no I/O.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        Ok(NatsBindings {
            connection_url: self.connection_url.clone(),
            host: self.host.clone(),
            port: self.port,
            subject_prefix: format!("{}{}.", self.subject_prefix, name),
        })
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
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for Bolt (`7687` in the container).
    pub port: u16,
    /// Mapped host port for HTTP (`7474` in the container).
    pub http_port: u16,
    /// Username for the Neo4j database.
    pub username: String,
    /// Password for `username`.
    pub password: String,
    /// Scenario-isolated database name (multi-database Enterprise).
    pub database: String,
}

/// Implementation of [`Isolatable`] for [`Neo4jBindings`] (provisions a new database via Cypher — I/O).
#[cfg(feature = "neo4j")]
impl Isolatable for Neo4jBindings {
    /// Provision a new Neo4j database named `name` using `CREATE DATABASE … WAIT`.
    ///
    /// Requires Neo4j Enterprise edition (which is the default image tag). The database is
    /// cleaned up when the suite container tears down.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        use neo4rs::query;
        let system_graph = neo4j::graph_system(&self.host, self.port).await?;
        let cypher = format!("CREATE DATABASE `{name}` WAIT");
        system_graph
            .run(query(&cypher))
            .await
            .map_err(|error| InfrastructureError::Provision {
                resource: format!("neo4j:isolate:{name}"),
                source: Box::new(std::io::Error::other(error.to_string())),
            })?;
        let bolt_url = format!(
            "bolt://{}:{}@{}:{}",
            self.username, self.password, self.host, self.port
        );
        let http_url = format!(
            "http://{}:{}@{}:{}",
            self.username, self.password, self.host, self.http_port
        );
        Ok(Neo4jBindings {
            bolt_url,
            http_url,
            host: self.host.clone(),
            port: self.port,
            http_port: self.http_port,
            username: self.username.clone(),
            password: self.password.clone(),
            database: name.to_string(),
        })
    }
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
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for Elasticsearch HTTP.
    pub port: u16,
    /// Prefix for logical index names (`shakedown_<scenario>_`).
    pub index_prefix: String,
    /// Set when [`ElasticsearchResource::with_security`] was used (`elastic` user by default).
    pub username: Option<String>,
    /// Password for `username` when security is enabled.
    pub password: Option<String>,
}

#[cfg(feature = "elasticsearch")]
impl ElasticsearchBindings {
    /// Full index name for a logical index name under this scenario's prefix.
    pub fn index_namespace(&self, logical_index: &str) -> String {
        format!("{}{}", self.index_prefix, logical_index)
    }
}

#[cfg(feature = "elasticsearch")]
impl Namespaced for ElasticsearchBindings {
    /// Return `name` prefixed with this scenario's Elasticsearch index prefix.
    fn namespace(&self, name: &str) -> String {
        format!("{}{}", self.index_prefix, name)
    }
}

#[cfg(feature = "elasticsearch")]
impl Isolatable for ElasticsearchBindings {
    /// Return new Bindings with a deeper index prefix (`{current}{name}_`) — no I/O.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        Ok(ElasticsearchBindings {
            url: self.url.clone(),
            host: self.host.clone(),
            port: self.port,
            index_prefix: format!("{}{}_", self.index_prefix, name),
            username: self.username.clone(),
            password: self.password.clone(),
        })
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
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for OpenSearch HTTP.
    pub port: u16,
    /// Prefix for logical index names (`shakedown_<scenario>_`).
    pub index_prefix: String,
    /// Set when [`OpenSearchResource::with_security`] was used (`admin` user by default).
    pub username: Option<String>,
    /// Password for `username` when security is enabled.
    pub password: Option<String>,
}

#[cfg(feature = "opensearch")]
impl OpenSearchBindings {
    /// Full index name for a logical index name under this scenario's prefix.
    pub fn index_namespace(&self, logical_index: &str) -> String {
        format!("{}{}", self.index_prefix, logical_index)
    }
}

#[cfg(feature = "opensearch")]
impl Namespaced for OpenSearchBindings {
    /// Return `name` prefixed with this scenario's OpenSearch index prefix.
    fn namespace(&self, name: &str) -> String {
        format!("{}{}", self.index_prefix, name)
    }
}

#[cfg(feature = "opensearch")]
impl Isolatable for OpenSearchBindings {
    /// Return new Bindings with a deeper index prefix (`{current}{name}_`) — no I/O.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        Ok(OpenSearchBindings {
            url: self.url.clone(),
            host: self.host.clone(),
            port: self.port,
            index_prefix: format!("{}{}_", self.index_prefix, name),
            username: self.username.clone(),
            password: self.password.clone(),
        })
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
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for Qdrant gRPC.
    pub grpc_port: u16,
    /// Mapped host port for Qdrant REST.
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

#[cfg(feature = "qdrant")]
impl Namespaced for QdrantBindings {
    /// Return `name` prefixed with this scenario's Qdrant collection prefix.
    fn namespace(&self, name: &str) -> String {
        format!("{}{}", self.collection_prefix, name)
    }
}

#[cfg(feature = "qdrant")]
impl Isolatable for QdrantBindings {
    /// Return new Bindings with a deeper collection prefix (`{current}{name}_`) — no I/O.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        Ok(QdrantBindings {
            grpc_url: self.grpc_url.clone(),
            rest_url: self.rest_url.clone(),
            host: self.host.clone(),
            grpc_port: self.grpc_port,
            rest_port: self.rest_port,
            collection_prefix: format!("{}{}_", self.collection_prefix, name),
        })
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
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for Weaviate REST.
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

#[cfg(feature = "weaviate")]
impl Namespaced for WeaviateBindings {
    /// Return `name` prefixed with this scenario's Weaviate class prefix, capitalised (Weaviate
    /// requires class names to start with an uppercase letter).
    fn namespace(&self, name: &str) -> String {
        let raw = format!("{}{}", self.class_prefix, name);
        let mut chars = raw.chars();
        match chars.next() {
            None => unreachable!("Weaviate class prefix is always non-empty after provision"),
            Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
        }
    }
}

#[cfg(feature = "weaviate")]
impl Isolatable for WeaviateBindings {
    /// Return new Bindings with a deeper class prefix (`{current}{name}_`) — no I/O.
    ///
    /// The returned prefix is capitalised as required by Weaviate class naming rules.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        Ok(WeaviateBindings {
            rest_url: self.rest_url.clone(),
            host: self.host.clone(),
            port: self.port,
            class_prefix: format!("{}{}_", self.class_prefix, name),
        })
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
    /// Hostname or IP (typically `127.0.0.1` for the suite container).
    pub host: String,
    /// Mapped host port for InfluxDB HTTP.
    pub port: u16,
    /// Admin token (fixed in the default container init — matches `DOCKER_INFLUXDB_INIT_ADMIN_TOKEN`).
    pub token: String,
    /// InfluxDB organization name (matches `DOCKER_INFLUXDB_INIT_ORG`).
    pub organization: String,
    /// Per-scenario bucket name.
    pub bucket: String,
}

/// Implementation of [`Isolatable`] for [`InfluxDbBindings`] (creates a new bucket via HTTP API — I/O).
#[cfg(feature = "influxdb")]
impl Isolatable for InfluxDbBindings {
    /// Create a new InfluxDB bucket named `name` using the HTTP API.
    ///
    /// The bucket is created in the same organization as the parent Bindings. It is cleaned up
    /// when the suite container tears down.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        let safe_name = influxdb::sanitize_bucket_name(name);
        let organization_id =
            influxdb::fetch_organization_id(&self.url, &self.token, &self.organization)
                .await
                .map_err(|error| InfrastructureError::Provision {
                    resource: format!("influxdb:isolate:{name}"),
                    source: Box::new(std::io::Error::other(error)),
                })?;
        influxdb::create_bucket(&self.url, &self.token, &organization_id, &safe_name)
            .await
            .map_err(|error| InfrastructureError::Provision {
                resource: format!("influxdb:isolate:{name}"),
                source: Box::new(std::io::Error::other(error)),
            })?;
        Ok(InfluxDbBindings {
            url: self.url.clone(),
            host: self.host.clone(),
            port: self.port,
            token: self.token.clone(),
            organization: self.organization.clone(),
            bucket: safe_name,
        })
    }
}

/// Connection details for a provisioned MongoDB scenario database (`mongodb` feature).
///
/// Filled from [`ResourceHandle::metadata`] by the lifecycle; read from
/// [`crate::lifecycle::Context::mongo_bindings`] after provision.
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

/// Implementation of [`Isolatable`] for [`MongoBindings`] — returns new Bindings with a different
/// database name. No I/O: MongoDB creates the database on first write.
#[cfg(feature = "mongodb")]
impl Isolatable for MongoBindings {
    /// Return new Bindings targeting a MongoDB database named `name`.
    ///
    /// No I/O is performed — MongoDB creates the database on first write. All connection details
    /// (host, port, credentials) are preserved from the parent Bindings.
    async fn isolate(&self, name: &str) -> Result<Self, InfrastructureError> {
        let connection_url = format!(
            "mongodb://{}:{}@{}:{}/{}?authSource=admin",
            self.username, self.password, self.host, self.port, name
        );
        Ok(MongoBindings {
            host: self.host.clone(),
            port: self.port,
            database: name.to_string(),
            username: self.username.clone(),
            password: self.password.clone(),
            connection_url,
            replica_set_host_ports: self.replica_set_host_ports.clone(),
        })
    }
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

    #[cfg(feature = "neo4j")]
    #[test]
    fn sanitize_neo4j_database_name_rules() {
        assert_eq!(
            super::sanitize_neo4j_database_name("neo4j_vertical_proof"),
            "shakedown-neo4j-vertical-proof"
        );
        assert_eq!(
            super::sanitize_neo4j_database_name(""),
            "shakedown-scenario"
        );
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

    #[test]
    fn kafka_bindings_namespaced_trait() {
        let bindings = KafkaBindings::new("localhost:9092", "shakedown_abc_");
        assert_eq!(bindings.namespace("orders"), "shakedown_abc_orders");
        assert_eq!(
            bindings.topic_namespace("orders"),
            bindings.namespace("orders")
        );
    }

    #[test]
    fn kafka_consumer_group_namespace_uses_topic_prefix() {
        // Given Kafka Bindings with a known scenario prefix
        let bindings = KafkaBindings::new("localhost:9092", "shakedown_abc_");
        // When we namespace a production consumer group ID
        let group = bindings.consumer_group_namespace("analysis-consumer-input");
        // Then it is prefixed identically to topics, isolating the group per scenario
        assert_eq!(group, "shakedown_abc_analysis-consumer-input");
        assert_eq!(group, bindings.topic_namespace("analysis-consumer-input"));
    }

    #[test]
    fn namespaced_generic_function_compiles_and_works() {
        fn make_name<B: Namespaced>(bindings: &B, logical: &str) -> String {
            bindings.namespace(logical)
        }
        let bindings = KafkaBindings::new("localhost:9092", "shakedown_abc_");
        assert_eq!(make_name(&bindings, "payments"), "shakedown_abc_payments");
    }

    #[tokio::test]
    async fn kafka_bindings_isolate_deepens_prefix() {
        // Given Kafka Bindings with a base scenario prefix
        let bindings = KafkaBindings::new("localhost:9092", "shakedown_abc_");
        // When we isolate a sub-context named "analytics"
        let sub = bindings
            .isolate("analytics")
            .await
            .expect("isolate should succeed");
        // Then the sub-prefix appends name + underscore separator and bootstrap is preserved
        assert_eq!(sub.topic_prefix, "shakedown_abc_analytics_");
        assert_eq!(sub.bootstrap_servers, "localhost:9092");
    }

    #[tokio::test]
    async fn isolatable_generic_function_compiles_and_works() {
        // Given a generic function that uses the Isolatable trait
        async fn sub_context<B: Isolatable>(bindings: &B, name: &str) -> B {
            bindings
                .isolate(name)
                .await
                .expect("isolate should succeed")
        }
        let bindings = KafkaBindings::new("localhost:9092", "shakedown_abc_");
        // When we call it with a KafkaBindings
        let sub = sub_context(&bindings, "metrics").await;
        // Then we get correctly prefixed Bindings back
        assert_eq!(sub.topic_prefix, "shakedown_abc_metrics_");
    }

    #[cfg(any(feature = "redis", feature = "valkey"))]
    #[test]
    fn redis_bindings_namespaced_matches_key_namespace() {
        // Given Redis Bindings with a scenario key prefix
        let bindings = RedisBindings {
            connection_url: "redis://127.0.0.1:6379".to_string(),
            host: "127.0.0.1".to_string(),
            port: 6379,
            key_prefix: "shakedown:abc:".to_string(),
        };
        // When we use Namespaced vs key_namespace
        // Then they agree for the same logical key
        assert_eq!(
            bindings.namespace("orders"),
            bindings.key_namespace("orders")
        );
    }

    #[cfg(any(feature = "redis", feature = "valkey"))]
    #[tokio::test]
    async fn redis_bindings_isolate_deepens_prefix() {
        // Given Redis Bindings with a base key prefix
        let bindings = RedisBindings {
            connection_url: "redis://127.0.0.1:6379".to_string(),
            host: "127.0.0.1".to_string(),
            port: 6379,
            key_prefix: "shakedown:abc:".to_string(),
        };
        // When we isolate "events"
        let sub = bindings
            .isolate("events")
            .await
            .expect("isolate should succeed");
        // Then the key prefix appends name + colon separator and connection details are preserved
        assert_eq!(sub.key_prefix, "shakedown:abc:events:");
        assert_eq!(sub.connection_url, "redis://127.0.0.1:6379");
    }

    #[cfg(feature = "redis-sentinel")]
    #[tokio::test]
    async fn redis_sentinel_bindings_namespaced_and_isolate() {
        // Given Sentinel-backed Bindings with a key prefix
        let bindings = RedisSentinelBindings {
            sentinel_hosts: "127.0.0.1:26379".to_string(),
            master_name: "mymaster".to_string(),
            connection_url: "redis://127.0.0.1:6379".to_string(),
            replica_connection_urls: vec![],
            key_prefix: "shakedown:abc:".to_string(),
        };
        // When we compare trait namespace to key_namespace
        assert_eq!(bindings.namespace("k"), bindings.key_namespace("k"));
        // And sentinel_client builds from comma-separated sentinel endpoints (no I/O)
        let _master_via_sentinel = bindings
            .sentinel_client()
            .expect("SentinelClient::build from harness-shaped sentinel_hosts");
        // And we isolate a sub-context
        let sub = bindings.isolate("s").await.expect("isolate should succeed");
        // Then the key prefix deepens with a colon separator
        assert_eq!(sub.key_prefix, "shakedown:abc:s:");
    }

    #[cfg(feature = "nats")]
    #[tokio::test]
    async fn nats_bindings_namespace_isolate_and_nested() {
        // Given NATS Bindings with a hierarchical subject prefix
        let bindings = NatsBindings {
            connection_url: "nats://127.0.0.1:4222".to_string(),
            host: "127.0.0.1".to_string(),
            port: 4222,
            subject_prefix: "shakedown.abc.".to_string(),
        };
        assert_eq!(bindings.namespace("orders"), "shakedown.abc.orders");
        assert_eq!(
            bindings.subject_namespace("orders"),
            bindings.namespace("orders")
        );
        // When we isolate and nest
        let sub = bindings
            .isolate("payments")
            .await
            .expect("isolate should succeed");
        assert_eq!(sub.subject_prefix, "shakedown.abc.payments.");
        assert_eq!(sub.namespace("events"), "shakedown.abc.payments.events");
        let sub2 = sub.isolate("retry").await.expect("nested isolate");
        assert_eq!(sub2.subject_prefix, "shakedown.abc.payments.retry.");
    }

    #[cfg(feature = "rabbitmq")]
    #[tokio::test]
    async fn rabbit_bindings_namespaced_isolate_nested() {
        // Given RabbitMQ Bindings with a resource prefix
        let bindings = RabbitBindings::new(
            "amqp://guest:guest@127.0.0.1:5672/%2f",
            "127.0.0.1",
            5672,
            "guest",
            "guest",
            "/",
            "shakedown_abc_",
        );
        assert_eq!(bindings.namespace("q1"), bindings.queue_namespace("q1"));
        assert_eq!(
            bindings.namespace("ex1"),
            bindings.exchange_namespace("ex1")
        );
        assert_eq!(bindings.namespace("x"), bindings.namespaced_name("x"));
        let sub = bindings
            .isolate("sub")
            .await
            .expect("isolate should succeed");
        assert_eq!(sub.resource_prefix, "shakedown_abc_sub_");
        let sub2 = sub.isolate("inner").await.expect("nested isolate");
        assert_eq!(sub2.resource_prefix, "shakedown_abc_sub_inner_");
    }

    #[cfg(feature = "elasticsearch")]
    #[tokio::test]
    async fn elasticsearch_bindings_namespaced_isolate_nested() {
        let bindings = ElasticsearchBindings {
            url: "http://127.0.0.1:9200".to_string(),
            host: "127.0.0.1".to_string(),
            port: 9200,
            index_prefix: "shakedown_abc_".to_string(),
            username: None,
            password: None,
        };
        assert_eq!(bindings.namespace("idx"), bindings.index_namespace("idx"));
        let sub = bindings.isolate("a").await.expect("isolate should succeed");
        assert_eq!(sub.index_prefix, "shakedown_abc_a_");
        let sub2 = sub.isolate("b").await.expect("nested isolate");
        assert_eq!(sub2.index_prefix, "shakedown_abc_a_b_");
    }

    #[cfg(feature = "opensearch")]
    #[tokio::test]
    async fn opensearch_bindings_namespaced_isolate_nested() {
        let bindings = OpenSearchBindings {
            url: "http://127.0.0.1:9200".to_string(),
            host: "127.0.0.1".to_string(),
            port: 9200,
            index_prefix: "shakedown_abc_".to_string(),
            username: None,
            password: None,
        };
        assert_eq!(bindings.namespace("idx"), bindings.index_namespace("idx"));
        let sub = bindings.isolate("a").await.expect("isolate should succeed");
        assert_eq!(sub.index_prefix, "shakedown_abc_a_");
    }

    #[cfg(feature = "qdrant")]
    #[tokio::test]
    async fn qdrant_bindings_namespaced_isolate_nested() {
        let bindings = QdrantBindings {
            grpc_url: "http://127.0.0.1:6334".to_string(),
            rest_url: "http://127.0.0.1:6333".to_string(),
            host: "127.0.0.1".to_string(),
            grpc_port: 6334,
            rest_port: 6333,
            collection_prefix: "shakedown_abc_".to_string(),
        };
        assert_eq!(bindings.namespace("c"), bindings.collection_namespace("c"));
        let sub = bindings.isolate("a").await.expect("isolate should succeed");
        assert_eq!(sub.collection_prefix, "shakedown_abc_a_");
    }

    #[cfg(feature = "weaviate")]
    #[tokio::test]
    async fn weaviate_bindings_namespaced_isolate() {
        let bindings = WeaviateBindings {
            rest_url: "http://127.0.0.1:8080".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8080,
            class_prefix: "shakedown_abc_".to_string(),
        };
        assert_eq!(
            bindings.namespace("thing"),
            bindings.class_namespace("thing")
        );
        let sub = bindings
            .isolate("sub")
            .await
            .expect("isolate should succeed");
        assert_eq!(sub.class_prefix, "shakedown_abc_sub_");
    }

    #[cfg(feature = "mongodb")]
    #[tokio::test]
    async fn mongo_bindings_isolate_sets_database_and_url() {
        let parent = MongoBindings {
            host: "127.0.0.1".to_string(),
            port: 27017,
            database: "app".to_string(),
            username: "root".to_string(),
            password: "secret".to_string(),
            connection_url: "mongodb://root:secret@127.0.0.1:27017/app?authSource=admin"
                .to_string(),
            replica_set_host_ports: vec![],
        };
        let sub = parent
            .isolate("analytics")
            .await
            .expect("isolate should succeed");
        assert_eq!(sub.database, "analytics");
        assert!(sub.connection_url.contains("/analytics"));
        assert!(sub.connection_url.contains("authSource=admin"));
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn duckdb_bindings_isolate_distinct_path() {
        let original = DuckDbBindings {
            database_url: duckdb_named_memory_database_path("scenario_a"),
        };
        let sub = original
            .isolate("analytics")
            .await
            .expect("isolate should succeed");
        assert_ne!(sub.database_url, original.database_url);
        assert!(sub.database_url.contains("analytics"));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_bindings_isolate_returns_new_in_memory_db() {
        // Given a SQLite Bindings for one scenario
        let original = SqliteBindings {
            database_url: sqlite_named_memory_uri("scenario_a"),
        };
        // When we isolate to "analytics"
        let sub = original
            .isolate("analytics")
            .await
            .expect("isolate should succeed");
        // Then the sub URL is different from the original (independent database)
        assert_ne!(sub.database_url, original.database_url);
        assert!(sub.database_url.contains("analytics"));
    }
}
