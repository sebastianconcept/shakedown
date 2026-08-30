//! Named resource binding getters and setters on [`super::Context`].
//!
//! This module is useful when a scenario or helper must read or write a named engine binding.
//! This module does not parse provision handle metadata.

use super::super::environment::DEFAULT_RESOURCE_BINDING_NAME;
#[cfg(feature = "duckdb")]
use crate::resources::DuckDbBindings;
#[cfg(feature = "elasticsearch")]
use crate::resources::ElasticsearchBindings;
#[cfg(feature = "influxdb")]
use crate::resources::InfluxDbBindings;
use crate::resources::KafkaBindings;
#[cfg(feature = "mariadb")]
use crate::resources::MariaDBBindings;
#[cfg(feature = "mongodb")]
use crate::resources::MongoBindings;
#[cfg(feature = "mysql")]
use crate::resources::MysqlBindings;
#[cfg(feature = "nats")]
use crate::resources::NatsBindings;
#[cfg(feature = "neo4j")]
use crate::resources::Neo4jBindings;
#[cfg(feature = "opensearch")]
use crate::resources::OpenSearchBindings;
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
use crate::resources::PostgresBindings;
#[cfg(feature = "qdrant")]
use crate::resources::QdrantBindings;
#[cfg(feature = "rabbitmq")]
use crate::resources::RabbitBindings;
#[cfg(any(feature = "redis", feature = "valkey"))]
use crate::resources::RedisBindings;
#[cfg(feature = "redis-sentinel")]
use crate::resources::RedisSentinelBindings;
#[cfg(feature = "sqlite")]
use crate::resources::SqliteBindings;
#[cfg(feature = "weaviate")]
use crate::resources::WeaviateBindings;

use super::Context;

impl Context {
    /// Set Kafka bindings for this scenario (typically after Kafka provision).
    pub fn set_kafka_bindings(&mut self, kafka_bindings: KafkaBindings) {
        self.kafka_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), kafka_bindings);
    }

    /// Set Kafka bindings under a logical name (multi-Kafka topologies).
    pub fn set_kafka_bindings_named(&mut self, name: &str, kafka_bindings: KafkaBindings) {
        self.kafka_bindings.insert(name.to_string(), kafka_bindings);
    }

    /// Get a reference to Kafka bindings if available.
    pub fn kafka_bindings(&self) -> Option<&KafkaBindings> {
        self.kafka_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Kafka bindings for a named resource registration.
    pub fn kafka_bindings_named(&self, name: &str) -> Option<&KafkaBindings> {
        self.kafka_bindings.get(name)
    }

    /// Set RabbitMQ bindings for this scenario (typically after Rabbit provision).
    #[cfg(feature = "rabbitmq")]
    pub fn set_rabbit_bindings(&mut self, rabbit_bindings: RabbitBindings) {
        self.rabbit_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), rabbit_bindings);
    }

    /// Set RabbitMQ bindings under a logical name.
    #[cfg(feature = "rabbitmq")]
    pub fn set_rabbit_bindings_named(&mut self, name: &str, rabbit_bindings: RabbitBindings) {
        self.rabbit_bindings
            .insert(name.to_string(), rabbit_bindings);
    }

    /// Get RabbitMQ bindings if available (`rabbitmq` feature).
    #[cfg(feature = "rabbitmq")]
    pub fn rabbit_bindings(&self) -> Option<&RabbitBindings> {
        self.rabbit_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// RabbitMQ bindings for a named resource registration (`rabbitmq` feature).
    #[cfg(feature = "rabbitmq")]
    pub fn rabbit_bindings_named(&self, name: &str) -> Option<&RabbitBindings> {
        self.rabbit_bindings.get(name)
    }

    /// Set Postgres bindings for this scenario (typically after Postgres provision).
    #[cfg(feature = "postgres")]
    pub fn set_postgres_bindings(&mut self, postgres_bindings: PostgresBindings) {
        self.postgres_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), postgres_bindings);
    }

    /// Set Postgres bindings under a logical name.
    #[cfg(feature = "postgres")]
    pub fn set_postgres_bindings_named(&mut self, name: &str, postgres_bindings: PostgresBindings) {
        self.postgres_bindings
            .insert(name.to_string(), postgres_bindings);
    }

    /// Get Postgres bindings if available (`postgres` feature).
    #[cfg(feature = "postgres")]
    pub fn postgres_bindings(&self) -> Option<&PostgresBindings> {
        self.postgres_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Postgres bindings for a named resource registration (`postgres` feature).
    #[cfg(feature = "postgres")]
    pub fn postgres_bindings_named(&self, name: &str) -> Option<&PostgresBindings> {
        self.postgres_bindings.get(name)
    }

    /// Set MariaDB bindings for this scenario (typically after MariaDB provision).
    #[cfg(feature = "mariadb")]
    pub fn set_mariadb_bindings(&mut self, mariadb_bindings: MariaDBBindings) {
        self.mariadb_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), mariadb_bindings);
    }

    /// Set MariaDB bindings under a logical name.
    #[cfg(feature = "mariadb")]
    pub fn set_mariadb_bindings_named(&mut self, name: &str, mariadb_bindings: MariaDBBindings) {
        self.mariadb_bindings
            .insert(name.to_string(), mariadb_bindings);
    }

    /// Get MariaDB bindings if available (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    pub fn mariadb_bindings(&self) -> Option<&MariaDBBindings> {
        self.mariadb_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// MariaDB bindings for a named resource registration (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    pub fn mariadb_bindings_named(&self, name: &str) -> Option<&MariaDBBindings> {
        self.mariadb_bindings.get(name)
    }

    /// Set MySQL (Oracle) bindings for this scenario (typically after MySQL provision).
    #[cfg(feature = "mysql")]
    pub fn set_mysql_bindings(&mut self, mysql_bindings: MysqlBindings) {
        self.mysql_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), mysql_bindings);
    }

    /// Set MySQL bindings under a logical name.
    #[cfg(feature = "mysql")]
    pub fn set_mysql_bindings_named(&mut self, name: &str, mysql_bindings: MysqlBindings) {
        self.mysql_bindings.insert(name.to_string(), mysql_bindings);
    }

    /// Get Oracle MySQL bindings if available (`mysql` feature).
    #[cfg(feature = "mysql")]
    pub fn mysql_bindings(&self) -> Option<&MysqlBindings> {
        self.mysql_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// MySQL bindings for a named resource registration (`mysql` feature).
    #[cfg(feature = "mysql")]
    pub fn mysql_bindings_named(&self, name: &str) -> Option<&MysqlBindings> {
        self.mysql_bindings.get(name)
    }

    /// Set SQLite bindings for this scenario (typically after SQLite provision).
    #[cfg(feature = "sqlite")]
    pub fn set_sqlite_bindings(&mut self, sqlite_bindings: SqliteBindings) {
        self.sqlite_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), sqlite_bindings);
    }

    /// Set SQLite bindings under a logical name.
    #[cfg(feature = "sqlite")]
    pub fn set_sqlite_bindings_named(&mut self, name: &str, sqlite_bindings: SqliteBindings) {
        self.sqlite_bindings
            .insert(name.to_string(), sqlite_bindings);
    }

    /// Get SQLite bindings if available (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    pub fn sqlite_bindings(&self) -> Option<&SqliteBindings> {
        self.sqlite_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// SQLite bindings for a named resource registration (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    pub fn sqlite_bindings_named(&self, name: &str) -> Option<&SqliteBindings> {
        self.sqlite_bindings.get(name)
    }

    /// Set DuckDB bindings for this scenario (typically after DuckDB provision).
    #[cfg(feature = "duckdb")]
    pub fn set_duckdb_bindings(&mut self, duckdb_bindings: DuckDbBindings) {
        self.duckdb_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), duckdb_bindings);
    }

    /// Set DuckDB bindings under a logical name.
    #[cfg(feature = "duckdb")]
    pub fn set_duckdb_bindings_named(&mut self, name: &str, duckdb_bindings: DuckDbBindings) {
        self.duckdb_bindings
            .insert(name.to_string(), duckdb_bindings);
    }

    /// DuckDB connection path when a DuckDB resource populated them (`duckdb` feature).
    #[cfg(feature = "duckdb")]
    pub fn duckdb_bindings(&self) -> Option<&DuckDbBindings> {
        self.duckdb_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// DuckDB bindings for a named resource registration (`duckdb` feature).
    #[cfg(feature = "duckdb")]
    pub fn duckdb_bindings_named(&self, name: &str) -> Option<&DuckDbBindings> {
        self.duckdb_bindings.get(name)
    }

    /// Set Redis / Valkey bindings for this scenario (typically after Redis or Valkey provision).
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn set_redis_bindings(&mut self, redis_bindings: RedisBindings) {
        self.redis_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), redis_bindings);
    }

    /// Set Redis / Valkey bindings under a logical name.
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn set_redis_bindings_named(&mut self, name: &str, redis_bindings: RedisBindings) {
        self.redis_bindings.insert(name.to_string(), redis_bindings);
    }

    /// Redis or Valkey connection details when a matching resource populated them (`redis` / `valkey` features).
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn redis_bindings(&self) -> Option<&RedisBindings> {
        self.redis_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Redis or Valkey bindings for a named resource registration.
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn redis_bindings_named(&self, name: &str) -> Option<&RedisBindings> {
        self.redis_bindings.get(name)
    }

    /// Set Redis Sentinel discovery bindings when [`crate::BuiltinResource::RedisSentinel`] populated them.
    #[cfg(feature = "redis-sentinel")]
    pub fn set_redis_sentinel_bindings(&mut self, redis_sentinel_bindings: RedisSentinelBindings) {
        self.redis_sentinel_bindings.insert(
            DEFAULT_RESOURCE_BINDING_NAME.to_string(),
            redis_sentinel_bindings,
        );
    }

    /// Set Redis Sentinel bindings under a logical name.
    #[cfg(feature = "redis-sentinel")]
    pub fn set_redis_sentinel_bindings_named(
        &mut self,
        name: &str,
        redis_sentinel_bindings: RedisSentinelBindings,
    ) {
        self.redis_sentinel_bindings
            .insert(name.to_string(), redis_sentinel_bindings);
    }

    /// Sentinel endpoints + master name + primary URL (`redis-sentinel` feature).
    #[cfg(feature = "redis-sentinel")]
    pub fn redis_sentinel_bindings(&self) -> Option<&RedisSentinelBindings> {
        self.redis_sentinel_bindings
            .get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Redis Sentinel bindings for a named resource registration (`redis-sentinel` feature).
    #[cfg(feature = "redis-sentinel")]
    pub fn redis_sentinel_bindings_named(&self, name: &str) -> Option<&RedisSentinelBindings> {
        self.redis_sentinel_bindings.get(name)
    }

    /// Set MongoDB bindings for this scenario (typically after Mongo provision).
    #[cfg(feature = "mongodb")]
    pub fn set_mongo_bindings(&mut self, mongo_bindings: MongoBindings) {
        self.mongo_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), mongo_bindings);
    }

    /// Set MongoDB bindings under a logical name.
    #[cfg(feature = "mongodb")]
    pub fn set_mongo_bindings_named(&mut self, name: &str, mongo_bindings: MongoBindings) {
        self.mongo_bindings.insert(name.to_string(), mongo_bindings);
    }

    /// MongoDB connection details when a Mongo resource populated them (`mongodb` feature).
    #[cfg(feature = "mongodb")]
    pub fn mongo_bindings(&self) -> Option<&MongoBindings> {
        self.mongo_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// MongoDB bindings for a named resource registration (`mongodb` feature).
    #[cfg(feature = "mongodb")]
    pub fn mongo_bindings_named(&self, name: &str) -> Option<&MongoBindings> {
        self.mongo_bindings.get(name)
    }

    /// Set NATS bindings for this scenario (typically after NATS provision).
    #[cfg(feature = "nats")]
    pub fn set_nats_bindings(&mut self, nats_bindings: NatsBindings) {
        self.nats_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), nats_bindings);
    }

    /// Set NATS bindings under a logical name.
    #[cfg(feature = "nats")]
    pub fn set_nats_bindings_named(&mut self, name: &str, nats_bindings: NatsBindings) {
        self.nats_bindings.insert(name.to_string(), nats_bindings);
    }

    /// NATS connection details when a NATS resource populated them (`nats` feature).
    #[cfg(feature = "nats")]
    pub fn nats_bindings(&self) -> Option<&NatsBindings> {
        self.nats_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// NATS bindings for a named resource registration (`nats` feature).
    #[cfg(feature = "nats")]
    pub fn nats_bindings_named(&self, name: &str) -> Option<&NatsBindings> {
        self.nats_bindings.get(name)
    }

    /// Set Neo4j bindings for this scenario (typically after Neo4j provision).
    #[cfg(feature = "neo4j")]
    pub fn set_neo4j_bindings(&mut self, neo4j_bindings: Neo4jBindings) {
        self.neo4j_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), neo4j_bindings);
    }

    /// Set Neo4j bindings under a logical name.
    #[cfg(feature = "neo4j")]
    pub fn set_neo4j_bindings_named(&mut self, name: &str, neo4j_bindings: Neo4jBindings) {
        self.neo4j_bindings.insert(name.to_string(), neo4j_bindings);
    }

    /// Neo4j connection details when a Neo4j resource populated them (`neo4j` feature).
    #[cfg(feature = "neo4j")]
    pub fn neo4j_bindings(&self) -> Option<&Neo4jBindings> {
        self.neo4j_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Neo4j bindings for a named resource registration (`neo4j` feature).
    #[cfg(feature = "neo4j")]
    pub fn neo4j_bindings_named(&self, name: &str) -> Option<&Neo4jBindings> {
        self.neo4j_bindings.get(name)
    }

    /// Set Elasticsearch bindings for this scenario (typically after Elasticsearch provision).
    #[cfg(feature = "elasticsearch")]
    pub fn set_elasticsearch_bindings(&mut self, elasticsearch_bindings: ElasticsearchBindings) {
        self.elasticsearch_bindings.insert(
            DEFAULT_RESOURCE_BINDING_NAME.to_string(),
            elasticsearch_bindings,
        );
    }

    /// Set Elasticsearch bindings under a logical name.
    #[cfg(feature = "elasticsearch")]
    pub fn set_elasticsearch_bindings_named(
        &mut self,
        name: &str,
        elasticsearch_bindings: ElasticsearchBindings,
    ) {
        self.elasticsearch_bindings
            .insert(name.to_string(), elasticsearch_bindings);
    }

    /// Elasticsearch HTTP URL and index prefix when an Elasticsearch resource populated them (`elasticsearch` feature).
    #[cfg(feature = "elasticsearch")]
    pub fn elasticsearch_bindings(&self) -> Option<&ElasticsearchBindings> {
        self.elasticsearch_bindings
            .get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Elasticsearch bindings for a named resource registration (`elasticsearch` feature).
    #[cfg(feature = "elasticsearch")]
    pub fn elasticsearch_bindings_named(&self, name: &str) -> Option<&ElasticsearchBindings> {
        self.elasticsearch_bindings.get(name)
    }

    /// Set OpenSearch bindings for this scenario (typically after OpenSearch provision).
    #[cfg(feature = "opensearch")]
    pub fn set_opensearch_bindings(&mut self, opensearch_bindings: OpenSearchBindings) {
        self.opensearch_bindings.insert(
            DEFAULT_RESOURCE_BINDING_NAME.to_string(),
            opensearch_bindings,
        );
    }

    /// Set OpenSearch bindings under a logical name.
    #[cfg(feature = "opensearch")]
    pub fn set_opensearch_bindings_named(
        &mut self,
        name: &str,
        opensearch_bindings: OpenSearchBindings,
    ) {
        self.opensearch_bindings
            .insert(name.to_string(), opensearch_bindings);
    }

    /// OpenSearch HTTP URL and index prefix when an OpenSearch resource populated them (`opensearch` feature).
    #[cfg(feature = "opensearch")]
    pub fn opensearch_bindings(&self) -> Option<&OpenSearchBindings> {
        self.opensearch_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// OpenSearch bindings for a named resource registration (`opensearch` feature).
    #[cfg(feature = "opensearch")]
    pub fn opensearch_bindings_named(&self, name: &str) -> Option<&OpenSearchBindings> {
        self.opensearch_bindings.get(name)
    }

    /// Set Qdrant bindings for this scenario (typically after Qdrant provision).
    #[cfg(feature = "qdrant")]
    pub fn set_qdrant_bindings(&mut self, qdrant_bindings: QdrantBindings) {
        self.qdrant_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), qdrant_bindings);
    }

    /// Set Qdrant bindings under a logical name.
    #[cfg(feature = "qdrant")]
    pub fn set_qdrant_bindings_named(&mut self, name: &str, qdrant_bindings: QdrantBindings) {
        self.qdrant_bindings
            .insert(name.to_string(), qdrant_bindings);
    }

    /// Qdrant gRPC/REST endpoints and collection prefix when a Qdrant resource populated them (`qdrant` feature).
    #[cfg(feature = "qdrant")]
    pub fn qdrant_bindings(&self) -> Option<&QdrantBindings> {
        self.qdrant_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Qdrant bindings for a named resource registration (`qdrant` feature).
    #[cfg(feature = "qdrant")]
    pub fn qdrant_bindings_named(&self, name: &str) -> Option<&QdrantBindings> {
        self.qdrant_bindings.get(name)
    }

    /// Set Weaviate bindings for this scenario (typically after Weaviate provision).
    #[cfg(feature = "weaviate")]
    pub fn set_weaviate_bindings(&mut self, weaviate_bindings: WeaviateBindings) {
        self.weaviate_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), weaviate_bindings);
    }

    /// Set Weaviate bindings under a logical name.
    #[cfg(feature = "weaviate")]
    pub fn set_weaviate_bindings_named(&mut self, name: &str, weaviate_bindings: WeaviateBindings) {
        self.weaviate_bindings
            .insert(name.to_string(), weaviate_bindings);
    }

    /// Weaviate REST URL and class name prefix when a Weaviate resource populated them (`weaviate` feature).
    #[cfg(feature = "weaviate")]
    pub fn weaviate_bindings(&self) -> Option<&WeaviateBindings> {
        self.weaviate_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Weaviate bindings for a named resource registration (`weaviate` feature).
    #[cfg(feature = "weaviate")]
    pub fn weaviate_bindings_named(&self, name: &str) -> Option<&WeaviateBindings> {
        self.weaviate_bindings.get(name)
    }

    /// Set InfluxDB bindings for this scenario (typically after InfluxDB provision).
    #[cfg(feature = "influxdb")]
    pub fn set_influxdb_bindings(&mut self, influxdb_bindings: InfluxDbBindings) {
        self.influxdb_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), influxdb_bindings);
    }

    /// Set InfluxDB bindings under a logical name.
    #[cfg(feature = "influxdb")]
    pub fn set_influxdb_bindings_named(&mut self, name: &str, influxdb_bindings: InfluxDbBindings) {
        self.influxdb_bindings
            .insert(name.to_string(), influxdb_bindings);
    }

    /// InfluxDB HTTP URL, token, org, and scenario bucket when an InfluxDB resource populated them (`influxdb` feature).
    #[cfg(feature = "influxdb")]
    pub fn influxdb_bindings(&self) -> Option<&InfluxDbBindings> {
        self.influxdb_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// InfluxDB bindings for a named resource registration (`influxdb` feature).
    #[cfg(feature = "influxdb")]
    pub fn influxdb_bindings_named(&self, name: &str) -> Option<&InfluxDbBindings> {
        self.influxdb_bindings.get(name)
    }

    /// Set TimescaleDB bindings for this scenario (typically after TimescaleDB provision).
    #[cfg(feature = "timescaledb")]
    pub fn set_timescaledb_bindings(&mut self, timescaledb_bindings: PostgresBindings) {
        self.timescaledb_bindings.insert(
            DEFAULT_RESOURCE_BINDING_NAME.to_string(),
            timescaledb_bindings,
        );
    }

    /// Set TimescaleDB bindings under a logical name.
    #[cfg(feature = "timescaledb")]
    pub fn set_timescaledb_bindings_named(
        &mut self,
        name: &str,
        timescaledb_bindings: PostgresBindings,
    ) {
        self.timescaledb_bindings
            .insert(name.to_string(), timescaledb_bindings);
    }

    /// TimescaleDB connection details when a TimescaleDB resource populated them (`timescaledb` feature).
    #[cfg(feature = "timescaledb")]
    pub fn timescaledb_bindings(&self) -> Option<&PostgresBindings> {
        self.timescaledb_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// TimescaleDB bindings for a named resource registration (`timescaledb` feature).
    #[cfg(feature = "timescaledb")]
    pub fn timescaledb_bindings_named(&self, name: &str) -> Option<&PostgresBindings> {
        self.timescaledb_bindings.get(name)
    }
}
