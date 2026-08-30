//! Typed helper facades (`kafka()`, `postgres()`, …) on [`super::Context`].
//!
//! This module is useful when a scenario body needs an engine helper bound to the current
//! context.
//! This module does not open connections during construction of the helper.

#[cfg(any(
    feature = "kafka",
    feature = "rabbitmq",
    feature = "postgres",
    feature = "mariadb",
    feature = "mysql",
    feature = "sqlite",
    feature = "duckdb",
    any(feature = "redis", feature = "valkey"),
    feature = "mongodb",
    feature = "nats",
    feature = "neo4j",
    feature = "elasticsearch",
    feature = "opensearch",
    feature = "qdrant",
    feature = "weaviate",
    feature = "influxdb",
    feature = "timescaledb",
))]
use crate::error::ShakedownError;
#[cfg(all(
    feature = "outbox",
    any(
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "sqlite",
        feature = "mongodb",
        feature = "duckdb"
    )
))]
use crate::outbox::Outbox;
#[cfg(feature = "duckdb")]
use crate::resources::duckdb::DuckDbHelpers;
#[cfg(feature = "elasticsearch")]
use crate::resources::elasticsearch::ElasticsearchHelpers;
#[cfg(feature = "influxdb")]
use crate::resources::influxdb::InfluxDbHelpers;
#[cfg(feature = "kafka")]
use crate::resources::kafka::KafkaHelpers;
#[cfg(feature = "mariadb")]
use crate::resources::mariadb::MariaDbHelpers;
#[cfg(feature = "mongodb")]
use crate::resources::mongo::MongoHelpers;
#[cfg(feature = "mysql")]
use crate::resources::mysql::MysqlHelpers;
#[cfg(feature = "nats")]
use crate::resources::nats::NatsHelpers;
#[cfg(feature = "neo4j")]
use crate::resources::neo4j::Neo4jHelpers;
#[cfg(feature = "opensearch")]
use crate::resources::opensearch::OpenSearchHelpers;
#[cfg(feature = "postgres")]
use crate::resources::postgres::PostgresHelpers;
#[cfg(feature = "qdrant")]
use crate::resources::qdrant::QdrantHelpers;
#[cfg(feature = "rabbitmq")]
use crate::resources::rabbit::RabbitHelpers;
#[cfg(any(feature = "redis", feature = "valkey"))]
use crate::resources::redis::RedisHelpers;
#[cfg(feature = "sqlite")]
use crate::resources::sqlite::SqliteHelpers;
#[cfg(feature = "timescaledb")]
use crate::resources::timescaledb::TimescaledbHelpers;
#[cfg(feature = "weaviate")]
use crate::resources::weaviate::WeaviateHelpers;

use super::Context;

impl Context {
    /// Build an outbox helper bound to this context (`outbox` + at least one storage engine feature).
    #[cfg(all(
        feature = "outbox",
        any(
            feature = "postgres",
            feature = "mariadb",
            feature = "mysql",
            feature = "sqlite",
            feature = "mongodb",
            feature = "duckdb"
        )
    ))]
    pub fn outbox(&self) -> Outbox<'_> {
        Outbox::new(self)
    }

    /// Kafka produce/consume helpers (`kafka` feature), scoped to the [`DEFAULT_RESOURCE_BINDING_NAME`] binding.
    #[cfg(feature = "kafka")]
    pub fn kafka(&self) -> Result<KafkaHelpers<'_>, ShakedownError> {
        KafkaHelpers::new(self)
    }

    /// Kafka helpers scoped to the binding registered under `name`.
    #[cfg(feature = "kafka")]
    pub fn kafka_named(&self, name: &str) -> Result<KafkaHelpers<'_>, ShakedownError> {
        KafkaHelpers::named(self, name)
    }

    /// RabbitMQ publish/topology helpers (`rabbitmq` feature).
    #[cfg(feature = "rabbitmq")]
    pub fn rabbit(&self) -> Result<RabbitHelpers<'_>, ShakedownError> {
        RabbitHelpers::new(self)
    }

    /// RabbitMQ helpers scoped to the binding registered under `name`.
    #[cfg(feature = "rabbitmq")]
    pub fn rabbit_named(&self, name: &str) -> Result<RabbitHelpers<'_>, ShakedownError> {
        RabbitHelpers::named(self, name)
    }

    /// PostgreSQL execute/query helpers (`postgres` feature).
    #[cfg(feature = "postgres")]
    pub fn postgres(&self) -> Result<PostgresHelpers<'_>, ShakedownError> {
        PostgresHelpers::new(self)
    }

    /// PostgreSQL helpers scoped to the binding registered under `name`.
    #[cfg(feature = "postgres")]
    pub fn postgres_named(&self, name: &str) -> Result<PostgresHelpers<'_>, ShakedownError> {
        PostgresHelpers::named(self, name)
    }

    /// MariaDB execute/query helpers (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    pub fn mariadb(&self) -> Result<MariaDbHelpers<'_>, ShakedownError> {
        MariaDbHelpers::new(self)
    }

    /// MariaDB helpers scoped to the binding registered under `name`.
    #[cfg(feature = "mariadb")]
    pub fn mariadb_named(&self, name: &str) -> Result<MariaDbHelpers<'_>, ShakedownError> {
        MariaDbHelpers::named(self, name)
    }

    /// Oracle MySQL execute/query helpers (`mysql` feature).
    #[cfg(feature = "mysql")]
    pub fn mysql(&self) -> Result<MysqlHelpers<'_>, ShakedownError> {
        MysqlHelpers::new(self)
    }

    /// Oracle MySQL helpers scoped to the binding registered under `name`.
    #[cfg(feature = "mysql")]
    pub fn mysql_named(&self, name: &str) -> Result<MysqlHelpers<'_>, ShakedownError> {
        MysqlHelpers::named(self, name)
    }

    /// SQLite execute/query helpers (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    pub fn sqlite(&self) -> Result<SqliteHelpers<'_>, ShakedownError> {
        SqliteHelpers::new(self)
    }

    /// SQLite helpers scoped to the binding registered under `name`.
    #[cfg(feature = "sqlite")]
    pub fn sqlite_named(&self, name: &str) -> Result<SqliteHelpers<'_>, ShakedownError> {
        SqliteHelpers::named(self, name)
    }

    /// DuckDB execute/query helpers (`duckdb` feature).
    #[cfg(feature = "duckdb")]
    pub fn duckdb(&self) -> Result<DuckDbHelpers<'_>, ShakedownError> {
        DuckDbHelpers::new(self)
    }

    /// DuckDB helpers scoped to the binding registered under `name`.
    #[cfg(feature = "duckdb")]
    pub fn duckdb_named(&self, name: &str) -> Result<DuckDbHelpers<'_>, ShakedownError> {
        DuckDbHelpers::named(self, name)
    }

    /// Redis / Valkey key/value helpers (`redis` and/or `valkey` features).
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn redis(&self) -> Result<RedisHelpers<'_>, ShakedownError> {
        RedisHelpers::new(self)
    }

    /// Redis / Valkey helpers scoped to the binding registered under `name`.
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn redis_named(&self, name: &str) -> Result<RedisHelpers<'_>, ShakedownError> {
        RedisHelpers::named(self, name)
    }

    /// MongoDB document helpers (`mongodb` feature).
    #[cfg(feature = "mongodb")]
    pub fn mongo(&self) -> Result<MongoHelpers<'_>, ShakedownError> {
        MongoHelpers::new(self)
    }

    /// MongoDB helpers scoped to the binding registered under `name`.
    #[cfg(feature = "mongodb")]
    pub fn mongo_named(&self, name: &str) -> Result<MongoHelpers<'_>, ShakedownError> {
        MongoHelpers::named(self, name)
    }

    /// NATS client helpers — publish/subscribe on scenario-namespaced subjects (`nats` feature). The suite broker runs with JetStream for [`crate::outbox::Outbox::assert_nats_published`].
    #[cfg(feature = "nats")]
    pub fn nats(&self) -> Result<NatsHelpers<'_>, ShakedownError> {
        NatsHelpers::new(self)
    }

    /// NATS helpers scoped to the binding registered under `name`.
    #[cfg(feature = "nats")]
    pub fn nats_named(&self, name: &str) -> Result<NatsHelpers<'_>, ShakedownError> {
        NatsHelpers::named(self, name)
    }

    /// Neo4j Cypher helpers (`neo4j` feature).
    #[cfg(feature = "neo4j")]
    pub fn neo4j(&self) -> Result<Neo4jHelpers<'_>, ShakedownError> {
        Neo4jHelpers::new(self)
    }

    /// Neo4j helpers scoped to the binding registered under `name`.
    #[cfg(feature = "neo4j")]
    pub fn neo4j_named(&self, name: &str) -> Result<Neo4jHelpers<'_>, ShakedownError> {
        Neo4jHelpers::named(self, name)
    }

    /// Elasticsearch index/search helpers (`elasticsearch` feature).
    #[cfg(feature = "elasticsearch")]
    pub fn elasticsearch(&self) -> Result<ElasticsearchHelpers<'_>, ShakedownError> {
        ElasticsearchHelpers::new(self)
    }

    /// Elasticsearch helpers scoped to the binding registered under `name`.
    #[cfg(feature = "elasticsearch")]
    pub fn elasticsearch_named(
        &self,
        name: &str,
    ) -> Result<ElasticsearchHelpers<'_>, ShakedownError> {
        ElasticsearchHelpers::named(self, name)
    }

    /// OpenSearch index/search helpers (`opensearch` feature).
    #[cfg(feature = "opensearch")]
    pub fn opensearch(&self) -> Result<OpenSearchHelpers<'_>, ShakedownError> {
        OpenSearchHelpers::new(self)
    }

    /// OpenSearch helpers scoped to the binding registered under `name`.
    #[cfg(feature = "opensearch")]
    pub fn opensearch_named(&self, name: &str) -> Result<OpenSearchHelpers<'_>, ShakedownError> {
        OpenSearchHelpers::named(self, name)
    }

    /// Qdrant vector helpers (`qdrant` feature).
    #[cfg(feature = "qdrant")]
    pub fn qdrant(&self) -> Result<QdrantHelpers<'_>, ShakedownError> {
        QdrantHelpers::new(self)
    }

    /// Qdrant helpers scoped to the binding registered under `name`.
    #[cfg(feature = "qdrant")]
    pub fn qdrant_named(&self, name: &str) -> Result<QdrantHelpers<'_>, ShakedownError> {
        QdrantHelpers::named(self, name)
    }

    /// Weaviate schema/object helpers (`weaviate` feature).
    #[cfg(feature = "weaviate")]
    pub fn weaviate(&self) -> Result<WeaviateHelpers<'_>, ShakedownError> {
        WeaviateHelpers::new(self)
    }

    /// Weaviate helpers scoped to the binding registered under `name`.
    #[cfg(feature = "weaviate")]
    pub fn weaviate_named(&self, name: &str) -> Result<WeaviateHelpers<'_>, ShakedownError> {
        WeaviateHelpers::named(self, name)
    }

    /// InfluxDB 2.x write/query helpers (`influxdb` feature).
    #[cfg(feature = "influxdb")]
    pub fn influxdb(&self) -> Result<InfluxDbHelpers<'_>, ShakedownError> {
        InfluxDbHelpers::new(self)
    }

    /// InfluxDB helpers scoped to the binding registered under `name`.
    #[cfg(feature = "influxdb")]
    pub fn influxdb_named(&self, name: &str) -> Result<InfluxDbHelpers<'_>, ShakedownError> {
        InfluxDbHelpers::named(self, name)
    }

    /// TimescaleDB SQL + hypertable helpers (`timescaledb` feature).
    #[cfg(feature = "timescaledb")]
    pub fn timescaledb(&self) -> Result<TimescaledbHelpers<'_>, ShakedownError> {
        TimescaledbHelpers::new(self)
    }

    /// TimescaleDB helpers scoped to the binding registered under `name`.
    #[cfg(feature = "timescaledb")]
    pub fn timescaledb_named(&self, name: &str) -> Result<TimescaledbHelpers<'_>, ShakedownError> {
        TimescaledbHelpers::named(self, name)
    }
}
