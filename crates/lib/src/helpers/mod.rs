//! Typed helpers for produce, consume, and assert operations on [`crate::lifecycle::Context`] bindings.

use std::time::Duration;

use crate::error::ShakedownError;

/// Default wall-clock wait for helper `consume_until` when `timeout` is `None` (20 seconds).
///
/// Same 20s default as publication observation (`outbox` feature) and outbox Kafka/Rabbit verifiers.
pub const DEFAULT_HELPER_CONSUME_TIMEOUT: Duration = Duration::from_secs(20);

#[cfg(any(
    feature = "kafka",
    feature = "rabbitmq",
    feature = "postgres",
    feature = "mariadb",
    feature = "mysql",
    feature = "sqlite",
    feature = "duckdb",
    feature = "redis",
    feature = "valkey",
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
pub(crate) fn map_backend_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> ShakedownError {
    ShakedownError::Infrastructure(crate::error::InfrastructureError::Backend(Box::new(error)))
}

/// A message consumed by [`crate::helpers::kafka::KafkaHelpers::consume_until`],
/// [`crate::helpers::rabbit::RabbitHelpers::consume_until`], or
/// [`crate::helpers::nats::NatsHelpers::subscribe_until`]: key/payload bytes plus the fully
/// qualified Kafka topic, AMQP queue name, or NATS subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumedMessage {
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    /// Full Kafka topic, AMQP queue name, or NATS subject (namespaced).
    pub source: String,
}

impl ConsumedMessage {
    /// UTF-8 key bytes, if present and valid.
    pub fn key_str(&self) -> Option<&str> {
        self.key
            .as_deref()
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
    }

    /// UTF-8 payload bytes, if present and valid.
    pub fn payload_str(&self) -> Option<&str> {
        self.payload
            .as_deref()
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
    }

    /// Deserialise JSON payload bytes into `T`.
    pub fn payload_json<T: serde::de::DeserializeOwned>(&self) -> Result<T, ShakedownError> {
        let payload = self
            .payload
            .as_deref()
            .ok_or_else(|| ShakedownError::PayloadDeserialisation("missing payload".to_string()))?;
        serde_json::from_slice(payload)
            .map_err(|error| ShakedownError::PayloadDeserialisation(error.to_string()))
    }
}

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "rabbitmq")]
pub mod rabbit;

#[cfg(any(feature = "postgres", feature = "timescaledb"))]
pub mod postgres;

#[cfg(any(feature = "mariadb", feature = "mysql"))]
mod mysql_family;

#[cfg(feature = "mariadb")]
pub mod mariadb;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "duckdb")]
pub mod duckdb;

#[cfg(any(feature = "redis", feature = "valkey"))]
pub mod redis;

#[cfg(feature = "mongodb")]
pub mod mongo;

#[cfg(feature = "nats")]
pub mod nats;

#[cfg(feature = "neo4j")]
pub mod neo4j;

#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;

#[cfg(feature = "opensearch")]
pub mod opensearch;

#[cfg(feature = "qdrant")]
pub mod qdrant;

#[cfg(feature = "weaviate")]
pub mod weaviate;

#[cfg(feature = "influxdb")]
pub mod influxdb;

#[cfg(feature = "timescaledb")]
pub mod timescaledb;

#[cfg(feature = "kafka")]
pub use kafka::KafkaHelpers;

#[cfg(feature = "rabbitmq")]
pub use rabbit::{ExchangeKind, RabbitHelpers};

#[cfg(feature = "postgres")]
pub use postgres::PostgresHelpers;

#[cfg(feature = "mariadb")]
pub use mariadb::MariaDbHelpers;

#[cfg(feature = "mysql")]
pub use mysql::MysqlHelpers;

#[cfg(feature = "sqlite")]
pub use sqlite::SqliteHelpers;

#[cfg(feature = "duckdb")]
pub use duckdb::DuckDbHelpers;

#[cfg(any(feature = "redis", feature = "valkey"))]
pub use redis::RedisHelpers;

#[cfg(feature = "mongodb")]
pub use mongo::MongoHelpers;

#[cfg(feature = "nats")]
pub use nats::NatsHelpers;

#[cfg(feature = "neo4j")]
pub use neo4j::Neo4jHelpers;

#[cfg(feature = "elasticsearch")]
pub use elasticsearch::ElasticsearchHelpers;

#[cfg(feature = "opensearch")]
pub use opensearch::OpenSearchHelpers;

#[cfg(feature = "qdrant")]
pub use qdrant::QdrantHelpers;

#[cfg(feature = "weaviate")]
pub use weaviate::WeaviateHelpers;

#[cfg(feature = "influxdb")]
pub use influxdb::{InfluxDbHelpers, InfluxFieldValue};

#[cfg(feature = "timescaledb")]
pub use timescaledb::TimescaledbHelpers;
