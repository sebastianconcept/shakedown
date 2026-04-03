//! Shared helpers for typed resource helpers (`map_backend_error`, [`ConsumedMessage`], timeouts).
//!
//! Lives at crate root so `resources::<engine>::helpers` and `helpers` can both use it without
//! `helpers` ↔ `resources` dependency cycles.

use std::time::Duration;

use crate::error::ShakedownError;
use crate::OutboxError;

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
use crate::error::IntoInfrastructureError;

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
    error.into_infrastructure_error().into()
}

/// A message consumed by [`crate::resources::kafka::KafkaHelpers::consume_until`],
/// [`crate::resources::rabbit::RabbitHelpers::consume_until`], or [`crate::resources::nats::NatsHelpers::subscribe_until`]:
/// key/payload bytes plus the fully qualified Kafka topic, AMQP queue name, or NATS subject.
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
        let payload = self.payload.as_deref().ok_or_else(|| {
            ShakedownError::Outbox(OutboxError::PayloadInvalid {
                message: "missing payload".to_string(),
            })
        })?;
        serde_json::from_slice(payload).map_err(|e| ShakedownError::Outbox(e.into()))
    }
}
