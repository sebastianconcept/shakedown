//! Typed helpers for produce, consume, and assert operations on [`crate::lifecycle::Context`] bindings.

use std::time::Duration;

use crate::error::ShakedownError;

/// Default wall-clock wait for helper `consume_until` when `timeout` is `None` (20 seconds).
///
/// Same 20s default as publication observation (`outbox` feature) and outbox Kafka/Rabbit verifiers.
pub const DEFAULT_HELPER_CONSUME_TIMEOUT: Duration = Duration::from_secs(20);

/// A message consumed by a helper's [`crate::helpers::kafka::KafkaHelpers::consume_until`] (and
/// symmetric RabbitMQ helpers): key/payload bytes plus the fully qualified topic or queue name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumedMessage {
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    /// Full Kafka topic or AMQP queue name (namespaced).
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

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(any(feature = "mariadb", feature = "mysql"))]
mod mysql_family;

#[cfg(feature = "mariadb")]
pub mod mariadb;

#[cfg(feature = "mysql")]
pub mod mysql;

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
