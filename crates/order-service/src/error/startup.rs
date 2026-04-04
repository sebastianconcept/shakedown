//! Errors connecting to Postgres, Redis, or Kafka during startup.

use displaydoc::Display;

/// Failed to initialize external dependencies (databases, broker).
#[derive(Display, Debug, thiserror::Error)]
pub enum StartupError {
    /// postgres connection failed
    Postgres(#[source] tokio_postgres::Error),
    /// redis client or connection failed
    Redis(#[source] redis::RedisError),
    /// kafka producer could not be created
    KafkaProducer(#[source] rdkafka::error::KafkaError),
    /// fulfillment gRPC endpoint invalid or connection failed
    FulfillmentGrpc(#[source] tonic::transport::Error),
}
