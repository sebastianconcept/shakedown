//! Environment-backed configuration for the order service.

use crate::error::ConfigError;

/// Values loaded from the process environment (see crate root `#!` for required variables).
#[derive(Debug, Clone)]
pub struct OrderServiceConfig {
    pub database_url: String,
    pub redis_url: String,
    pub kafka_bootstrap: String,
    /// Tonic endpoint for Fulfillment `Fulfill` RPC, e.g. `http://fulfillment-service:50051`.
    pub fulfillment_grpc_uri: Option<String>,
    pub outbox_enabled: bool,
    pub listen_port: u16,
}

impl OrderServiceConfig {
    /// Load configuration from environment variables.
    pub fn from_environment() -> Result<Self, ConfigError> {
        let database_url =
            std::env::var("DATABASE_URL").map_err(|_| ConfigError::MissingEnv("DATABASE_URL"))?;
        let redis_url =
            std::env::var("REDIS_URL").map_err(|_| ConfigError::MissingEnv("REDIS_URL"))?;
        let kafka_bootstrap = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
            .map_err(|_| ConfigError::MissingEnv("KAFKA_BOOTSTRAP_SERVERS"))?;
        let fulfillment_grpc_uri = std::env::var("FULFILLMENT_GRPC_URI").ok();
        let outbox_enabled = std::env::var("OUTBOX_ENABLED")
            .map(|value| value == "true" || value == "1")
            .unwrap_or(false);
        let listen_port: u16 = match std::env::var("PORT") {
            Ok(value) => value.parse().map_err(|_| ConfigError::InvalidPort)?,
            Err(_) => 3000,
        };

        Ok(Self {
            database_url,
            redis_url,
            kafka_bootstrap,
            fulfillment_grpc_uri,
            outbox_enabled,
            listen_port,
        })
    }
}
