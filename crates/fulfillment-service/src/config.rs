//! Environment-backed configuration for the fulfillment service.

use crate::error::ConfigError;

/// Values loaded from the process environment.
#[derive(Debug, Clone)]
pub struct FulfillmentServiceConfig {
    pub mongodb_uri: String,
    pub kafka_bootstrap: String,
    pub rabbitmq_url: String,
    pub payment_gateway_base_url: Option<String>,
    /// HTTP (Axum): health, `GET /fulfill/:id`, optional `POST /fulfill`.
    pub listen_port: u16,
    /// gRPC unary `Fulfill` (Order → Fulfillment sync path). Default **50051**.
    pub grpc_listen_port: u16,
}

impl FulfillmentServiceConfig {
    /// Load configuration from environment variables.
    pub fn from_environment() -> Result<Self, ConfigError> {
        let mongodb_uri =
            std::env::var("MONGODB_URI").map_err(|_| ConfigError::MissingEnv("MONGODB_URI"))?;
        let kafka_bootstrap = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
            .map_err(|_| ConfigError::MissingEnv("KAFKA_BOOTSTRAP_SERVERS"))?;
        let rabbitmq_url =
            std::env::var("RABBITMQ_URL").map_err(|_| ConfigError::MissingEnv("RABBITMQ_URL"))?;
        let payment_gateway_base_url = std::env::var("PAYMENT_GATEWAY_URL").ok();
        let listen_port: u16 = match std::env::var("PORT") {
            Ok(value) => value.parse().map_err(|_| ConfigError::InvalidPort)?,
            Err(_) => 3001,
        };
        let grpc_listen_port: u16 = match std::env::var("GRPC_PORT") {
            Ok(value) => value.parse().map_err(|_| ConfigError::InvalidPort)?,
            Err(_) => 50051,
        };

        Ok(Self {
            mongodb_uri,
            kafka_bootstrap,
            rabbitmq_url,
            payment_gateway_base_url,
            listen_port,
            grpc_listen_port,
        })
    }
}
