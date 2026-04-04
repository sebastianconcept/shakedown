//! Shared [`AppState`] for HTTP and the Kafka consumer task.

use std::sync::Arc;

use lapin::ConnectionProperties;

use crate::config::FulfillmentServiceConfig;
use crate::error::StartupError;
use crate::repository::FulfillmentRepository;

/// Axum [`State`](axum::extract::State) and input to the Kafka orders consumer loop.
#[derive(Clone)]
pub struct AppState {
    pub fulfillments: FulfillmentRepository,
    pub rabbit_connection: Arc<lapin::Connection>,
    pub payment_gateway_base_url: Option<String>,
}

impl AppState {
    /// Connect to MongoDB and RabbitMQ using the given configuration.
    pub async fn connect(configuration: &FulfillmentServiceConfig) -> Result<Self, StartupError> {
        let mongo_client = mongodb::Client::with_uri_str(&configuration.mongodb_uri)
            .await
            .map_err(StartupError::Mongo)?;
        let mongo_database = mongo_client.database("fulfillment");

        let rabbit_connection = Arc::new(
            lapin::Connection::connect(
                &configuration.rabbitmq_url,
                ConnectionProperties::default(),
            )
            .await
            .map_err(StartupError::Rabbit)?,
        );

        Ok(Self {
            fulfillments: FulfillmentRepository::new(mongo_database),
            rabbit_connection,
            payment_gateway_base_url: configuration.payment_gateway_base_url.clone(),
        })
    }
}
