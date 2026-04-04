//! Minimal fulfillment service for Shakedown examples — MongoDB, RabbitMQ, Kafka consumer.
//!
//! ## Layout
//!
//! - [`config`] — environment variables
//! - [`error`] — [`ServiceError`](error::ServiceError), [`ConfigError`](error::ConfigError), [`StartupError`](error::StartupError)
//! - [`repository`] — [`FulfillmentRepository`](repository::FulfillmentRepository)
//! - [`fulfillment`] — idempotent create + Rabbit publish ([`idempotency`](idempotency))
//! - [`kafka_topic`] — `KAFKA_TOPIC_ORDERS` (aligned with order service)
//! - [`state`] — [`AppState`](state::AppState)
//! - [`http`] — Axum routes and server
//! - [`kafka_consumer`] — background consumer

pub mod config;
pub mod error;
pub mod fulfillment;
pub mod grpc;
pub mod http;
pub mod kafka_consumer;
pub mod kafka_topic;
pub mod repository;
pub mod state;

use crate::config::FulfillmentServiceConfig;
use crate::state::AppState;

/// Initialize clients from the environment, spawn the Kafka consumer, and run the HTTP server.
pub async fn run() -> Result<(), error::ServiceError> {
    let configuration =
        FulfillmentServiceConfig::from_environment().map_err(error::ServiceError::Config)?;
    let listen_port = configuration.listen_port;
    let kafka_bootstrap = configuration.kafka_bootstrap.clone();
    let grpc_listen_port = configuration.grpc_listen_port;

    let application_state = AppState::connect(&configuration)
        .await
        .map_err(error::ServiceError::Startup)?;

    let kafka_state = application_state.clone();
    tokio::spawn(async move {
        kafka_consumer::kafka_orders_consumer_loop(kafka_bootstrap, kafka_state).await;
    });

    let grpc_state = application_state.clone();
    let grpc_addr = std::net::SocketAddr::from(([0, 0, 0, 0], grpc_listen_port));
    tokio::spawn(async move {
        if let Err(error) = grpc::serve(grpc_state, grpc_addr).await {
            tracing::error!(?error, "gRPC server task ended");
        }
    });

    let application_router = http::router(application_state);
    http::serve(application_router, listen_port).await
}
