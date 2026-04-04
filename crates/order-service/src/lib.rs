//! Minimal order service for Shakedown examples — Postgres, Redis, Kafka, optional outbox.
//!
//! ## Layout
//!
//! - [`config`] — environment variables
//! - [`error`] — [`ServiceError`](error::ServiceError), [`ConfigError`](error::ConfigError), [`StartupError`](error::StartupError)
//! - [`repository`] — Postgres [`OrderRepository`](repository::OrderRepository)
//! - [`cache`] — Redis [`OrderCache`](cache::OrderCache)
//! - [`messaging`] — Kafka [`OrderMessaging`](messaging::OrderMessaging) when not using outbox
//! - [`kafka_topic`] — `KAFKA_TOPIC_ORDERS` (aligned with book / fulfillment consumer)
//! - [`state`] — [`AppState`](state::AppState)
//! - [`http`] — Axum routes and server

pub mod cache;
pub mod config;
pub mod error;
pub mod http;
pub mod kafka_topic;
pub mod messaging;
pub mod repository;
pub mod state;

use crate::config::OrderServiceConfig;
use crate::state::AppState;

/// Initialize clients from the environment and run the HTTP server until shutdown.
pub async fn run() -> Result<(), error::ServiceError> {
    let configuration =
        OrderServiceConfig::from_environment().map_err(error::ServiceError::Config)?;
    let listen_port = configuration.listen_port;
    let application_state = AppState::connect(configuration)
        .await
        .map_err(error::ServiceError::Startup)?;
    let application_router = http::router(application_state);
    http::serve(application_router, listen_port).await
}
