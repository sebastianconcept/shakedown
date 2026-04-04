//! Errors connecting to MongoDB or RabbitMQ during startup.

use displaydoc::Display;

/// Failed to initialize external dependencies.
#[derive(Display, Debug, thiserror::Error)]
pub enum StartupError {
    /// mongodb client or operation failed during startup
    Mongo(#[source] mongodb::error::Error),
    /// rabbitmq connection failed
    Rabbit(#[source] lapin::Error),
}
