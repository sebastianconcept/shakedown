//! Errors loading [`crate::config::FulfillmentServiceConfig`](crate::config::FulfillmentServiceConfig).

use displaydoc::Display;

/// Invalid or missing environment for the fulfillment service.
#[derive(Display, Debug, thiserror::Error)]
pub enum ConfigError {
    /// missing required environment variable `{0}`
    MissingEnv(&'static str),
    /// PORT must be a valid u16
    InvalidPort,
}
