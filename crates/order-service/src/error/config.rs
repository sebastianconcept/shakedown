//! Errors loading [`crate::config::OrderServiceConfig`](crate::config::OrderServiceConfig).

use displaydoc::Display;

/// Invalid or missing environment for the order service.
#[derive(Display, Debug, thiserror::Error)]
pub enum ConfigError {
    /// missing required environment variable `{0}`
    MissingEnv(&'static str),
    /// PORT must be a valid u16
    InvalidPort,
}
