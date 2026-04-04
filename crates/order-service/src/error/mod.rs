//! Error hierarchy for the example order service: config → startup → serve.
//!
//! Conversions at call sites use explicit [`ServiceError::Config`], [`ServiceError::Startup`],
//! [`ServiceError::Serve`] so the boundary where an error is classified stays visible.

mod config;
mod startup;

pub use config::ConfigError;
pub use startup::StartupError;

use displaydoc::Display;

/// Top-level failure from [`crate::run`](crate::run).
#[derive(Display, Debug, thiserror::Error)]
pub enum ServiceError {
    /// configuration error
    Config(#[source] ConfigError),
    /// startup error
    Startup(#[source] StartupError),
    /// HTTP server error
    Serve(#[source] std::io::Error),
}
