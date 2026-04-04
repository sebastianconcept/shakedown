//! Error hierarchy: config → startup → serve.

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
