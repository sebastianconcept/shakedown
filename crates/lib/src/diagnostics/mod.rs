//! Infrastructure diagnostics via Docker Engine API (bollard).
//!
//! Read-only: never creates, starts, or stops containers (see product DECISIONS).
//!
//! Enabled when a Docker-backed resource feature is active (`kafka`, `postgres`, `mariadb`, `mysql`)
//! unless the `no-diagnostics` feature is set.

mod connection;

pub mod labels;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
mod docker_events;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
pub(crate) use docker_events::{push_lifecycle_phase, DockerEventsMonitor};

pub use connection::{suite_diagnostics_handle, try_connect, DiagnosticsHandle};
