//! Infrastructure diagnostics via the Docker Engine API ([`bollard`]).
//!
//! This module never creates, starts, or stops containers. Instead, it provides read-only observation of containers
//! that shakedown has already started (resource stats, log streaming, and engine events) plus optional resource budget assertions.
//!
//! Shakedown diagnostics gives you correlated infra signals in [`crate::event::EventLog`] and failure
//! timelines with better ergonomics than ad hoc `docker` CLI usage in tests.
//!
//! When a Docker-backed resource feature is active (`kafka`, `rabbitmq`, `postgres`, `mariadb`, `mysql`)
//! or `services`, `diagnostics` is enabled, and `no-diagnostics` is off, [`try_connect`] / [`suite_diagnostics_handle`]
//! provide a shared client; [`crate::lifecycle::run_scenario`] spawns background tasks and merges their
//! output during diagnostic shutdown.

mod connection;

pub mod labels;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
mod container_logs;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
mod docker_events;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
mod ring_buffer;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
mod resource_stats;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
mod assertions;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
pub(crate) use resource_stats::{ResourceStatsMonitor, SharedResourceStatsRings, StatsConfig};

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
pub(crate) use container_logs::ContainerLogsMonitor;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
pub(crate) use docker_events::{push_lifecycle_phase, DockerEventsMonitor};

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
pub use assertions::ResourceAssertBuilder;

pub use connection::{suite_diagnostics_handle, try_connect, DiagnosticsHandle};
