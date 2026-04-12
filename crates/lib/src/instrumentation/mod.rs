//! Observability during scenarios: structured event log, failure timeline, and Docker diagnostics.
//!
//! When a scenario fails it is critical to know whether the failure is essential
//! complexity (a real bug in your code) or accidental complexity (an infrastructure issue in the
//! test setup). Without phase-tagged infrastructure events a red test always raises the question
//! "was that real or flaky?" — eroding confidence in the entire test suite.
//!
//! Shakedown automatically collects timestamped [`event::Event`] values from every
//! participant (resources, bridges, faults, services) into an [`event::EventLog`]. On failure the
//! [`timeline`] module renders a correlated, human-readable summary ordered by timestamp and
//! tagged with the active lifecycle phase. When Docker-backed resources or `services` are enabled,
//! the `diagnostics` submodule streams container logs and Docker events (OOM, restart, health
//! transitions) into the same log for infrastructure-level root-cause hints.
//!
//! # Book reference
//!
//! Part IV, *Chapter 18: Diagnostics* — explains the failure timeline format, how phase tags
//! classify essential vs accidental complexity, and how to interpret Docker diagnostic events.

pub mod event;
pub mod timeline;

#[cfg(any(
    feature = "kafka",
    feature = "rabbitmq",
    feature = "postgres",
    feature = "mariadb",
    feature = "mysql",
    feature = "redis",
    feature = "redis-sentinel",
    feature = "valkey",
    feature = "mongodb",
    feature = "nats",
    feature = "neo4j",
    feature = "elasticsearch",
    feature = "opensearch",
    feature = "qdrant",
    feature = "weaviate",
    feature = "influxdb",
    feature = "timescaledb",
    feature = "services",
))]
pub mod diagnostics;
