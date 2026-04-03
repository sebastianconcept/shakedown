//! Observability during scenarios: structured [`event`] log, terminal / JSON [`timeline`], and
//! (when a Docker-backed resource or `services` is enabled) the `diagnostics` submodule.

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
