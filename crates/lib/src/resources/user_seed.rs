//! User-provided async seed callbacks stored on built-in resources ([`super::Resource::apply_default_seed`]).
//!
//! Use [`UserSeedFn`] and the per-bindings aliases so scenario code receives the correct binding type
//! when the harness invokes the closure during the seed phase.
//!
//! # When to use a seed path vs a closure
//!
//! For SQL resources, [`super::PostgresResource::with_seed_path`] (and the `seed = "..."` resource
//! macro argument) loads **static** reference data from a single `.sql` file — no scenario-specific
//! values, ideal for lookup tables and enums. Use [`PostgresResource::set_seed`] (and the same
//! pattern on other engines) when seed data depends on bindings (isolated database URL, topic
//! prefix), must be generated (UUIDs, timestamps), or uses async helpers. You may set both: the
//! file runs first, then the closure.

use std::future::Future;
use std::pin::Pin;

use crate::error::{InfrastructureError, ShakedownError};

/// Async seed closure: receives scenario bindings by value and returns a harness error.
///
/// Prefer the per-engine aliases ([`PostgresUserSeedFn`], [`KafkaUserSeedFn`], …) at call sites.
pub type UserSeedFn<Bindings> = Box<
    dyn FnOnce(Bindings) -> Pin<Box<dyn Future<Output = Result<(), ShakedownError>> + Send>> + Send,
>;

/// Map user/harness [`ShakedownError`] into [`InfrastructureError`] for [`super::Resource::apply_default_seed`].
pub(crate) fn map_seed_error(error: ShakedownError) -> InfrastructureError {
    match error {
        ShakedownError::Infrastructure(infrastructure_error) => infrastructure_error,
        other => InfrastructureError::Backend(Box::new(std::io::Error::other(other.to_string()))),
    }
}

#[cfg(feature = "postgres")]
pub type PostgresUserSeedFn = UserSeedFn<super::PostgresBindings>;

#[cfg(feature = "timescaledb")]
pub type TimescaleDbUserSeedFn = UserSeedFn<super::PostgresBindings>;

#[cfg(feature = "mariadb")]
pub type MariaDbUserSeedFn = UserSeedFn<super::MariaDBBindings>;

#[cfg(feature = "mysql")]
pub type MysqlUserSeedFn = UserSeedFn<super::MysqlBindings>;

#[cfg(feature = "sqlite")]
pub type SqliteUserSeedFn = UserSeedFn<super::SqliteBindings>;

#[cfg(feature = "kafka")]
pub type KafkaUserSeedFn = UserSeedFn<super::KafkaBindings>;

#[cfg(feature = "rabbitmq")]
pub type RabbitUserSeedFn = UserSeedFn<super::RabbitBindings>;

#[cfg(any(feature = "redis", feature = "valkey"))]
pub type RedisUserSeedFn = UserSeedFn<super::RedisBindings>;

#[cfg(feature = "mongodb")]
pub type MongoUserSeedFn = UserSeedFn<super::MongoBindings>;

#[cfg(feature = "nats")]
pub type NatsUserSeedFn = UserSeedFn<super::NatsBindings>;

#[cfg(feature = "neo4j")]
pub type Neo4jUserSeedFn = UserSeedFn<super::Neo4jBindings>;

#[cfg(feature = "elasticsearch")]
pub type ElasticsearchUserSeedFn = UserSeedFn<super::ElasticsearchBindings>;

#[cfg(feature = "opensearch")]
pub type OpenSearchUserSeedFn = UserSeedFn<super::OpenSearchBindings>;

#[cfg(feature = "qdrant")]
pub type QdrantUserSeedFn = UserSeedFn<super::QdrantBindings>;

#[cfg(feature = "weaviate")]
pub type WeaviateUserSeedFn = UserSeedFn<super::WeaviateBindings>;

#[cfg(feature = "influxdb")]
pub type InfluxDbUserSeedFn = UserSeedFn<super::InfluxDbBindings>;

#[cfg(feature = "duckdb")]
pub type DuckDbUserSeedFn = UserSeedFn<super::DuckDbBindings>;
