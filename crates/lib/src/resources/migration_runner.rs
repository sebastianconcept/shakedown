//! Framework-agnostic SQL migration bridge ([`MigrationRunner`]).
//!
//! Bindings for relational engines expose [`crate::resources::PostgresBindings::run_migrations`],
//! [`crate::resources::SqliteBindings::run_migrations`], and their MySQL-family counterparts.
//! Errors are mapped to [`ShakedownError::Infrastructure`](crate::error::ShakedownError::Infrastructure)
//! via [`InfrastructureError::Backend`](crate::error::InfrastructureError::Backend).

use async_trait::async_trait;

#[cfg(any(
    feature = "postgres",
    feature = "timescaledb",
    feature = "sqlite",
    feature = "mariadb",
    feature = "mysql",
    feature = "sqlx",
))]
use crate::error::{InfrastructureError, ShakedownError};

/// Runs SQL migrations against a database reachable at a connection URL.
///
/// Implement this trait for your migration tool (for example a Diesel runner, a custom `*.sql`
/// applier, or [`sqlx::migrate::Migrator`] when the `sqlx` feature is enabled — see
/// [`MigrationRunner`] implementation for `Migrator` below).
#[async_trait]
pub trait MigrationRunner: Send + Sync {
    /// Apply migrations for the database at `connection_url`.
    async fn run_migrations(
        &self,
        connection_url: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Map a migration failure into [`ShakedownError::Infrastructure`].
#[cfg(any(
    feature = "postgres",
    feature = "timescaledb",
    feature = "sqlite",
    feature = "mariadb",
    feature = "mysql",
    feature = "sqlx",
))]
pub(crate) fn map_migration_failure(
    source: Box<dyn std::error::Error + Send + Sync>,
) -> ShakedownError {
    ShakedownError::Infrastructure(InfrastructureError::Backend(source))
}

/// [`sqlx::migrate::Migrator`] (`sqlx` 0.8) is URL-driven: connect a pool from `connection_url`
/// and run pending migrations.
///
/// Supports:
/// - `postgresql://` / `postgres://` — requires the `postgres` Cargo feature.
/// - `sqlite:` or `file:` URIs — requires the `sqlite` Cargo feature. Shakedown's
///   in-memory SQLite bindings use `file:<name>?mode=memory&cache=shared` URIs, so both
///   schemes are recognised as SQLite for transparent integration.
#[cfg(feature = "sqlx")]
#[async_trait]
impl MigrationRunner for sqlx::migrate::Migrator {
    async fn run_migrations(
        &self,
        connection_url: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let lower = connection_url.to_ascii_lowercase();
        if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
            #[cfg(feature = "postgres")]
            {
                let pool = sqlx::postgres::PgPool::connect(connection_url)
                    .await
                    .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> {
                        Box::new(error)
                    })?;
                return self.run(&pool).await.map_err(
                    |error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) },
                );
            }
            #[cfg(not(feature = "postgres"))]
            {
                return Err(Box::new(std::io::Error::other(
                    "sqlx Migrator: postgres URLs require the `postgres` Cargo feature",
                )));
            }
        }
        // Match both `sqlite:` scheme and `file:` URIs (Shakedown in-memory SQLite uses `file:`).
        if lower.starts_with("sqlite:") || lower.starts_with("file:") {
            #[cfg(feature = "sqlite")]
            {
                use sqlx::sqlite::SqliteConnectOptions;
                use std::str::FromStr as _;
                let options = SqliteConnectOptions::from_str(connection_url).map_err(
                    |error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) },
                )?;
                let pool = sqlx::sqlite::SqlitePool::connect_with(options)
                    .await
                    .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> {
                        Box::new(error)
                    })?;
                return self.run(&pool).await.map_err(
                    |error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) },
                );
            }
            #[cfg(not(feature = "sqlite"))]
            {
                return Err(Box::new(std::io::Error::other(
                    "sqlx Migrator: sqlite/file URLs require the `sqlite` Cargo feature",
                )));
            }
        }
        Err(Box::new(std::io::Error::other(format!(
            "sqlx Migrator: unsupported connection URL scheme (expected postgres/postgresql, sqlite, or file): {}",
            connection_url.split('?').next().unwrap_or(connection_url)
        ))))
    }
}
