//! `run_migrations` / optional sqlx pools on SQL [`crate::resources`] binding types.

#[cfg(any(
    feature = "postgres",
    feature = "timescaledb",
    feature = "sqlite",
    feature = "mariadb",
    feature = "mysql",
))]
use crate::error::ShakedownError;
#[cfg(any(
    feature = "postgres",
    feature = "timescaledb",
    feature = "sqlite",
    feature = "mariadb",
    feature = "mysql",
))]
use crate::resources::migration_runner::{map_migration_failure, MigrationRunner};

#[cfg(any(feature = "postgres", feature = "timescaledb"))]
impl crate::resources::PostgresBindings {
    /// Run migrations against this scenario's database using `runner`.
    ///
    /// Maps failures to [`ShakedownError::Infrastructure`].
    pub async fn run_migrations(
        &self,
        runner: &(impl MigrationRunner + ?Sized),
    ) -> Result<(), ShakedownError> {
        runner
            .run_migrations(self.connection_url.as_str())
            .await
            .map_err(map_migration_failure)
    }

    /// Open a [`sqlx::postgres::PgPool`](sqlx::postgres::PgPool) for this scenario's database (`postgres` + `sqlx` + Postgres or TimescaleDB bindings).
    #[cfg(all(feature = "sqlx", any(feature = "postgres", feature = "timescaledb")))]
    pub async fn pg_pool(&self) -> Result<sqlx::postgres::PgPool, ShakedownError> {
        sqlx::postgres::PgPool::connect(self.connection_url.as_str())
            .await
            .map_err(|error| map_migration_failure(Box::new(error)))
    }
}

#[cfg(feature = "sqlite")]
impl crate::resources::SqliteBindings {
    /// Run migrations against this scenario's database using `runner`.
    pub async fn run_migrations(
        &self,
        runner: &(impl MigrationRunner + ?Sized),
    ) -> Result<(), ShakedownError> {
        runner
            .run_migrations(self.database_url.as_str())
            .await
            .map_err(map_migration_failure)
    }

    /// Open a [`sqlx::sqlite::SqlitePool`](sqlx::sqlite::SqlitePool) for this scenario's database (`sqlite` + `sqlx`).
    ///
    /// Uses URI connection options so that Shakedown's `file:…?mode=memory&cache=shared` in-memory
    /// URIs open against the **same** named database as the rusqlite connection held by the harness.
    #[cfg(all(feature = "sqlite", feature = "sqlx"))]
    pub async fn sqlite_pool(&self) -> Result<sqlx::sqlite::SqlitePool, ShakedownError> {
        use sqlx::sqlite::SqliteConnectOptions;
        use std::str::FromStr as _;
        let options = SqliteConnectOptions::from_str(self.database_url.as_str())
            .map_err(|error| map_migration_failure(Box::new(error)))?;
        sqlx::sqlite::SqlitePool::connect_with(options)
            .await
            .map_err(|error| map_migration_failure(Box::new(error)))
    }
}

#[cfg(feature = "mariadb")]
impl crate::resources::MariaDBBindings {
    /// Run migrations against this scenario's database using `runner`.
    pub async fn run_migrations(
        &self,
        runner: &(impl MigrationRunner + ?Sized),
    ) -> Result<(), ShakedownError> {
        runner
            .run_migrations(self.connection_url.as_str())
            .await
            .map_err(map_migration_failure)
    }
}

#[cfg(feature = "mysql")]
impl crate::resources::MysqlBindings {
    /// Run migrations against this scenario's database using `runner`.
    pub async fn run_migrations(
        &self,
        runner: &(impl MigrationRunner + ?Sized),
    ) -> Result<(), ShakedownError> {
        runner
            .run_migrations(self.connection_url.as_str())
            .await
            .map_err(map_migration_failure)
    }
}
