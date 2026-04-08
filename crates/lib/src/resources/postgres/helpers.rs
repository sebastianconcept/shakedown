//! PostgreSQL typed helpers — [`PostgresHelpers`] for scenario-scoped SQL.
//!
//! Shared `postgres_*` functions are also used by TimescaleDB helpers (same wire protocol).

#[cfg(any(feature = "postgres", feature = "timescaledb"))]
use tokio_postgres::types::ToSql;
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
use tokio_postgres::{Client, NoTls, Row};

#[cfg(feature = "postgres")]
use crate::error::InfrastructureError;
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
use crate::error::{AssertionError, ScenarioError, ShakedownError};
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
use crate::helper_prelude::map_backend_error;
#[cfg(feature = "postgres")]
use crate::lifecycle::Context;
#[cfg(feature = "postgres")]
use crate::resources::MigrationRunner;
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
use crate::resources::PostgresBindings;

#[cfg(any(feature = "postgres", feature = "timescaledb"))]
async fn connect_client(postgres_bindings: &PostgresBindings) -> Result<Client, ShakedownError> {
    let (client, connection) =
        tokio_postgres::connect(postgres_bindings.connection_url.as_str(), NoTls)
            .await
            .map_err(map_backend_error)?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

/// Run DDL/DML using [`PostgresBindings`] (shared with TimescaleDB helpers).
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
pub(crate) async fn postgres_execute(
    postgres_bindings: &PostgresBindings,
    query: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64, ShakedownError> {
    let client = connect_client(postgres_bindings).await?;
    client
        .execute(query, params)
        .await
        .map_err(map_backend_error)
}

/// Assert at least one row (shared with TimescaleDB helpers).
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
pub(crate) async fn postgres_assert_row_exists(
    postgres_bindings: &PostgresBindings,
    query: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<(), ShakedownError> {
    let client = connect_client(postgres_bindings).await?;
    let rows = client
        .query(query, params)
        .await
        .map_err(map_backend_error)?;
    if rows.is_empty() {
        return Err(ShakedownError::Scenario(ScenarioError::Assertion(
            AssertionError::Unspecified {
                detail: "assert_row_exists: query returned no rows".to_string(),
            },
        )));
    }
    Ok(())
}

/// Assert exact row count (shared with TimescaleDB helpers).
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
pub(crate) async fn postgres_assert_row_count(
    postgres_bindings: &PostgresBindings,
    query: &str,
    params: &[&(dyn ToSql + Sync)],
    expected: usize,
) -> Result<(), ShakedownError> {
    let client = connect_client(postgres_bindings).await?;
    let rows = client
        .query(query, params)
        .await
        .map_err(map_backend_error)?;
    let actual = rows.len();
    if actual != expected {
        return Err(ShakedownError::Scenario(ScenarioError::Assertion(
            AssertionError::Unspecified {
                detail: format!("assert_row_count: expected {expected} rows, got {actual}"),
            },
        )));
    }
    Ok(())
}

/// Run `query` and return all matching rows (shared with TimescaleDB helpers).
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
pub(crate) async fn postgres_query_all(
    postgres_bindings: &PostgresBindings,
    query: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, ShakedownError> {
    let client = connect_client(postgres_bindings).await?;
    client.query(query, params).await.map_err(map_backend_error)
}

/// Facade for PostgreSQL helpers bound to [`Context`].
#[cfg(feature = "postgres")]
pub struct PostgresHelpers<'ctx> {
    context: &'ctx Context,
}

#[cfg(feature = "postgres")]
impl<'ctx> PostgresHelpers<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    fn postgres_bindings(&self) -> Result<&PostgresBindings, ShakedownError> {
        self.context.postgres_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "postgres bindings".to_string(),
            ))
        })
    }

    /// Run an `INSERT`, `UPDATE`, `DELETE`, or DDL statement. Returns the number of rows affected.
    pub async fn execute(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, ShakedownError> {
        let postgres_bindings = self.postgres_bindings()?;
        postgres_execute(postgres_bindings, query, params).await
    }

    /// Assert that `query` returns at least one row.
    pub async fn assert_row_exists(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<(), ShakedownError> {
        let postgres_bindings = self.postgres_bindings()?;
        postgres_assert_row_exists(postgres_bindings, query, params).await
    }

    /// Assert that `query` returns exactly `expected` rows.
    pub async fn assert_row_count(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
        expected: usize,
    ) -> Result<(), ShakedownError> {
        let postgres_bindings = self.postgres_bindings()?;
        postgres_assert_row_count(postgres_bindings, query, params, expected).await
    }

    /// Run `query` and return exactly one row (error if zero or more than one).
    pub async fn query_one(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, ShakedownError> {
        let postgres_bindings = self.postgres_bindings()?;
        let client = connect_client(postgres_bindings).await?;
        let mut rows = client
            .query(query, params)
            .await
            .map_err(map_backend_error)?;
        match rows.len() {
            0 => Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: "query_one: expected exactly one row, got zero".to_string(),
                },
            ))),
            1 => Ok(rows.remove(0)),
            n => Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: format!("query_one: expected exactly one row, got {n}"),
                },
            ))),
        }
    }

    /// Run `query` and return all matching rows.
    pub async fn query_all(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, ShakedownError> {
        let postgres_bindings = self.postgres_bindings()?;
        postgres_query_all(postgres_bindings, query, params).await
    }

    /// Run migrations against this scenario's database (same entry style as [`Self::execute`]).
    pub async fn run_migrations(
        &self,
        runner: &(impl MigrationRunner + ?Sized),
    ) -> Result<(), ShakedownError> {
        let postgres_bindings = self.postgres_bindings()?;
        postgres_bindings.run_migrations(runner).await
    }

    /// Connected [`sqlx::postgres::PgPool`](sqlx::postgres::PgPool) for this scenario (`postgres` + `sqlx`).
    #[cfg(all(feature = "postgres", feature = "sqlx"))]
    pub async fn pg_pool(&self) -> Result<sqlx::postgres::PgPool, ShakedownError> {
        let postgres_bindings = self.postgres_bindings()?;
        postgres_bindings.pg_pool().await
    }
}
