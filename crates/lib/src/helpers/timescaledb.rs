//! TimescaleDB typed helpers — [`TimescaledbHelpers`] (`timescaledb` feature).
//!
//! Delegates SQL operations to the same helpers as PostgreSQL; adds hypertable assertions.

use tokio_postgres::types::ToSql;
use tokio_postgres::Row;

use crate::error::{InfrastructureError, ShakedownError};
use crate::helpers::postgres::{
    postgres_assert_row_count, postgres_assert_row_exists, postgres_execute, postgres_query_all,
};
use crate::lifecycle::Context;
use crate::resources::PostgresBindings;

/// Facade for TimescaleDB helpers bound to [`Context`] (`timescaledb` feature).
pub struct TimescaledbHelpers<'ctx> {
    context: &'ctx Context,
}

impl<'ctx> TimescaledbHelpers<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    fn timescaledb_bindings(&self) -> Result<&PostgresBindings, ShakedownError> {
        self.context.timescaledb_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "timescaledb bindings".to_string(),
            ))
        })
    }

    /// Run an `INSERT`, `UPDATE`, `DELETE`, or DDL statement.
    pub async fn execute(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, ShakedownError> {
        let postgres_bindings = self.timescaledb_bindings()?;
        postgres_execute(postgres_bindings, query, params).await
    }

    /// Run `query` and return all matching rows.
    pub async fn query(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, ShakedownError> {
        let postgres_bindings = self.timescaledb_bindings()?;
        postgres_query_all(postgres_bindings, query, params).await
    }

    /// Assert that `query` returns at least one row.
    pub async fn assert_row_exists(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<(), ShakedownError> {
        let postgres_bindings = self.timescaledb_bindings()?;
        postgres_assert_row_exists(postgres_bindings, query, params).await
    }

    /// Assert that `query` returns exactly `expected` rows.
    pub async fn assert_row_count(
        &self,
        query: &str,
        params: &[&(dyn ToSql + Sync)],
        expected: usize,
    ) -> Result<(), ShakedownError> {
        let postgres_bindings = self.timescaledb_bindings()?;
        postgres_assert_row_count(postgres_bindings, query, params, expected).await
    }

    /// Assert a hypertable named `table_name` exists (see `timescaledb_information.hypertables`).
    pub async fn assert_hypertable_exists(&self, table_name: &str) -> Result<(), ShakedownError> {
        let postgres_bindings = self.timescaledb_bindings()?;
        postgres_assert_row_exists(
            postgres_bindings,
            "SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = $1 LIMIT 1",
            &[&table_name],
        )
        .await
    }
}
