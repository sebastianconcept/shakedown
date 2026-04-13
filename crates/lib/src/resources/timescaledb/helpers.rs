//! TimescaleDB typed helpers — [`TimescaledbHelpers`] (`timescaledb` feature).
//!
//! Delegates SQL operations to the same helpers as PostgreSQL; adds hypertable assertions.

use tokio_postgres::types::ToSql;
use tokio_postgres::Row;

use crate::error::named_binding::not_provisioned_named_binding;
use crate::error::ShakedownError;
use crate::lifecycle::{Context, DEFAULT_RESOURCE_BINDING_NAME};
use crate::resources::postgres::{
    postgres_assert_row_count, postgres_assert_row_exists, postgres_execute, postgres_query_all,
};
use crate::resources::MigrationRunner;
use crate::resources::PostgresBindings;

/// Facade for TimescaleDB helpers bound to [`Context`] (`timescaledb` feature).
pub struct TimescaledbHelpers<'ctx> {
    context: &'ctx Context,
    binding_name: String,
}

impl<'ctx> TimescaledbHelpers<'ctx> {
    /// Helpers scoped to the [`DEFAULT_RESOURCE_BINDING_NAME`] binding.
    pub fn new(context: &'ctx Context) -> Result<Self, ShakedownError> {
        Self::named(context, DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Helpers scoped to the binding registered under `name` in [`Context`].
    pub fn named(context: &'ctx Context, name: &str) -> Result<Self, ShakedownError> {
        if context.timescaledb_bindings_named(name).is_none() {
            return Err(not_provisioned_named_binding("timescaledb bindings", name));
        }
        Ok(Self {
            context,
            binding_name: name.to_string(),
        })
    }

    fn timescaledb_bindings(&self) -> Result<&PostgresBindings, ShakedownError> {
        self.context
            .timescaledb_bindings_named(self.binding_name.as_str())
            .ok_or_else(|| {
                not_provisioned_named_binding("timescaledb bindings", self.binding_name.as_str())
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

    /// Run migrations against this scenario's database (same entry style as [`Self::execute`]).
    pub async fn run_migrations(
        &self,
        runner: &(impl MigrationRunner + ?Sized),
    ) -> Result<(), ShakedownError> {
        let postgres_bindings = self.timescaledb_bindings()?;
        postgres_bindings.run_migrations(runner).await
    }

    /// Connected [`sqlx::postgres::PgPool`](sqlx::postgres::PgPool) for this scenario (`timescaledb` + `sqlx`).
    #[cfg(all(feature = "timescaledb", feature = "sqlx"))]
    pub async fn pg_pool(&self) -> Result<sqlx::postgres::PgPool, ShakedownError> {
        let postgres_bindings = self.timescaledb_bindings()?;
        postgres_bindings.pg_pool().await
    }
}
