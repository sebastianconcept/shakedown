//! Oracle MySQL typed helpers — [`MysqlHelpers`] for scenario-scoped SQL via [`mysql_async`].

use mysql_async::Params;
use mysql_async::Row;

use crate::error::{InfrastructureError, ShakedownError};
use crate::lifecycle::Context;
use crate::resources::mysql_family;
use crate::resources::MigrationRunner;
use crate::resources::MysqlBindings;

/// Facade for Oracle MySQL helpers bound to [`Context`].
pub struct MysqlHelpers<'ctx> {
    context: &'ctx Context,
}

impl<'ctx> MysqlHelpers<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    fn mysql_bindings(&self) -> Result<&MysqlBindings, ShakedownError> {
        self.context.mysql_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "mysql bindings".to_string(),
            ))
        })
    }

    /// Run an `INSERT`, `UPDATE`, `DELETE`, or DDL statement. Returns the number of rows affected
    /// or (for `SELECT`) the number of rows returned.
    pub async fn execute(
        &self,
        query: &str,
        params: impl Into<Params> + Send,
    ) -> Result<u64, ShakedownError> {
        let bindings = self.mysql_bindings()?;
        mysql_family::execute(bindings.connection_url.as_str(), query, params).await
    }

    /// Assert that `query` returns at least one row.
    pub async fn assert_row_exists(
        &self,
        query: &str,
        params: impl Into<Params> + Send,
    ) -> Result<(), ShakedownError> {
        let bindings = self.mysql_bindings()?;
        mysql_family::assert_row_exists(bindings.connection_url.as_str(), query, params).await
    }

    /// Assert that `query` returns exactly `expected` rows.
    pub async fn assert_row_count(
        &self,
        query: &str,
        params: impl Into<Params> + Send,
        expected: usize,
    ) -> Result<(), ShakedownError> {
        let bindings = self.mysql_bindings()?;
        mysql_family::assert_row_count(bindings.connection_url.as_str(), query, params, expected)
            .await
    }

    /// Run `query` and return exactly one row (error if zero or more than one).
    pub async fn query_one(
        &self,
        query: &str,
        params: impl Into<Params> + Send,
    ) -> Result<Row, ShakedownError> {
        let bindings = self.mysql_bindings()?;
        mysql_family::query_one(bindings.connection_url.as_str(), query, params).await
    }

    /// Run `query` and return all matching rows.
    pub async fn query_all(
        &self,
        query: &str,
        params: impl Into<Params> + Send,
    ) -> Result<Vec<Row>, ShakedownError> {
        let bindings = self.mysql_bindings()?;
        mysql_family::query_all(bindings.connection_url.as_str(), query, params).await
    }

    /// Run migrations against this scenario's database (same entry style as [`Self::execute`]).
    pub async fn run_migrations(
        &self,
        runner: &(impl MigrationRunner + ?Sized),
    ) -> Result<(), ShakedownError> {
        let bindings = self.mysql_bindings()?;
        bindings.run_migrations(runner).await
    }
}
