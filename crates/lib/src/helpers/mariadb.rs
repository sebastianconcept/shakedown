//! MariaDB typed helpers — [`MariaDbHelpers`] for scenario-scoped SQL via [`mysql_async`].

use mysql_async::Params;
use mysql_async::Row;

use crate::error::{InfrastructureError, ShakedownError};
use crate::helpers::mysql_family;
use crate::lifecycle::Context;
use crate::resources::MariaDBBindings;

/// Facade for MariaDB helpers bound to [`Context`].
pub struct MariaDbHelpers<'ctx> {
    context: &'ctx Context,
}

impl<'ctx> MariaDbHelpers<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    fn mariadb_bindings(&self) -> Result<&MariaDBBindings, ShakedownError> {
        self.context.mariadb_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "mariadb bindings".to_string(),
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
        let bindings = self.mariadb_bindings()?;
        mysql_family::execute(bindings.connection_url.as_str(), query, params).await
    }

    /// Assert that `query` returns at least one row.
    pub async fn assert_row_exists(
        &self,
        query: &str,
        params: impl Into<Params> + Send,
    ) -> Result<(), ShakedownError> {
        let bindings = self.mariadb_bindings()?;
        mysql_family::assert_row_exists(bindings.connection_url.as_str(), query, params).await
    }

    /// Assert that `query` returns exactly `expected` rows.
    pub async fn assert_row_count(
        &self,
        query: &str,
        params: impl Into<Params> + Send,
        expected: usize,
    ) -> Result<(), ShakedownError> {
        let bindings = self.mariadb_bindings()?;
        mysql_family::assert_row_count(bindings.connection_url.as_str(), query, params, expected)
            .await
    }

    /// Run `query` and return exactly one row (error if zero or more than one).
    pub async fn query_one(
        &self,
        query: &str,
        params: impl Into<Params> + Send,
    ) -> Result<Row, ShakedownError> {
        let bindings = self.mariadb_bindings()?;
        mysql_family::query_one(bindings.connection_url.as_str(), query, params).await
    }

    /// Run `query` and return all matching rows.
    pub async fn query_all(
        &self,
        query: &str,
        params: impl Into<Params> + Send,
    ) -> Result<Vec<Row>, ShakedownError> {
        let bindings = self.mariadb_bindings()?;
        mysql_family::query_all(bindings.connection_url.as_str(), query, params).await
    }
}
