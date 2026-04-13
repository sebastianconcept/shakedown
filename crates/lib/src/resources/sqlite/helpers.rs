//! SQLite typed helpers — [`SqliteHelpers`] for scenario-scoped SQL via [`rusqlite`] on a blocking pool.
//!
//! Queries run inside [`tokio::task::spawn_blocking`] so the public API stays `async` while
//! [`rusqlite`] stays synchronous ([`crate::resources::SqliteBindings`] use a shared in-memory URI).

use rusqlite::types::Value;
use rusqlite::{Connection, OpenFlags, Params};

use crate::error::named_binding::not_provisioned_named_binding;
use crate::error::{AssertionError, ScenarioError, ShakedownError};
use crate::helper_prelude::map_backend_error;
use crate::lifecycle::{Context, DEFAULT_RESOURCE_BINDING_NAME};
use crate::resources::MigrationRunner;
use crate::resources::SqliteBindings;

fn open_connection(database_url: &str) -> Result<Connection, rusqlite::Error> {
    Connection::open_with_flags(
        database_url,
        OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE,
    )
}

fn row_to_values(row: &rusqlite::Row) -> rusqlite::Result<Vec<Value>> {
    let column_count = row.as_ref().column_count();
    let mut values = Vec::with_capacity(column_count);
    for column_index in 0..column_count {
        values.push(row.get::<_, Value>(column_index)?);
    }
    Ok(values)
}

/// Facade for SQLite helpers bound to [`Context`].
pub struct SqliteHelpers<'ctx> {
    context: &'ctx Context,
    binding_name: String,
}

impl<'ctx> SqliteHelpers<'ctx> {
    /// Helpers scoped to the [`DEFAULT_RESOURCE_BINDING_NAME`] binding.
    pub fn new(context: &'ctx Context) -> Result<Self, ShakedownError> {
        Self::named(context, DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Helpers scoped to the binding registered under `name` in [`Context`].
    pub fn named(context: &'ctx Context, name: &str) -> Result<Self, ShakedownError> {
        if context.sqlite_bindings_named(name).is_none() {
            return Err(not_provisioned_named_binding("sqlite bindings", name));
        }
        Ok(Self {
            context,
            binding_name: name.to_string(),
        })
    }

    fn sqlite_bindings(&self) -> Result<&SqliteBindings, ShakedownError> {
        self.context
            .sqlite_bindings_named(self.binding_name.as_str())
            .ok_or_else(|| {
                not_provisioned_named_binding("sqlite bindings", self.binding_name.as_str())
            })
    }

    /// Run an `INSERT`, `UPDATE`, `DELETE`, or DDL statement. Returns the number of rows affected.
    ///
    /// Pass parameters with [`rusqlite::params!`] or `()` when there are no bind arguments.
    pub async fn execute<P: Params + Send + 'static>(
        &self,
        query: &str,
        params: P,
    ) -> Result<u64, ShakedownError> {
        let sqlite_bindings = self.sqlite_bindings()?;
        let database_url = sqlite_bindings.database_url.clone();
        let query_owned = query.to_string();
        tokio::task::spawn_blocking(move || {
            let connection = open_connection(database_url.as_str())?;
            let affected = connection.execute(query_owned.as_str(), params)?;
            Ok::<u64, rusqlite::Error>(affected as u64)
        })
        .await
        .map_err(map_backend_error)?
        .map_err(map_backend_error)
    }

    /// Assert that `query` returns at least one row.
    pub async fn assert_row_exists<P: Params + Send + 'static>(
        &self,
        query: &str,
        params: P,
    ) -> Result<(), ShakedownError> {
        let sqlite_bindings = self.sqlite_bindings()?;
        let database_url = sqlite_bindings.database_url.clone();
        let query_owned = query.to_string();
        let found = tokio::task::spawn_blocking(move || {
            let connection = open_connection(database_url.as_str())?;
            let mut statement = connection.prepare(query_owned.as_str())?;
            let mut rows = statement.query(params)?;
            Ok::<bool, rusqlite::Error>(rows.next()?.is_some())
        })
        .await
        .map_err(map_backend_error)?
        .map_err(map_backend_error)?;
        if !found {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: "assert_row_exists: query returned no rows".to_string(),
                },
            )));
        }
        Ok(())
    }

    /// Assert that `query` returns exactly `expected` rows.
    pub async fn assert_row_count<P: Params + Send + 'static>(
        &self,
        query: &str,
        params: P,
        expected: usize,
    ) -> Result<(), ShakedownError> {
        let sqlite_bindings = self.sqlite_bindings()?;
        let database_url = sqlite_bindings.database_url.clone();
        let query_owned = query.to_string();
        let actual = tokio::task::spawn_blocking(move || {
            let connection = open_connection(database_url.as_str())?;
            let mut statement = connection.prepare(query_owned.as_str())?;
            let mut rows = statement.query(params)?;
            let mut count: usize = 0;
            while rows.next()?.is_some() {
                count += 1;
            }
            Ok::<usize, rusqlite::Error>(count)
        })
        .await
        .map_err(map_backend_error)?
        .map_err(map_backend_error)?;
        if actual != expected {
            return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                AssertionError::Unspecified {
                    detail: format!("assert_row_count: expected {expected} rows, got {actual}"),
                },
            )));
        }
        Ok(())
    }

    /// Run `query` and return exactly one row as column values in order (error if zero or more than one).
    pub async fn query_one<P: Params + Send + 'static>(
        &self,
        query: &str,
        params: P,
    ) -> Result<Vec<Value>, ShakedownError> {
        let sqlite_bindings = self.sqlite_bindings()?;
        let database_url = sqlite_bindings.database_url.clone();
        let query_owned = query.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<Value>, ShakedownError> {
            let connection = open_connection(database_url.as_str()).map_err(map_backend_error)?;
            let mut statement = connection
                .prepare(query_owned.as_str())
                .map_err(map_backend_error)?;
            let mut rows = statement.query(params).map_err(map_backend_error)?;
            let first = match rows.next().map_err(map_backend_error)? {
                Some(row) => row_to_values(row).map_err(map_backend_error)?,
                None => {
                    return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                        AssertionError::Unspecified {
                            detail: "query_one: expected exactly one row, got zero".to_string(),
                        },
                    )));
                }
            };
            if rows.next().map_err(map_backend_error)?.is_some() {
                return Err(ShakedownError::Scenario(ScenarioError::Assertion(
                    AssertionError::Unspecified {
                        detail: "query_one: expected exactly one row, got more than one"
                            .to_string(),
                    },
                )));
            }
            Ok(first)
        })
        .await
        .map_err(map_backend_error)?
    }

    /// Run `query` and return all matching rows as column values in order.
    pub async fn query_all<P: Params + Send + 'static>(
        &self,
        query: &str,
        params: P,
    ) -> Result<Vec<Vec<Value>>, ShakedownError> {
        let sqlite_bindings = self.sqlite_bindings()?;
        let database_url = sqlite_bindings.database_url.clone();
        let query_owned = query.to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<Vec<Value>>, ShakedownError> {
            let connection = open_connection(database_url.as_str()).map_err(map_backend_error)?;
            let mut statement = connection
                .prepare(query_owned.as_str())
                .map_err(map_backend_error)?;
            let mut rows = statement.query(params).map_err(map_backend_error)?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().map_err(map_backend_error)? {
                out.push(row_to_values(row).map_err(map_backend_error)?);
            }
            Ok(out)
        })
        .await
        .map_err(map_backend_error)?
    }

    /// Run migrations against this scenario's database (same entry style as [`Self::execute`]).
    pub async fn run_migrations(
        &self,
        runner: &(impl MigrationRunner + ?Sized),
    ) -> Result<(), ShakedownError> {
        let sqlite_bindings = self.sqlite_bindings()?;
        sqlite_bindings.run_migrations(runner).await
    }

    /// Connected [`sqlx::sqlite::SqlitePool`](sqlx::sqlite::SqlitePool) for this scenario (`sqlite` + `sqlx`).
    #[cfg(feature = "sqlx")]
    pub async fn sqlite_pool(&self) -> Result<sqlx::sqlite::SqlitePool, ShakedownError> {
        let sqlite_bindings = self.sqlite_bindings()?;
        sqlite_bindings.sqlite_pool().await
    }
}
