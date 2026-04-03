//! SQLite typed helpers — [`SqliteHelpers`] for scenario-scoped SQL via [`rusqlite`] on a blocking pool.
//!
//! Queries run inside [`tokio::task::spawn_blocking`] so the public API stays `async` while
//! [`rusqlite`] stays synchronous ([`crate::resources::SqliteBindings`] use a shared in-memory URI).

use rusqlite::types::Value;
use rusqlite::{Connection, OpenFlags, Params};

use crate::error::{AssertionError, InfrastructureError, ScenarioError, ShakedownError};
use crate::helper_prelude::map_backend_error;
use crate::lifecycle::Context;
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
}

impl<'ctx> SqliteHelpers<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    fn sqlite_bindings(&self) -> Result<&SqliteBindings, ShakedownError> {
        self.context.sqlite_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "sqlite bindings".to_string(),
            ))
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
}
