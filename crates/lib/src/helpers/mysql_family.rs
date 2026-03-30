//! Shared [`mysql_async`] helpers for MariaDB and Oracle MySQL facades.

use mysql_async::prelude::Queryable;
use mysql_async::{Params, Row};

use crate::error::{AssertionError, InfrastructureError, ScenarioError, ShakedownError};

fn map_backend_error(error: impl std::error::Error + Send + Sync + 'static) -> ShakedownError {
    ShakedownError::Infrastructure(InfrastructureError::Backend(Box::new(error)))
}

/// Run `INSERT` / `UPDATE` / `DELETE` / DDL, or `SELECT`; returns affected rows or selected row count.
pub(crate) async fn execute(
    connection_url: &str,
    query: &str,
    params: impl Into<Params> + Send,
) -> Result<u64, ShakedownError> {
    let options = mysql_async::Opts::from_url(connection_url).map_err(map_backend_error)?;
    let pool = mysql_async::Pool::new(options);
    let mut connection = pool.get_conn().await.map_err(map_backend_error)?;
    let result = connection
        .exec_iter(query, params)
        .await
        .map_err(map_backend_error)?;
    let count = if result.is_empty() {
        let affected_rows = result.affected_rows();
        result.drop_result().await.map_err(map_backend_error)?;
        affected_rows
    } else {
        let rows = result
            .collect_and_drop::<Row>()
            .await
            .map_err(map_backend_error)?;
        rows.len() as u64
    };
    connection.disconnect().await.map_err(map_backend_error)?;
    pool.disconnect().await.map_err(map_backend_error)?;
    Ok(count)
}

pub(crate) async fn assert_row_exists(
    connection_url: &str,
    query: &str,
    params: impl Into<Params> + Send,
) -> Result<(), ShakedownError> {
    let options = mysql_async::Opts::from_url(connection_url).map_err(map_backend_error)?;
    let pool = mysql_async::Pool::new(options);
    let mut connection = pool.get_conn().await.map_err(map_backend_error)?;
    let maybe_row: Option<Row> = connection
        .exec_first(query, params)
        .await
        .map_err(map_backend_error)?;
    connection.disconnect().await.map_err(map_backend_error)?;
    pool.disconnect().await.map_err(map_backend_error)?;
    if maybe_row.is_none() {
        return Err(ShakedownError::Scenario(ScenarioError::Assertion(
            AssertionError::Unspecified {
                detail: "assert_row_exists: query returned no rows".to_string(),
            },
        )));
    }
    Ok(())
}

pub(crate) async fn assert_row_count(
    connection_url: &str,
    query: &str,
    params: impl Into<Params> + Send,
    expected: usize,
) -> Result<(), ShakedownError> {
    let options = mysql_async::Opts::from_url(connection_url).map_err(map_backend_error)?;
    let pool = mysql_async::Pool::new(options);
    let mut connection = pool.get_conn().await.map_err(map_backend_error)?;
    let result = connection
        .exec_iter(query, params)
        .await
        .map_err(map_backend_error)?;
    let rows = result
        .collect_and_drop::<Row>()
        .await
        .map_err(map_backend_error)?;
    let actual = rows.len();
    connection.disconnect().await.map_err(map_backend_error)?;
    pool.disconnect().await.map_err(map_backend_error)?;
    if actual != expected {
        return Err(ShakedownError::Scenario(ScenarioError::Assertion(
            AssertionError::Unspecified {
                detail: format!("assert_row_count: expected {expected} rows, got {actual}"),
            },
        )));
    }
    Ok(())
}

pub(crate) async fn query_one(
    connection_url: &str,
    query: &str,
    params: impl Into<Params> + Send,
) -> Result<Row, ShakedownError> {
    let options = mysql_async::Opts::from_url(connection_url).map_err(map_backend_error)?;
    let pool = mysql_async::Pool::new(options);
    let mut connection = pool.get_conn().await.map_err(map_backend_error)?;
    let result = connection
        .exec_iter(query, params)
        .await
        .map_err(map_backend_error)?;
    let rows = result
        .collect_and_drop::<Row>()
        .await
        .map_err(map_backend_error)?;
    connection.disconnect().await.map_err(map_backend_error)?;
    pool.disconnect().await.map_err(map_backend_error)?;
    match rows.len() {
        0 => Err(ShakedownError::Scenario(ScenarioError::Assertion(
            AssertionError::Unspecified {
                detail: "query_one: expected exactly one row, got zero".to_string(),
            },
        ))),
        1 => Ok(rows.into_iter().next().expect("length checked")),
        n => Err(ShakedownError::Scenario(ScenarioError::Assertion(
            AssertionError::Unspecified {
                detail: format!("query_one: expected exactly one row, got {n}"),
            },
        ))),
    }
}

pub(crate) async fn query_all(
    connection_url: &str,
    query: &str,
    params: impl Into<Params> + Send,
) -> Result<Vec<Row>, ShakedownError> {
    let options = mysql_async::Opts::from_url(connection_url).map_err(map_backend_error)?;
    let pool = mysql_async::Pool::new(options);
    let mut connection = pool.get_conn().await.map_err(map_backend_error)?;
    let result = connection
        .exec_iter(query, params)
        .await
        .map_err(map_backend_error)?;
    let rows = result
        .collect_and_drop::<Row>()
        .await
        .map_err(map_backend_error)?;
    connection.disconnect().await.map_err(map_backend_error)?;
    pool.disconnect().await.map_err(map_backend_error)?;
    Ok(rows)
}
