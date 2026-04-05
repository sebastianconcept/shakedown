//! DuckDB `outbox_event` DDL and row fetch helpers.

use std::path::Path;

use crate::error::ShakedownError;

use super::util::{
    as_backend_error, outbox_expected_status_miss_error, parse_outbox_row_state,
    parse_payload_json, row_not_found_any, OutboxAssertRow,
};
use super::OutboxEntry;

const DUCKDB_SETUP_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS outbox_event (
    id             TEXT PRIMARY KEY DEFAULT (uuid()),
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    type           TEXT NOT NULL,
    payload        TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    attempts       INTEGER NOT NULL DEFAULT 0,
    next_retry_at  TEXT,
    created_at     TEXT NOT NULL DEFAULT (current_timestamp),
    sent_at        TEXT
);
"#;

const DUCKDB_OUTBOX_FETCH_WITH_STATUS: &str = r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, created_at, sent_at
FROM outbox_event
WHERE aggregate_id = ?1 AND type = ?2 AND status = ?3
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

const DUCKDB_OUTBOX_FETCH_LATEST: &str = r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, created_at, sent_at
FROM outbox_event
WHERE aggregate_id = ?1 AND type = ?2
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

const DUCKDB_OUTBOX_SELECT_LATEST_STATUS: &str = r#"
SELECT status
FROM outbox_event
WHERE aggregate_id = ?1 AND type = ?2
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

fn duckdb_read_outbox_assert_row(row: &duckdb::Row<'_>) -> Result<OutboxAssertRow, duckdb::Error> {
    let attempts: i64 = row.get(6)?;
    let attempts = i32::try_from(attempts)
        .map_err(|_| duckdb::Error::IntegralValueOutOfRange(6, i128::from(attempts)))?;
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        attempts,
        row.get(7)?,
        row.get::<_, Option<String>>(8)?,
    ))
}

pub(crate) async fn setup(database_url: &str) -> Result<(), ShakedownError> {
    let path = database_url.to_string();
    tokio::task::spawn_blocking(move || {
        // DuckDB does not support SELECT FOR UPDATE SKIP LOCKED; assertion-only path (see module docs).
        let connection = duckdb::Connection::open(Path::new(path.as_str()))?;
        connection.execute_batch(DUCKDB_SETUP_SQL)?;
        Ok::<(), duckdb::Error>(())
    })
    .await
    .map_err(as_backend_error)?
    .map_err(as_backend_error)?;
    Ok(())
}

pub(crate) async fn fetch_entry_with_status(
    database_url: &str,
    aggregate_id: &str,
    event_type: &str,
    expected_status: &str,
) -> Result<OutboxEntry, ShakedownError> {
    let database_url = database_url.to_string();
    let aggregate_id_string = aggregate_id.to_string();
    let event_type_string = event_type.to_string();
    let expected_status_string = expected_status.to_string();
    let result: Result<Result<OutboxEntry, ShakedownError>, ShakedownError> =
        tokio::task::spawn_blocking(move || {
            // DuckDB does not support SELECT FOR UPDATE SKIP LOCKED; assertion-only path (see module docs).
            let connection = duckdb::Connection::open(Path::new(database_url.as_str()))
                .map_err(as_backend_error)?;
            let mut statement = connection
                .prepare(DUCKDB_OUTBOX_FETCH_WITH_STATUS)
                .map_err(as_backend_error)?;
            let mut rows = statement
                .query(duckdb::params![
                    aggregate_id_string.as_str(),
                    event_type_string.as_str(),
                    expected_status_string.as_str(),
                ])
                .map_err(as_backend_error)?;
            if let Some(row) = rows.next().map_err(as_backend_error)? {
                let outbox_assert_row =
                    duckdb_read_outbox_assert_row(row).map_err(as_backend_error)?;
                let payload = parse_payload_json(outbox_assert_row.4.as_str())?;
                let status = parse_outbox_row_state(
                    outbox_assert_row.5.as_str(),
                    outbox_assert_row.2.as_str(),
                    outbox_assert_row.3.as_str(),
                )?;
                return Ok(Ok(OutboxEntry {
                    id: outbox_assert_row.0,
                    aggregate_type: outbox_assert_row.1,
                    aggregate_id: outbox_assert_row.2,
                    event_type: outbox_assert_row.3,
                    payload,
                    status,
                    attempts: outbox_assert_row.6,
                    created_at: outbox_assert_row.7,
                    sent_at: outbox_assert_row.8,
                }));
            }
            let mut latest_statement = connection
                .prepare(DUCKDB_OUTBOX_SELECT_LATEST_STATUS)
                .map_err(as_backend_error)?;
            let mut latest_rows = latest_statement
                .query(duckdb::params![
                    aggregate_id_string.as_str(),
                    event_type_string.as_str(),
                ])
                .map_err(as_backend_error)?;
            let latest_status = if let Some(row) = latest_rows.next().map_err(as_backend_error)? {
                let status: String = row.get(0).map_err(as_backend_error)?;
                Some(status)
            } else {
                None
            };
            Ok(Err(outbox_expected_status_miss_error(
                &aggregate_id_string,
                &event_type_string,
                &expected_status_string,
                latest_status,
            )
            .into()))
        })
        .await
        .map_err(as_backend_error)?;
    result?
}

pub(crate) async fn fetch_latest_entry(
    database_url: &str,
    aggregate_id: &str,
    event_type: &str,
) -> Result<OutboxEntry, ShakedownError> {
    let database_url = database_url.to_string();
    let aggregate_id_string = aggregate_id.to_string();
    let event_type_string = event_type.to_string();
    let result: Result<Result<OutboxEntry, ShakedownError>, ShakedownError> =
        tokio::task::spawn_blocking(move || {
            // DuckDB does not support SELECT FOR UPDATE SKIP LOCKED; assertion-only path (see module docs).
            let connection = duckdb::Connection::open(Path::new(database_url.as_str()))
                .map_err(as_backend_error)?;
            let mut statement = connection
                .prepare(DUCKDB_OUTBOX_FETCH_LATEST)
                .map_err(as_backend_error)?;
            let mut rows = statement
                .query(duckdb::params![
                    aggregate_id_string.as_str(),
                    event_type_string.as_str(),
                ])
                .map_err(as_backend_error)?;
            if let Some(row) = rows.next().map_err(as_backend_error)? {
                let outbox_assert_row =
                    duckdb_read_outbox_assert_row(row).map_err(as_backend_error)?;
                let payload = parse_payload_json(outbox_assert_row.4.as_str())?;
                let status = parse_outbox_row_state(
                    outbox_assert_row.5.as_str(),
                    outbox_assert_row.2.as_str(),
                    outbox_assert_row.3.as_str(),
                )?;
                return Ok(Ok(OutboxEntry {
                    id: outbox_assert_row.0,
                    aggregate_type: outbox_assert_row.1,
                    aggregate_id: outbox_assert_row.2,
                    event_type: outbox_assert_row.3,
                    payload,
                    status,
                    attempts: outbox_assert_row.6,
                    created_at: outbox_assert_row.7,
                    sent_at: outbox_assert_row.8,
                }));
            }
            Ok(Err(row_not_found_any(
                aggregate_id_string.as_str(),
                event_type_string.as_str(),
            )
            .into()))
        })
        .await
        .map_err(as_backend_error)?;
    result?
}
