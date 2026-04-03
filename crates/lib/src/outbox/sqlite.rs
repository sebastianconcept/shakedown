//! SQLite `outbox_event` DDL and row fetch helpers.

use crate::error::ShakedownError;

use super::util::{
    as_backend_error, outbox_expected_status_miss_error, parse_outbox_row_state,
    parse_payload_json, row_not_found_any, OutboxAssertRow,
};
use super::OutboxEntry;

const SQLITE_OUTBOX_FETCH_WITH_STATUS: &str = r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, created_at, sent_at
FROM outbox_event
WHERE aggregate_id = ?1 AND type = ?2 AND status = ?3
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

const SQLITE_OUTBOX_FETCH_LATEST: &str = r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, created_at, sent_at
FROM outbox_event
WHERE aggregate_id = ?1 AND type = ?2
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

const SQLITE_OUTBOX_SELECT_LATEST_STATUS: &str = r#"
SELECT status
FROM outbox_event
WHERE aggregate_id = ?1 AND type = ?2
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

const SQLITE_SETUP_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS outbox_event (
    id             TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    type           TEXT NOT NULL,
    payload        TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    attempts       INTEGER NOT NULL DEFAULT 0,
    next_retry_at  TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    sent_at        TEXT
);
"#;

pub(crate) async fn setup(database_url: &str) -> Result<(), ShakedownError> {
    let sqlite_url = database_url.to_string();
    tokio::task::spawn_blocking(move || {
        use rusqlite::OpenFlags;
        // SQLite does not support SELECT FOR UPDATE SKIP LOCKED.
        // Outbox claim-locking semantics are out of scope for this path; only row-state
        // assertions (pending/sent/failed/retry) are verified here.
        let connection = rusqlite::Connection::open_with_flags(
            sqlite_url.as_str(),
            OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        connection.execute_batch(SQLITE_SETUP_SQL)?;
        Ok::<(), rusqlite::Error>(())
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
    let sqlite_url = database_url.to_string();
    let aggregate_id_owned = aggregate_id.to_string();
    let event_type_owned = event_type.to_string();
    let expected_status_owned = expected_status.to_string();
    let maybe_row: Option<OutboxAssertRow> = tokio::task::spawn_blocking(move || {
        use rusqlite::OpenFlags;
        let connection = rusqlite::Connection::open_with_flags(
            sqlite_url.as_str(),
            OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        let mut statement = connection.prepare(SQLITE_OUTBOX_FETCH_WITH_STATUS)?;
        let mut rows =
            statement.query([aggregate_id_owned, event_type_owned, expected_status_owned])?;
        if let Some(row) = rows.next()? {
            Ok::<_, rusqlite::Error>(Some((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            )))
        } else {
            Ok::<_, rusqlite::Error>(None)
        }
    })
    .await
    .map_err(as_backend_error)?
    .map_err(as_backend_error)?;
    if let Some(row) = maybe_row {
        let payload = parse_payload_json(row.4.as_str())?;
        let status = parse_outbox_row_state(row.5.as_str(), row.2.as_str(), row.3.as_str())?;
        return Ok(OutboxEntry {
            id: row.0,
            aggregate_type: row.1,
            aggregate_id: row.2,
            event_type: row.3,
            payload,
            status,
            attempts: row.6,
            created_at: row.7,
            sent_at: row.8,
        });
    }
    let sqlite_url2 = database_url.to_string();
    let aggregate_id_owned = aggregate_id.to_string();
    let event_type_owned = event_type.to_string();
    let latest_status: Option<String> = tokio::task::spawn_blocking(move || {
        use rusqlite::OpenFlags;
        let connection = rusqlite::Connection::open_with_flags(
            sqlite_url2.as_str(),
            OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        let mut statement = connection.prepare(SQLITE_OUTBOX_SELECT_LATEST_STATUS)?;
        let mut rows = statement.query([aggregate_id_owned, event_type_owned])?;
        if let Some(row) = rows.next()? {
            Ok::<_, rusqlite::Error>(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    })
    .await
    .map_err(as_backend_error)?
    .map_err(as_backend_error)?;
    Err(
        outbox_expected_status_miss_error(aggregate_id, event_type, expected_status, latest_status)
            .into(),
    )
}

pub(crate) async fn fetch_latest_entry(
    database_url: &str,
    aggregate_id: &str,
    event_type: &str,
) -> Result<OutboxEntry, ShakedownError> {
    let sqlite_url = database_url.to_string();
    let aggregate_id_owned = aggregate_id.to_string();
    let event_type_owned = event_type.to_string();
    let maybe_row: Option<OutboxAssertRow> = tokio::task::spawn_blocking(move || {
        use rusqlite::OpenFlags;
        let connection = rusqlite::Connection::open_with_flags(
            sqlite_url.as_str(),
            OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        let mut statement = connection.prepare(SQLITE_OUTBOX_FETCH_LATEST)?;
        let mut rows = statement.query([aggregate_id_owned, event_type_owned])?;
        if let Some(row) = rows.next()? {
            Ok::<_, rusqlite::Error>(Some((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            )))
        } else {
            Ok::<_, rusqlite::Error>(None)
        }
    })
    .await
    .map_err(as_backend_error)?
    .map_err(as_backend_error)?;
    if let Some(row) = maybe_row {
        let payload = parse_payload_json(row.4.as_str())?;
        let status = parse_outbox_row_state(row.5.as_str(), row.2.as_str(), row.3.as_str())?;
        return Ok(OutboxEntry {
            id: row.0,
            aggregate_type: row.1,
            aggregate_id: row.2,
            event_type: row.3,
            payload,
            status,
            attempts: row.6,
            created_at: row.7,
            sent_at: row.8,
        });
    }
    Err(row_not_found_any(aggregate_id, event_type).into())
}
