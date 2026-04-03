//! MariaDB / MySQL shared `outbox_event` DDL and row fetch helpers (mysql_async).

use crate::error::ShakedownError;

use super::util::{
    as_backend_error, outbox_expected_status_miss_error, parse_outbox_row_state,
    parse_payload_json, row_not_found_any, OutboxAssertRow,
};
use super::OutboxEntry;

const MYSQL_FAMILY_OUTBOX_FETCH_WITH_STATUS: &str = r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, CAST(created_at AS CHAR), CAST(sent_at AS CHAR)
FROM outbox_event
WHERE aggregate_id = ? AND type = ? AND status = ?
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

const MYSQL_FAMILY_OUTBOX_FETCH_LATEST: &str = r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, CAST(created_at AS CHAR), CAST(sent_at AS CHAR)
FROM outbox_event
WHERE aggregate_id = ? AND type = ?
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

const MYSQL_FAMILY_OUTBOX_SELECT_LATEST_STATUS: &str = r#"
SELECT status
FROM outbox_event
WHERE aggregate_id = ? AND type = ?
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

const MYSQL_FAMILY_SETUP_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS outbox_event (
    id             VARCHAR(36) PRIMARY KEY DEFAULT (UUID()),
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    type           TEXT NOT NULL,
    payload        JSON NOT NULL,
    status         VARCHAR(20) NOT NULL DEFAULT 'pending',
    attempts       INT NOT NULL DEFAULT 0,
    next_retry_at  DATETIME(6),
    created_at     DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    sent_at        DATETIME(6)
);
"#;

pub(crate) async fn setup(connection_url: &str) -> Result<(), ShakedownError> {
    let options = mysql_async::Opts::from_url(connection_url).map_err(as_backend_error)?;
    let pool = mysql_async::Pool::new(options);
    let mut connection = pool.get_conn().await.map_err(as_backend_error)?;
    use mysql_async::prelude::Queryable;
    connection
        .query_drop(MYSQL_FAMILY_SETUP_SQL)
        .await
        .map_err(as_backend_error)?;
    connection.disconnect().await.map_err(as_backend_error)?;
    Ok(())
}

pub(crate) async fn fetch_entry_with_status(
    connection_url: &str,
    aggregate_id: &str,
    event_type: &str,
    expected_status: &str,
) -> Result<OutboxEntry, ShakedownError> {
    use mysql_async::prelude::Queryable;
    let options = mysql_async::Opts::from_url(connection_url).map_err(as_backend_error)?;
    let pool = mysql_async::Pool::new(options);
    let mut connection = pool.get_conn().await.map_err(as_backend_error)?;
    let maybe_row: Option<OutboxAssertRow> = connection
        .exec_first(
            MYSQL_FAMILY_OUTBOX_FETCH_WITH_STATUS,
            (aggregate_id, event_type, expected_status),
        )
        .await
        .map_err(as_backend_error)?;
    if let Some(row) = maybe_row {
        connection.disconnect().await.map_err(as_backend_error)?;
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
    let latest_status: Option<String> = connection
        .exec_first(
            MYSQL_FAMILY_OUTBOX_SELECT_LATEST_STATUS,
            (aggregate_id, event_type),
        )
        .await
        .map_err(as_backend_error)?;
    connection.disconnect().await.map_err(as_backend_error)?;
    Err(
        outbox_expected_status_miss_error(aggregate_id, event_type, expected_status, latest_status)
            .into(),
    )
}

pub(crate) async fn fetch_latest_entry(
    connection_url: &str,
    aggregate_id: &str,
    event_type: &str,
) -> Result<OutboxEntry, ShakedownError> {
    use mysql_async::prelude::Queryable;
    let options = mysql_async::Opts::from_url(connection_url).map_err(as_backend_error)?;
    let pool = mysql_async::Pool::new(options);
    let mut connection = pool.get_conn().await.map_err(as_backend_error)?;
    let maybe_row: Option<OutboxAssertRow> = connection
        .exec_first(MYSQL_FAMILY_OUTBOX_FETCH_LATEST, (aggregate_id, event_type))
        .await
        .map_err(as_backend_error)?;
    connection.disconnect().await.map_err(as_backend_error)?;
    if let Some(row) = maybe_row {
        let payload = parse_payload_json(row.4.as_str())?;
        return Ok(OutboxEntry {
            id: row.0,
            aggregate_type: row.1,
            aggregate_id: row.2.clone(),
            event_type: row.3.clone(),
            payload,
            status: parse_outbox_row_state(row.5.as_str(), row.2.as_str(), row.3.as_str())?,
            attempts: row.6,
            created_at: row.7,
            sent_at: row.8,
        });
    }
    Err(row_not_found_any(aggregate_id, event_type).into())
}
