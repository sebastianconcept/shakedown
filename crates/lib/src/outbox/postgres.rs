//! PostgreSQL `outbox_event` DDL and row fetch helpers.

use crate::error::ShakedownError;

use super::util::{
    as_backend_error, outbox_expected_status_miss_error, parse_outbox_row_state,
    parse_payload_json, row_not_found_any,
};
use super::OutboxEntry;

const POSTGRES_OUTBOX_FETCH_WITH_STATUS: &str = r#"
SELECT id::text, aggregate_type, aggregate_id, type, payload::text, status, attempts, created_at::text, sent_at::text
FROM outbox_event
WHERE aggregate_id = $1 AND type = $2 AND status = $3
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

const POSTGRES_OUTBOX_FETCH_LATEST: &str = r#"
SELECT id::text, aggregate_type, aggregate_id, type, payload::text, status, attempts, created_at::text, sent_at::text
FROM outbox_event
WHERE aggregate_id = $1 AND type = $2
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

const POSTGRES_OUTBOX_SELECT_LATEST_STATUS: &str = r#"
SELECT status::text
FROM outbox_event
WHERE aggregate_id = $1 AND type = $2
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

const POSTGRES_SETUP_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS outbox_event (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    type           TEXT NOT NULL,
    payload        JSONB NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    attempts       INT NOT NULL DEFAULT 0,
    next_retry_at  TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at        TIMESTAMPTZ
);
"#;

pub(crate) async fn setup(connection_url: &str) -> Result<(), ShakedownError> {
    let (client, connection) = tokio_postgres::connect(connection_url, tokio_postgres::NoTls)
        .await
        .map_err(as_backend_error)?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(POSTGRES_SETUP_SQL)
        .await
        .map_err(as_backend_error)?;
    Ok(())
}

pub(crate) async fn fetch_entry_with_status(
    connection_url: &str,
    aggregate_id: &str,
    event_type: &str,
    expected_status: &str,
) -> Result<OutboxEntry, ShakedownError> {
    let (client, connection) = tokio_postgres::connect(connection_url, tokio_postgres::NoTls)
        .await
        .map_err(as_backend_error)?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let maybe_row = client
        .query_opt(
            POSTGRES_OUTBOX_FETCH_WITH_STATUS,
            &[&aggregate_id, &event_type, &expected_status],
        )
        .await
        .map_err(as_backend_error)?;
    if let Some(row) = maybe_row {
        let payload_text: String = row.get(4);
        let payload = parse_payload_json(payload_text.as_str())?;
        let aggregate_id_value: String = row.get(2);
        let event_type_value: String = row.get(3);
        let status_text: String = row.get(5);
        return Ok(OutboxEntry {
            id: row.get(0),
            aggregate_type: row.get(1),
            aggregate_id: aggregate_id_value.clone(),
            event_type: event_type_value.clone(),
            payload,
            status: parse_outbox_row_state(
                status_text.as_str(),
                aggregate_id_value.as_str(),
                event_type_value.as_str(),
            )?,
            attempts: row.get(6),
            created_at: row.get(7),
            sent_at: row.get(8),
        });
    }
    let latest_status_row = client
        .query_opt(
            POSTGRES_OUTBOX_SELECT_LATEST_STATUS,
            &[&aggregate_id, &event_type],
        )
        .await
        .map_err(as_backend_error)?;
    let latest = latest_status_row.map(|row| row.get::<_, String>(0));
    Err(outbox_expected_status_miss_error(aggregate_id, event_type, expected_status, latest).into())
}

pub(crate) async fn fetch_latest_entry(
    connection_url: &str,
    aggregate_id: &str,
    event_type: &str,
) -> Result<OutboxEntry, ShakedownError> {
    let (client, connection) = tokio_postgres::connect(connection_url, tokio_postgres::NoTls)
        .await
        .map_err(as_backend_error)?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let maybe_row = client
        .query_opt(POSTGRES_OUTBOX_FETCH_LATEST, &[&aggregate_id, &event_type])
        .await
        .map_err(as_backend_error)?;
    if let Some(row) = maybe_row {
        let payload_text: String = row.get(4);
        let payload = parse_payload_json(payload_text.as_str())?;
        let aggregate_id_value: String = row.get(2);
        let event_type_value: String = row.get(3);
        let status_text: String = row.get(5);
        return Ok(OutboxEntry {
            id: row.get(0),
            aggregate_type: row.get(1),
            aggregate_id: aggregate_id_value.clone(),
            event_type: event_type_value.clone(),
            payload,
            status: parse_outbox_row_state(
                status_text.as_str(),
                aggregate_id_value.as_str(),
                event_type_value.as_str(),
            )?,
            attempts: row.get(6),
            created_at: row.get(7),
            sent_at: row.get(8),
        });
    }
    Err(row_not_found_any(aggregate_id, event_type).into())
}
