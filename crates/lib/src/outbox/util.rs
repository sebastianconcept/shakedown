//! Shared parsing, error mapping, and row-shape helpers for outbox backends.

use crate::error::{IntoInfrastructureError, ShakedownError};
use crate::OutboxError;

use super::OutboxRowState;

/// Row tuple returned by outbox `SELECT` helpers (relational backends + SQLite/DuckDB blocking path).
#[cfg(any(
    feature = "mariadb",
    feature = "mysql",
    feature = "sqlite",
    feature = "duckdb"
))]
pub(crate) type OutboxAssertRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    i32,
    String,
    Option<String>,
);

pub(crate) fn as_backend_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> ShakedownError {
    error.into_infrastructure_error().into()
}

pub(crate) fn parse_payload_json(payload_text: &str) -> Result<serde_json::Value, OutboxError> {
    serde_json::from_str(payload_text).map_err(OutboxError::from)
}

pub(crate) fn parse_outbox_row_state(
    status: &str,
    aggregate_id: &str,
    event_type: &str,
) -> Result<OutboxRowState, OutboxError> {
    match status {
        "pending" => Ok(OutboxRowState::Pending),
        "sent" => Ok(OutboxRowState::Sent),
        "failed" => Ok(OutboxRowState::Failed),
        _ => Err(OutboxError::UnknownState {
            status: status.to_string(),
            aggregate_id: aggregate_id.to_string(),
            event_type: event_type.to_string(),
        }),
    }
}

pub(crate) fn outbox_row_state_label(state: OutboxRowState) -> &'static str {
    match state {
        OutboxRowState::Pending => "pending",
        OutboxRowState::Sent => "sent",
        OutboxRowState::Failed => "failed",
    }
}

/// No row with `expected_status`: either missing entirely, or present under a different status.
pub(crate) fn outbox_expected_status_miss_error(
    aggregate_id: &str,
    event_type: &str,
    expected_status: &str,
    latest_row_status: Option<String>,
) -> OutboxError {
    match latest_row_status {
        Some(actual) => OutboxError::WrongState {
            aggregate_id: aggregate_id.to_string(),
            event_type: event_type.to_string(),
            expected: expected_status.to_string(),
            actual,
        },
        None => OutboxError::RowNotFound {
            aggregate_id: aggregate_id.to_string(),
            event_type: event_type.to_string(),
            expected_status: expected_status.to_string(),
        },
    }
}

pub(crate) fn row_not_found_any(aggregate_id: &str, event_type: &str) -> OutboxError {
    OutboxError::RowNotFound {
        aggregate_id: aggregate_id.to_string(),
        event_type: event_type.to_string(),
        expected_status: "any".to_string(),
    }
}
