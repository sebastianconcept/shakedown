//! Outbox-related error type for row assertions, publication observation, and typed payload checks.
//!
//! Loaded as `outbox_error` from the crate root and re-exported as [`crate::OutboxError`].

use displaydoc::Display;
use thiserror::Error;

/// Outbox row assertions, publication observation on the bus, and typed payload checks.
///
/// Returned inside [`crate::error::ShakedownError::Outbox`]. Use [`OutboxError::JsonPayload`] (via [`serde_json::Error`])
/// when a structured parse failed so [`std::error::Error::source`] chains to the JSON error; use
/// [`OutboxError::PayloadInvalid`] for shape or domain messages without an underlying [`std::error::Error`].
#[derive(Debug, Display, Error)]
#[ignore_extra_doc_attributes]
pub enum OutboxError {
    /// No outbox storage binding on [`crate::lifecycle::Context`]. Outbox row helpers need exactly one of postgres, mariadb, mysql, sqlite, mongodb, or duckdb provisioned (see `Outbox` / `mvp-outbox` docs).
    ///
    /// Cause: `ctx.outbox()` (or similar) was used without registering and provisioning a supported storage resource.
    NoRelationalBindings,
    /// Outbox row not found for aggregate_id='{aggregate_id}', event_type='{event_type}', expected status='{expected_status}'
    ///
    /// Cause: no row matches the aggregate id and event type, or the row exists but not in the expected terminal status (timing, wrong id, or application never wrote the event).
    RowNotFound {
        aggregate_id: String,
        event_type: String,
        expected_status: String,
    },
    /// Outbox row in wrong state for aggregate_id='{aggregate_id}', event_type='{event_type}': expected '{expected}', got '{actual}'
    ///
    /// Cause: a row was found but its status column (or equivalent) does not match the assertion.
    WrongState {
        aggregate_id: String,
        event_type: String,
        expected: String,
        actual: String,
    },
    /// Outbox row has unknown status '{status}' for aggregate_id='{aggregate_id}', event_type='{event_type}'
    ///
    /// Cause: the stored status value is not one the harness recognises — often a schema or application bug.
    UnknownState {
        status: String,
        aggregate_id: String,
        event_type: String,
    },
    /// Outbox retry below threshold for aggregate_id='{aggregate_id}': attempts={attempts} min_attempts={min_attempts}
    ///
    /// Cause: a retry-count assertion expected at least `min_attempts` attempts but observed fewer (often eventual consistency or backoff).
    RetryBelowThreshold {
        aggregate_id: String,
        attempts: i32,
        min_attempts: i32,
    },
    /// [{transport}] Outbox publication not found for aggregate_id='{aggregate_id}' (channel='{channel}')
    ///
    /// Cause: no matching message was observed on the bus within the proof window (Kafka topic, Rabbit queue/exchange, etc.) — distinct from relational outbox row assertions.
    PublicationNotFound {
        transport: &'static str,
        channel: String,
        aggregate_id: String,
    },
    /// [{transport}] Outbox publication observation timed out for aggregate_id='{aggregate_id}' (channel='{channel}')
    ///
    /// Cause: the observation window expired before a matching publication appeared (slow pipeline, wrong channel, or aggregate id mismatch).
    PublicationTimeout {
        transport: &'static str,
        channel: String,
        aggregate_id: String,
    },
    /// Outbox payload deserialisation failed: {0}
    ///
    /// Cause: bytes from the outbox row could not be parsed as the expected type (schema drift or corruption).
    JsonPayload(#[from] serde_json::Error),
    /// Outbox payload deserialisation failed: {message}
    ///
    /// Cause: document or column shape did not match expectations (missing field, wrong BSON type).
    PayloadInvalid { message: String },
    /// Outbox payload assertion failed for aggregate_id='{aggregate_id}', event_type='{event_type}'
    ///
    /// Cause: the payload was deserialised but the scenario’s predicate rejected it.
    PayloadAssertionFailed {
        aggregate_id: String,
        event_type: String,
    },
}
