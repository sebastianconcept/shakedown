//! Outbox setup helpers for scenario databases.
//!
//! This module creates the canonical `outbox_event` table in the active relational
//! binding attached to [`crate::lifecycle::Context`].

use crate::error::{InfrastructureError, ShakedownError};
use crate::lifecycle::Context;

/// Row state for records in the canonical `outbox_event` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxRowState {
    /// Event is waiting for publisher processing.
    Pending,
    /// Event has been published successfully.
    Sent,
    /// Event publishing failed.
    Failed,
}

/// One `outbox_event` row mapped to a stable Rust shape for assertions.
#[derive(Debug, Clone)]
pub struct OutboxEntry {
    /// Primary key of the event.
    pub id: String,
    /// Domain aggregate type (for example `order`).
    pub aggregate_type: String,
    /// Domain aggregate identifier.
    pub aggregate_id: String,
    /// Event type (`type` SQL column).
    pub event_type: String,
    /// Raw event payload as JSON.
    pub payload: serde_json::Value,
    /// Current row state.
    pub status: OutboxRowState,
    /// Publish attempts count.
    pub attempts: i32,
    /// Engine-normalized creation timestamp.
    pub created_at: String,
    /// Engine-normalized sent timestamp, when available.
    pub sent_at: Option<String>,
}

/// Row tuple returned by outbox `SELECT` helpers (relational backends + SQLite blocking path).
#[cfg(any(feature = "mariadb", feature = "mysql", feature = "sqlite"))]
type OutboxAssertRow = (
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

// --- Outbox row queries (one definition per engine family; WHERE differs by use case) ---

#[cfg(feature = "postgres")]
const POSTGRES_OUTBOX_FETCH_WITH_STATUS: &str = r#"
SELECT id::text, aggregate_type, aggregate_id, type, payload::text, status, attempts, created_at::text, sent_at::text
FROM outbox_event
WHERE aggregate_id = $1 AND type = $2 AND status = $3
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

#[cfg(feature = "postgres")]
const POSTGRES_OUTBOX_FETCH_LATEST: &str = r#"
SELECT id::text, aggregate_type, aggregate_id, type, payload::text, status, attempts, created_at::text, sent_at::text
FROM outbox_event
WHERE aggregate_id = $1 AND type = $2
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

#[cfg(feature = "postgres")]
const POSTGRES_OUTBOX_SELECT_LATEST_STATUS: &str = r#"
SELECT status::text
FROM outbox_event
WHERE aggregate_id = $1 AND type = $2
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

#[cfg(any(feature = "mariadb", feature = "mysql"))]
const MYSQL_FAMILY_OUTBOX_FETCH_WITH_STATUS: &str = r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, CAST(created_at AS CHAR), CAST(sent_at AS CHAR)
FROM outbox_event
WHERE aggregate_id = ? AND type = ? AND status = ?
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

#[cfg(any(feature = "mariadb", feature = "mysql"))]
const MYSQL_FAMILY_OUTBOX_FETCH_LATEST: &str = r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, CAST(created_at AS CHAR), CAST(sent_at AS CHAR)
FROM outbox_event
WHERE aggregate_id = ? AND type = ?
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

#[cfg(any(feature = "mariadb", feature = "mysql"))]
const MYSQL_FAMILY_OUTBOX_SELECT_LATEST_STATUS: &str = r#"
SELECT status
FROM outbox_event
WHERE aggregate_id = ? AND type = ?
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

#[cfg(feature = "sqlite")]
const SQLITE_OUTBOX_FETCH_WITH_STATUS: &str = r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, created_at, sent_at
FROM outbox_event
WHERE aggregate_id = ?1 AND type = ?2 AND status = ?3
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

#[cfg(feature = "sqlite")]
const SQLITE_OUTBOX_FETCH_LATEST: &str = r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, created_at, sent_at
FROM outbox_event
WHERE aggregate_id = ?1 AND type = ?2
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

#[cfg(feature = "sqlite")]
const SQLITE_OUTBOX_SELECT_LATEST_STATUS: &str = r#"
SELECT status
FROM outbox_event
WHERE aggregate_id = ?1 AND type = ?2
ORDER BY created_at DESC, id DESC
LIMIT 1
"#;

#[cfg(feature = "postgres")]
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

#[cfg(any(feature = "mariadb", feature = "mysql"))]
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

#[cfg(feature = "sqlite")]
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

/// Feature-gated outbox helper bound to one scenario context.
pub struct Outbox<'ctx> {
    context: &'ctx Context,
}

/// Chainable assertion builder for one outbox row.
#[derive(Debug)]
pub struct OutboxAssertBuilder {
    entry: OutboxEntry,
}

impl OutboxAssertBuilder {
    /// Return the asserted outbox entry for additional ad-hoc checks.
    pub fn entry(&self) -> &OutboxEntry {
        &self.entry
    }

    /// Deserialize payload as `T` and apply a user assertion predicate.
    pub async fn with_payload<T: serde::de::DeserializeOwned>(
        self,
        payload_assertion: impl FnOnce(&T) -> bool,
    ) -> Result<(), ShakedownError> {
        let payload = serde_json::from_value::<T>(self.entry.payload.clone())
            .map_err(|error| ShakedownError::PayloadDeserialisation(error.to_string()))?;
        if !payload_assertion(&payload) {
            return Err(ShakedownError::PayloadAssertionFailed {
                aggregate_id: self.entry.aggregate_id,
                event_type: self.entry.event_type,
            });
        }
        Ok(())
    }
}

impl<'ctx> Outbox<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    /// Create the canonical `outbox_event` table in the active scenario database.
    ///
    /// Idempotent by design (`CREATE TABLE IF NOT EXISTS`).
    pub async fn setup(&self) -> Result<(), ShakedownError> {
        #[cfg(feature = "postgres")]
        if let Some(postgres_bindings) = self.context.postgres_bindings() {
            let (client, connection) = tokio_postgres::connect(
                postgres_bindings.connection_url.as_str(),
                tokio_postgres::NoTls,
            )
            .await
            .map_err(as_backend_error)?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .batch_execute(POSTGRES_SETUP_SQL)
                .await
                .map_err(as_backend_error)?;
            return Ok(());
        }

        #[cfg(feature = "mariadb")]
        if let Some(mariadb_bindings) = self.context.mariadb_bindings() {
            let options = mysql_async::Opts::from_url(mariadb_bindings.connection_url.as_str())
                .map_err(as_backend_error)?;
            let pool = mysql_async::Pool::new(options);
            let mut connection = pool.get_conn().await.map_err(as_backend_error)?;
            use mysql_async::prelude::Queryable;
            connection
                .query_drop(MYSQL_FAMILY_SETUP_SQL)
                .await
                .map_err(as_backend_error)?;
            connection.disconnect().await.map_err(as_backend_error)?;
            return Ok(());
        }

        #[cfg(feature = "mysql")]
        if let Some(mysql_bindings) = self.context.mysql_bindings() {
            let options = mysql_async::Opts::from_url(mysql_bindings.connection_url.as_str())
                .map_err(as_backend_error)?;
            let pool = mysql_async::Pool::new(options);
            let mut connection = pool.get_conn().await.map_err(as_backend_error)?;
            use mysql_async::prelude::Queryable;
            connection
                .query_drop(MYSQL_FAMILY_SETUP_SQL)
                .await
                .map_err(as_backend_error)?;
            connection.disconnect().await.map_err(as_backend_error)?;
            return Ok(());
        }

        #[cfg(feature = "sqlite")]
        if let Some(sqlite_bindings) = self.context.sqlite_bindings() {
            let sqlite_url = sqlite_bindings.database_url.clone();
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
            return Ok(());
        }

        Err(ShakedownError::NoRelationalBindings)
    }

    /// Assert a row with matching identifiers exists in pending state.
    pub async fn assert_pending(
        &self,
        aggregate_id: &str,
        event_type: &str,
    ) -> Result<OutboxAssertBuilder, ShakedownError> {
        self.assert_state(aggregate_id, event_type, OutboxRowState::Pending)
            .await
    }

    /// Assert a row with matching identifiers exists in sent state.
    pub async fn assert_sent(
        &self,
        aggregate_id: &str,
        event_type: &str,
    ) -> Result<OutboxAssertBuilder, ShakedownError> {
        self.assert_state(aggregate_id, event_type, OutboxRowState::Sent)
            .await
    }

    /// Assert a row with matching identifiers exists in failed state.
    pub async fn assert_failed(
        &self,
        aggregate_id: &str,
        event_type: &str,
    ) -> Result<OutboxAssertBuilder, ShakedownError> {
        self.assert_state(aggregate_id, event_type, OutboxRowState::Failed)
            .await
    }

    /// Assert a row has retry attempts at or above a minimum.
    pub async fn assert_retry(
        &self,
        aggregate_id: &str,
        event_type: &str,
        min_attempts: i32,
    ) -> Result<(), ShakedownError> {
        let entry = self.fetch_latest_entry(aggregate_id, event_type).await?;
        if entry.attempts < min_attempts {
            return Err(ShakedownError::OutboxRetryBelowThreshold {
                aggregate_id: aggregate_id.to_string(),
                attempts: entry.attempts,
                min_attempts,
            });
        }
        Ok(())
    }

    /// Assert a Kafka record for `aggregate_id` exists in the scenario-namespaced topic.
    ///
    /// The caller provides the base topic name; this helper applies the scenario topic prefix.
    #[cfg(feature = "kafka")]
    pub async fn assert_kafka_published(
        &self,
        base_topic: &str,
        aggregate_id: &str,
    ) -> Result<(), ShakedownError> {
        use crate::publication::{
            KafkaPublicationVerifier, PublicationObserveSpec, PublicationVerifier,
        };

        let verifier = KafkaPublicationVerifier;
        let spec = PublicationObserveSpec {
            aggregate_id: aggregate_id.to_string(),
            logical_channel: base_topic.to_string(),
            timeout: None,
        };
        PublicationVerifier::assert_observed(&verifier, self.context, &spec, &()).await
    }

    /// Assert an AMQP message for `aggregate_id` was observed on the scenario queue (Option A topology).
    ///
    /// `base_exchange` and `base_queue` are logical names; [`crate::resources::RabbitBindings::namespaced_name`]
    /// is applied for the broker. The verifier consumes from the namespaced queue; declare that queue and bind
    /// it to your exchange in seed **before** publishing. `routing_key` for the assertion is `aggregate_id`
    /// (override by calling [`crate::publication::PublicationVerifier::assert_observed`] with custom
    /// [`crate::publication::RabbitMqPublicationVerifierOptions`]).
    #[cfg(feature = "rabbitmq")]
    pub async fn assert_rabbit_published(
        &self,
        base_exchange: &str,
        base_queue: &str,
        aggregate_id: &str,
    ) -> Result<(), ShakedownError> {
        use crate::publication::{
            PublicationObserveSpec, PublicationVerifier, RabbitMqPublicationVerifier,
            RabbitMqPublicationVerifierOptions,
        };

        let rabbit_bindings = self.context.rabbit_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "rabbitmq bindings".to_string(),
            ))
        })?;

        let verifier = RabbitMqPublicationVerifier;
        let spec = PublicationObserveSpec {
            aggregate_id: aggregate_id.to_string(),
            logical_channel: base_queue.to_string(),
            timeout: None,
        };
        let options = RabbitMqPublicationVerifierOptions {
            exchange: rabbit_bindings.namespaced_name(base_exchange),
            routing_key: aggregate_id.to_string(),
        };
        PublicationVerifier::assert_observed(&verifier, self.context, &spec, &options).await
    }

    async fn assert_state(
        &self,
        aggregate_id: &str,
        event_type: &str,
        expected_state: OutboxRowState,
    ) -> Result<OutboxAssertBuilder, ShakedownError> {
        let entry = self
            .fetch_entry_with_status(aggregate_id, event_type, expected_state)
            .await?;
        Ok(OutboxAssertBuilder { entry })
    }

    async fn fetch_entry_with_status(
        &self,
        aggregate_id: &str,
        event_type: &str,
        expected_state: OutboxRowState,
    ) -> Result<OutboxEntry, ShakedownError> {
        let expected_status = outbox_row_state_label(expected_state);
        #[cfg(feature = "postgres")]
        if let Some(postgres_bindings) = self.context.postgres_bindings() {
            let (client, connection) = tokio_postgres::connect(
                postgres_bindings.connection_url.as_str(),
                tokio_postgres::NoTls,
            )
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
            return Err(outbox_expected_status_miss_error(
                aggregate_id,
                event_type,
                expected_status,
                latest,
            ));
        }

        #[cfg(feature = "mariadb")]
        if let Some(mariadb_bindings) = self.context.mariadb_bindings() {
            use mysql_async::prelude::Queryable;
            let options = mysql_async::Opts::from_url(mariadb_bindings.connection_url.as_str())
                .map_err(as_backend_error)?;
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
                let status =
                    parse_outbox_row_state(row.5.as_str(), row.2.as_str(), row.3.as_str())?;
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
            return Err(outbox_expected_status_miss_error(
                aggregate_id,
                event_type,
                expected_status,
                latest_status,
            ));
        }

        #[cfg(feature = "mysql")]
        if let Some(mysql_bindings) = self.context.mysql_bindings() {
            use mysql_async::prelude::Queryable;
            let options = mysql_async::Opts::from_url(mysql_bindings.connection_url.as_str())
                .map_err(as_backend_error)?;
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
                let status =
                    parse_outbox_row_state(row.5.as_str(), row.2.as_str(), row.3.as_str())?;
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
            return Err(outbox_expected_status_miss_error(
                aggregate_id,
                event_type,
                expected_status,
                latest_status,
            ));
        }

        #[cfg(feature = "sqlite")]
        if let Some(sqlite_bindings) = self.context.sqlite_bindings() {
            let sqlite_url = sqlite_bindings.database_url.clone();
            let aggregate_id_value = aggregate_id.to_string();
            let event_type_value = event_type.to_string();
            let maybe_row: Option<OutboxAssertRow> = tokio::task::spawn_blocking(move || {
                use rusqlite::OpenFlags;
                let connection = rusqlite::Connection::open_with_flags(
                    sqlite_url.as_str(),
                    OpenFlags::SQLITE_OPEN_URI
                        | OpenFlags::SQLITE_OPEN_READ_WRITE
                        | OpenFlags::SQLITE_OPEN_CREATE,
                )?;
                let mut statement = connection.prepare(SQLITE_OUTBOX_FETCH_WITH_STATUS)?;
                let mut rows = statement.query([
                    aggregate_id_value,
                    event_type_value,
                    expected_status.to_string(),
                ])?;
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
                let status =
                    parse_outbox_row_state(row.5.as_str(), row.2.as_str(), row.3.as_str())?;
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
            let sqlite_url2 = sqlite_bindings.database_url.clone();
            let aggregate_id_latest = aggregate_id.to_string();
            let event_type_latest = event_type.to_string();
            let latest_status: Option<String> = tokio::task::spawn_blocking(move || {
                use rusqlite::OpenFlags;
                let connection = rusqlite::Connection::open_with_flags(
                    sqlite_url2.as_str(),
                    OpenFlags::SQLITE_OPEN_URI
                        | OpenFlags::SQLITE_OPEN_READ_WRITE
                        | OpenFlags::SQLITE_OPEN_CREATE,
                )?;
                let mut statement = connection.prepare(SQLITE_OUTBOX_SELECT_LATEST_STATUS)?;
                let mut rows = statement.query([aggregate_id_latest, event_type_latest])?;
                if let Some(row) = rows.next()? {
                    Ok::<_, rusqlite::Error>(Some(row.get(0)?))
                } else {
                    Ok(None)
                }
            })
            .await
            .map_err(as_backend_error)?
            .map_err(as_backend_error)?;
            return Err(outbox_expected_status_miss_error(
                aggregate_id,
                event_type,
                expected_status,
                latest_status,
            ));
        }

        Err(ShakedownError::NoRelationalBindings)
    }

    async fn fetch_latest_entry(
        &self,
        aggregate_id: &str,
        event_type: &str,
    ) -> Result<OutboxEntry, ShakedownError> {
        #[cfg(feature = "postgres")]
        if let Some(postgres_bindings) = self.context.postgres_bindings() {
            let (client, connection) = tokio_postgres::connect(
                postgres_bindings.connection_url.as_str(),
                tokio_postgres::NoTls,
            )
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
            return Err(ShakedownError::OutboxRowNotFound {
                aggregate_id: aggregate_id.to_string(),
                event_type: event_type.to_string(),
                expected_status: "any".to_string(),
            });
        }

        #[cfg(feature = "mariadb")]
        if let Some(mariadb_bindings) = self.context.mariadb_bindings() {
            use mysql_async::prelude::Queryable;
            let options = mysql_async::Opts::from_url(mariadb_bindings.connection_url.as_str())
                .map_err(as_backend_error)?;
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
            return Err(ShakedownError::OutboxRowNotFound {
                aggregate_id: aggregate_id.to_string(),
                event_type: event_type.to_string(),
                expected_status: "any".to_string(),
            });
        }

        #[cfg(feature = "mysql")]
        if let Some(mysql_bindings) = self.context.mysql_bindings() {
            use mysql_async::prelude::Queryable;
            let options = mysql_async::Opts::from_url(mysql_bindings.connection_url.as_str())
                .map_err(as_backend_error)?;
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
            return Err(ShakedownError::OutboxRowNotFound {
                aggregate_id: aggregate_id.to_string(),
                event_type: event_type.to_string(),
                expected_status: "any".to_string(),
            });
        }

        #[cfg(feature = "sqlite")]
        if let Some(sqlite_bindings) = self.context.sqlite_bindings() {
            let sqlite_url = sqlite_bindings.database_url.clone();
            let aggregate_id_value = aggregate_id.to_string();
            let event_type_value = event_type.to_string();
            let maybe_row: Option<OutboxAssertRow> = tokio::task::spawn_blocking(move || {
                use rusqlite::OpenFlags;
                let connection = rusqlite::Connection::open_with_flags(
                    sqlite_url.as_str(),
                    OpenFlags::SQLITE_OPEN_URI
                        | OpenFlags::SQLITE_OPEN_READ_WRITE
                        | OpenFlags::SQLITE_OPEN_CREATE,
                )?;
                let mut statement = connection.prepare(SQLITE_OUTBOX_FETCH_LATEST)?;
                let mut rows = statement.query([aggregate_id_value, event_type_value])?;
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
                let status =
                    parse_outbox_row_state(row.5.as_str(), row.2.as_str(), row.3.as_str())?;
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
            return Err(ShakedownError::OutboxRowNotFound {
                aggregate_id: aggregate_id.to_string(),
                event_type: event_type.to_string(),
                expected_status: "any".to_string(),
            });
        }

        Err(ShakedownError::NoRelationalBindings)
    }
}

fn as_backend_error(error: impl std::error::Error + Send + Sync + 'static) -> ShakedownError {
    ShakedownError::Infrastructure(InfrastructureError::Backend(Box::new(error)))
}

fn parse_payload_json(payload_text: &str) -> Result<serde_json::Value, ShakedownError> {
    serde_json::from_str(payload_text)
        .map_err(|error| ShakedownError::PayloadDeserialisation(error.to_string()))
}

fn parse_outbox_row_state(
    status: &str,
    aggregate_id: &str,
    event_type: &str,
) -> Result<OutboxRowState, ShakedownError> {
    match status {
        "pending" => Ok(OutboxRowState::Pending),
        "sent" => Ok(OutboxRowState::Sent),
        "failed" => Ok(OutboxRowState::Failed),
        _ => Err(ShakedownError::OutboxUnknownState {
            status: status.to_string(),
            aggregate_id: aggregate_id.to_string(),
            event_type: event_type.to_string(),
        }),
    }
}

fn outbox_row_state_label(state: OutboxRowState) -> &'static str {
    match state {
        OutboxRowState::Pending => "pending",
        OutboxRowState::Sent => "sent",
        OutboxRowState::Failed => "failed",
    }
}

/// No row with `expected_status`: either missing entirely, or present under a different status.
fn outbox_expected_status_miss_error(
    aggregate_id: &str,
    event_type: &str,
    expected_status: &str,
    latest_row_status: Option<String>,
) -> ShakedownError {
    match latest_row_status {
        Some(actual) => ShakedownError::OutboxWrongState {
            aggregate_id: aggregate_id.to_string(),
            event_type: event_type.to_string(),
            expected: expected_status.to_string(),
            actual,
        },
        None => ShakedownError::OutboxRowNotFound {
            aggregate_id: aggregate_id.to_string(),
            event_type: event_type.to_string(),
            expected_status: expected_status.to_string(),
        },
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::{Outbox, OutboxEntry, OutboxRowState};
    use crate::error::ShakedownError;
    use crate::lifecycle::Context;
    use crate::resources::{BuiltinResource, SqliteResource};
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct OrderPlaced {
        total: f64,
    }

    #[tokio::test]
    async fn setup_is_idempotent_for_sqlite() {
        let mut context = Context::new();
        context.register_builtin(BuiltinResource::Sqlite(SqliteResource::new()));
        let mut event_log = crate::EventLog::new();
        crate::run_scenario(
            &mut context,
            "outbox_sqlite_setup",
            |scenario_context| {
                Box::pin(async move {
                    let outbox = Outbox::new(scenario_context);
                    outbox.setup().await.expect("first setup should pass");
                    outbox.setup().await.expect("second setup should pass");
                })
            },
            &mut event_log,
        )
        .await
        .expect("scenario should pass");
    }

    #[tokio::test]
    async fn sqlite_assert_helpers_cover_states_and_retry() {
        let mut context = Context::new();
        context.register_builtin(BuiltinResource::Sqlite(SqliteResource::new()));
        let mut event_log = crate::EventLog::new();
        crate::run_scenario(
            &mut context,
            "outbox_sqlite_assert_helpers",
            |scenario_context| {
                Box::pin(async move {
                    let outbox = Outbox::new(scenario_context);
                    outbox.setup().await.expect("setup should pass");
                    let sqlite_bindings = scenario_context
                        .sqlite_bindings()
                        .expect("sqlite bindings should exist")
                        .clone();
                    let database_url = sqlite_bindings.database_url;
                    tokio::task::spawn_blocking(move || {
                        let connection = rusqlite::Connection::open(database_url)
                            .expect("open sqlite database");
                        connection
                            .execute(
                                "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts)
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                                (
                                    "order",
                                    "order-1",
                                    "OrderPlaced",
                                    r#"{"aggregate_id":"order-1","total":99.0}"#,
                                    "pending",
                                    2_i32,
                                ),
                            )
                            .expect("insert outbox row");
                        connection
                            .execute(
                                "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts)
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                                (
                                    "order",
                                    "order-2",
                                    "OrderPlaced",
                                    r#"{"aggregate_id":"order-2","total":42.0}"#,
                                    "failed",
                                    5_i32,
                                ),
                            )
                            .expect("insert failed row");
                        connection
                            .execute(
                                "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts)
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                                ("order", "order-3", "OrderPlaced", r#"{"not_total":"oops"}"#, "sent", 1_i32),
                            )
                            .expect("insert sent row with invalid payload shape");
                    })
                    .await
                    .expect("blocking task should join");

                    outbox
                        .assert_pending("order-1", "OrderPlaced")
                        .await
                        .expect("pending assert should pass");
                    outbox
                        .assert_failed("order-2", "OrderPlaced")
                        .await
                        .expect("failed assert should pass");
                    outbox
                        .assert_pending("order-1", "OrderPlaced")
                        .await
                        .expect("pending assert should pass")
                        .with_payload::<OrderPlaced>(|payload| payload.total == 99.0)
                        .await
                        .expect("typed payload assert should pass");
                    outbox
                        .assert_retry("order-2", "OrderPlaced", 3)
                        .await
                        .expect("retry threshold should pass");

                    let wrong_state = outbox
                        .assert_sent("order-1", "OrderPlaced")
                        .await
                        .expect_err("sent assert should fail for pending row");
                    assert!(matches!(wrong_state, ShakedownError::OutboxWrongState { .. }));

                    let payload_assertion_failed = outbox
                        .assert_pending("order-1", "OrderPlaced")
                        .await
                        .expect("pending row should exist")
                        .with_payload::<OrderPlaced>(|payload| payload.total == 1.0)
                        .await
                        .expect_err("payload predicate should fail");
                    assert!(matches!(
                        payload_assertion_failed,
                        ShakedownError::PayloadAssertionFailed { .. }
                    ));

                    let payload_deserialisation_error = outbox
                        .assert_sent("order-3", "OrderPlaced")
                        .await
                        .expect("sent row should exist")
                        .with_payload::<OrderPlaced>(|payload| payload.total == 50.0)
                        .await
                        .expect_err("payload deserialisation should fail");
                    assert!(matches!(
                        payload_deserialisation_error,
                        ShakedownError::PayloadDeserialisation(_)
                    ));
                })
            },
            &mut event_log,
        )
        .await
        .expect("scenario should pass");
    }

    #[test]
    fn outbox_types_are_debug_and_clone() {
        let pending = OutboxRowState::Pending;
        let sent = OutboxRowState::Sent;
        let failed = OutboxRowState::Failed;
        let row = OutboxEntry {
            id: "evt-1".to_string(),
            aggregate_type: "order".to_string(),
            aggregate_id: "order-1".to_string(),
            event_type: "OrderPlaced".to_string(),
            payload: serde_json::json!({"aggregate_id":"order-1"}),
            status: pending.clone(),
            attempts: 0,
            created_at: "2026-03-26T00:00:00Z".to_string(),
            sent_at: None,
        };
        assert!(format!("{:?}", row).contains("OutboxEntry"));
        assert!(matches!(sent.clone(), OutboxRowState::Sent));
        assert!(matches!(failed.clone(), OutboxRowState::Failed));
    }
}

#[cfg(all(test, feature = "sqlite", feature = "rabbitmq"))]
mod rabbit_pub_tests {
    use super::Outbox;
    use crate::error::{InfrastructureError, ShakedownError};
    use crate::lifecycle::Context;
    use crate::resources::{BuiltinResource, SqliteResource};

    #[tokio::test]
    async fn assert_rabbit_published_errors_without_rabbit_bindings() {
        let mut context = Context::new();
        context.register_builtin(BuiltinResource::Sqlite(SqliteResource::new()));
        let mut event_log = crate::EventLog::new();
        crate::run_scenario(
            &mut context,
            "outbox_rabbit_pub_no_bindings",
            |scenario_context| {
                Box::pin(async move {
                    let outbox = Outbox::new(scenario_context);
                    outbox.setup().await.expect("setup");
                    let error = outbox
                        .assert_rabbit_published("orders_ex", "proof", "order-1")
                        .await
                        .expect_err("rabbit bindings missing");
                    match error {
                        ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                            message,
                        )) => {
                            assert!(message.contains("rabbitmq"));
                        }
                        other => panic!("unexpected error: {other:?}"),
                    }
                })
            },
            &mut event_log,
        )
        .await
        .expect("scenario should pass");
    }
}
