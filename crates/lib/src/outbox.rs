//! Outbox setup helpers for scenario databases.
//!
//! This module creates the canonical `outbox_event` table in the active relational
//! binding attached to [`crate::lifecycle::Context`].

use crate::error::{InfrastructureError, ShakedownError};
use crate::lifecycle::Context;
#[cfg(feature = "kafka")]
use rdkafka::config::ClientConfig;
#[cfg(feature = "kafka")]
use rdkafka::consumer::{BaseConsumer, Consumer};
#[cfg(feature = "kafka")]
use rdkafka::message::Message;
#[cfg(feature = "kafka")]
use std::time::{Duration, Instant};

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

#[cfg(feature = "kafka")]
const DEFAULT_KAFKA_ASSERT_TIMEOUT: Duration = Duration::from_secs(10);

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
        let entry = self.fetch_entry(aggregate_id, event_type).await?;
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
        let kafka_bindings = self.context.kafka_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "kafka bindings".to_string(),
            ))
        })?;

        let topic = kafka_bindings.topic_namespace(base_topic);
        let consumer_group = format!(
            "shakedown-outbox-assert-{}",
            self.context.scenario_id().as_str()
        );
        let consumer: BaseConsumer = ClientConfig::new()
            .set(
                "bootstrap.servers",
                kafka_bindings.bootstrap_servers.as_str(),
            )
            .set("group.id", consumer_group.as_str())
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(as_backend_error)?;

        consumer
            .subscribe(&[topic.as_str()])
            .map_err(as_backend_error)?;

        let deadline = Instant::now() + DEFAULT_KAFKA_ASSERT_TIMEOUT;
        while Instant::now() < deadline {
            let remaining_duration = deadline.saturating_duration_since(Instant::now());
            let poll_duration = remaining_duration.min(Duration::from_millis(200));
            if let Some(result) = consumer.poll(poll_duration) {
                let message = result.map_err(as_backend_error)?;
                if kafka_message_matches_aggregate_id(&message, aggregate_id) {
                    return Ok(());
                }
            }
        }

        Err(ShakedownError::OutboxKafkaRecordNotFound {
            topic,
            aggregate_id: aggregate_id.to_string(),
        })
    }

    async fn assert_state(
        &self,
        aggregate_id: &str,
        event_type: &str,
        expected_state: OutboxRowState,
    ) -> Result<OutboxAssertBuilder, ShakedownError> {
        let entry = self.fetch_entry(aggregate_id, event_type).await?;
        if entry.status != expected_state {
            return Err(ShakedownError::OutboxWrongState {
                aggregate_id: aggregate_id.to_string(),
                event_type: event_type.to_string(),
                expected: outbox_row_state_label(expected_state).to_string(),
                actual: outbox_row_state_label(entry.status.clone()).to_string(),
            });
        }
        Ok(OutboxAssertBuilder { entry })
    }

    async fn fetch_entry(
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
                .query_opt(
                    r#"
SELECT id::text, aggregate_type, aggregate_id, type, payload::text, status, attempts, created_at::text, sent_at::text
FROM outbox_event
WHERE aggregate_id = $1 AND type = $2
LIMIT 1
"#,
                    &[&aggregate_id, &event_type],
                )
                .await
                .map_err(as_backend_error)?;
            if let Some(row) = maybe_row {
                let payload_text: String = row.get(4);
                let payload = serde_json::from_str(payload_text.as_str()).map_err(|error| {
                    ShakedownError::Infrastructure(InfrastructureError::Backend(Box::new(error)))
                })?;
                return Ok(OutboxEntry {
                    id: row.get(0),
                    aggregate_type: row.get(1),
                    aggregate_id: row.get(2),
                    event_type: row.get(3),
                    payload,
                    status: parse_outbox_row_state(&row.get::<usize, String>(5)),
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
            let maybe_row: Option<(String, String, String, String, String, String, i32, String, Option<String>)> = connection
                .exec_first(
                    r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, CAST(created_at AS CHAR), CAST(sent_at AS CHAR)
FROM outbox_event
WHERE aggregate_id = ? AND type = ?
LIMIT 1
"#,
                    (aggregate_id, event_type),
                )
                .await
                .map_err(as_backend_error)?;
            connection.disconnect().await.map_err(as_backend_error)?;
            if let Some(row) = maybe_row {
                let payload = serde_json::from_str(row.4.as_str()).map_err(|error| {
                    ShakedownError::Infrastructure(InfrastructureError::Backend(Box::new(error)))
                })?;
                return Ok(OutboxEntry {
                    id: row.0,
                    aggregate_type: row.1,
                    aggregate_id: row.2,
                    event_type: row.3,
                    payload,
                    status: parse_outbox_row_state(row.5.as_str()),
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
            let maybe_row: Option<(String, String, String, String, String, String, i32, String, Option<String>)> = connection
                .exec_first(
                    r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, CAST(created_at AS CHAR), CAST(sent_at AS CHAR)
FROM outbox_event
WHERE aggregate_id = ? AND type = ?
LIMIT 1
"#,
                    (aggregate_id, event_type),
                )
                .await
                .map_err(as_backend_error)?;
            connection.disconnect().await.map_err(as_backend_error)?;
            if let Some(row) = maybe_row {
                let payload = serde_json::from_str(row.4.as_str()).map_err(|error| {
                    ShakedownError::Infrastructure(InfrastructureError::Backend(Box::new(error)))
                })?;
                return Ok(OutboxEntry {
                    id: row.0,
                    aggregate_type: row.1,
                    aggregate_id: row.2,
                    event_type: row.3,
                    payload,
                    status: parse_outbox_row_state(row.5.as_str()),
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
            let maybe_row = tokio::task::spawn_blocking(move || {
                use rusqlite::OpenFlags;
                let connection = rusqlite::Connection::open_with_flags(
                    sqlite_url.as_str(),
                    OpenFlags::SQLITE_OPEN_URI
                        | OpenFlags::SQLITE_OPEN_READ_WRITE
                        | OpenFlags::SQLITE_OPEN_CREATE,
                )?;
                let mut statement = connection.prepare(
                    r#"
SELECT id, aggregate_type, aggregate_id, type, payload, status, attempts, created_at, sent_at
FROM outbox_event
WHERE aggregate_id = ?1 AND type = ?2
LIMIT 1
"#,
                )?;
                let mut rows = statement.query([aggregate_id_value, event_type_value])?;
                if let Some(row) = rows.next()? {
                    let payload_text: String = row.get(4)?;
                    let payload = serde_json::from_str(payload_text.as_str())
                        .unwrap_or_else(|_| serde_json::json!(payload_text));
                    Ok::<_, rusqlite::Error>(Some(OutboxEntry {
                        id: row.get(0)?,
                        aggregate_type: row.get(1)?,
                        aggregate_id: row.get(2)?,
                        event_type: row.get(3)?,
                        payload,
                        status: parse_outbox_row_state(row.get::<usize, String>(5)?.as_str()),
                        attempts: row.get(6)?,
                        created_at: row.get(7)?,
                        sent_at: row.get(8)?,
                    }))
                } else {
                    Ok::<_, rusqlite::Error>(None)
                }
            })
            .await
            .map_err(as_backend_error)?
            .map_err(as_backend_error)?;
            if let Some(entry) = maybe_row {
                return Ok(entry);
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

fn parse_outbox_row_state(status: &str) -> OutboxRowState {
    match status {
        "pending" => OutboxRowState::Pending,
        "sent" => OutboxRowState::Sent,
        "failed" => OutboxRowState::Failed,
        _ => OutboxRowState::Failed,
    }
}

fn outbox_row_state_label(state: OutboxRowState) -> &'static str {
    match state {
        OutboxRowState::Pending => "pending",
        OutboxRowState::Sent => "sent",
        OutboxRowState::Failed => "failed",
    }
}

#[cfg(feature = "kafka")]
fn kafka_message_matches_aggregate_id(message: &impl Message, aggregate_id: &str) -> bool {
    if let Some(key_bytes) = message.key() {
        if let Ok(key_text) = std::str::from_utf8(key_bytes) {
            if key_text == aggregate_id {
                return true;
            }
        }
    }

    if let Some(payload_bytes) = message.payload() {
        if let Ok(payload_text) = std::str::from_utf8(payload_bytes) {
            if let Ok(payload_json) = serde_json::from_str::<serde_json::Value>(payload_text) {
                return payload_json
                    .get("aggregate_id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|payload_aggregate_id| payload_aggregate_id == aggregate_id);
            }
        }
    }

    false
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
