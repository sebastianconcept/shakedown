//! Transactional outbox helpers for scenario databases.
//!
//! Services that write domain events to an outbox database table (or collection) and then publish them
//! to a message bus can be cumbersome to test end-to-end. This module provisions a shakedown
//! `outbox_event` table (or MongoDB collection), exposes row-state assertions (`assert_pending`,
//! `assert_sent`, …), and integrates with [`crate::publication`] so you can verify the full
//! pipeline — database write → outbox poll → broker publication — ergonomically in a single scenario.
//!
//! Call `ctx.setup_outbox()` inside your seed or scenario body to create the outbox
//! table for the active relational binding. Use the row-state assertion helpers to check that your
//! service's outbox relay processed each event correctly. Pair with a
//! [`crate::publication::PublicationVerifier`] to confirm delivery reached the broker.
//!
//! **DuckDB and SQLite (assertion-only):** row-state assertions do not exercise
//! `SELECT FOR UPDATE SKIP LOCKED` or other claim-locking semantics; they only read the latest
//! matching row for observability-style checks.
//!
//! [`OutboxError`] is defined in `outbox/error.rs` (crate-private) and re-exported at the shakedown crate
//! root as [`crate::OutboxError`].
//!
//! # Book reference
//!
//! Part II, *Chapter 12: Outbox Pattern* — explains the transactional outbox model, how
//! Shakedown provisions the outbox table, and how to assert the full write → relay → publish
//! pipeline in one scenario.

pub use crate::OutboxError;

mod util;

#[cfg(feature = "postgres")]
mod postgres;

#[cfg(any(feature = "mariadb", feature = "mysql"))]
mod mysql_family;

#[cfg(feature = "sqlite")]
mod sqlite;

#[cfg(feature = "mongodb")]
mod mongo;

#[cfg(feature = "duckdb")]
mod duckdb;

#[cfg(feature = "rabbitmq")]
use crate::error::InfrastructureError;
use crate::error::ShakedownError;
use crate::lifecycle::Context;

use util::outbox_row_state_label;

/// Row state for records in the canonical `outbox_event` table or collection.
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
        let payload =
            serde_json::from_value::<T>(self.entry.payload.clone()).map_err(OutboxError::from)?;
        if !payload_assertion(&payload) {
            return Err(OutboxError::PayloadAssertionFailed {
                aggregate_id: self.entry.aggregate_id,
                event_type: self.entry.event_type,
            }
            .into());
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
            return postgres::setup(postgres_bindings.connection_url.as_str()).await;
        }

        #[cfg(feature = "mariadb")]
        if let Some(mariadb_bindings) = self.context.mariadb_bindings() {
            return mysql_family::setup(mariadb_bindings.connection_url.as_str()).await;
        }

        #[cfg(feature = "mysql")]
        if let Some(mysql_bindings) = self.context.mysql_bindings() {
            return mysql_family::setup(mysql_bindings.connection_url.as_str()).await;
        }

        #[cfg(feature = "sqlite")]
        if let Some(sqlite_bindings) = self.context.sqlite_bindings() {
            return sqlite::setup(sqlite_bindings.database_url.as_str()).await;
        }

        #[cfg(feature = "mongodb")]
        if let Some(mongo_bindings) = self.context.mongo_bindings() {
            return mongo::ensure_outbox_collection(mongo_bindings).await;
        }

        #[cfg(feature = "duckdb")]
        if let Some(duckdb_bindings) = self.context.duckdb_bindings() {
            return duckdb::setup(duckdb_bindings.database_url.as_str()).await;
        }

        Err(OutboxError::NoRelationalBindings.into())
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
            return Err(OutboxError::RetryBelowThreshold {
                aggregate_id: aggregate_id.to_string(),
                attempts: entry.attempts,
                min_attempts,
            }
            .into());
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

    /// Assert a NATS message for `aggregate_id` was persisted to JetStream on the scenario-namespaced
    /// subject.
    ///
    /// Pass the **logical** subject name (without the per-scenario prefix); it is passed through
    /// [`crate::resources::NatsBindings::subject_namespace`]. The built-in [`crate::resources::NatsResource`]
    /// starts the server with JetStream enabled so core `publish` traffic is captured by the stream.
    ///
    /// JetStream retains messages, so you may **publish (or run the service under test) first** and
    /// call this helper afterward — no subscribe-first ordering requirement.
    #[cfg(feature = "nats")]
    pub async fn assert_nats_published(
        &self,
        base_subject: &str,
        aggregate_id: &str,
    ) -> Result<(), ShakedownError> {
        use crate::publication::{
            NatsPublicationVerifier, NatsPublicationVerifierOptions, PublicationObserveSpec,
            PublicationVerifier,
        };

        let verifier = NatsPublicationVerifier;
        let spec = PublicationObserveSpec {
            aggregate_id: aggregate_id.to_string(),
            logical_channel: base_subject.to_string(),
            timeout: None,
        };
        let options = NatsPublicationVerifierOptions::default();
        PublicationVerifier::assert_observed(&verifier, self.context, &spec, &options).await
    }

    /// Assert a Redis Stream entry for `aggregate_id` was observed on the scenario-namespaced stream.
    ///
    /// Pass the **logical** stream name (without the per-scenario key prefix); it is combined with
    /// [`crate::resources::RedisBindings::key_prefix`]. The default [`crate::publication::RedisStreamsPublicationVerifierOptions`]
    /// reads the `"payload"` field as JSON for [`crate::publication::message_matches_aggregate_id`].
    /// Override options via [`crate::publication::PublicationVerifier::assert_observed`].
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub async fn assert_redis_streams_published(
        &self,
        base_stream: &str,
        aggregate_id: &str,
    ) -> Result<(), ShakedownError> {
        use crate::publication::{
            PublicationObserveSpec, PublicationVerifier, RedisStreamsPublicationVerifier,
            RedisStreamsPublicationVerifierOptions,
        };

        let verifier = RedisStreamsPublicationVerifier;
        let spec = PublicationObserveSpec {
            aggregate_id: aggregate_id.to_string(),
            logical_channel: base_stream.to_string(),
            timeout: None,
        };
        let options = RedisStreamsPublicationVerifierOptions::default();
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
            return postgres::fetch_entry_with_status(
                postgres_bindings.connection_url.as_str(),
                aggregate_id,
                event_type,
                expected_status,
            )
            .await;
        }

        #[cfg(feature = "mariadb")]
        if let Some(mariadb_bindings) = self.context.mariadb_bindings() {
            return mysql_family::fetch_entry_with_status(
                mariadb_bindings.connection_url.as_str(),
                aggregate_id,
                event_type,
                expected_status,
            )
            .await;
        }

        #[cfg(feature = "mysql")]
        if let Some(mysql_bindings) = self.context.mysql_bindings() {
            return mysql_family::fetch_entry_with_status(
                mysql_bindings.connection_url.as_str(),
                aggregate_id,
                event_type,
                expected_status,
            )
            .await;
        }

        #[cfg(feature = "sqlite")]
        if let Some(sqlite_bindings) = self.context.sqlite_bindings() {
            return sqlite::fetch_entry_with_status(
                sqlite_bindings.database_url.as_str(),
                aggregate_id,
                event_type,
                expected_status,
            )
            .await;
        }

        #[cfg(feature = "mongodb")]
        if let Some(mongo_bindings) = self.context.mongo_bindings() {
            return mongo::fetch_entry_with_status(
                mongo_bindings,
                aggregate_id,
                event_type,
                expected_status,
            )
            .await;
        }

        #[cfg(feature = "duckdb")]
        if let Some(duckdb_bindings) = self.context.duckdb_bindings() {
            return duckdb::fetch_entry_with_status(
                duckdb_bindings.database_url.as_str(),
                aggregate_id,
                event_type,
                expected_status,
            )
            .await;
        }

        Err(OutboxError::NoRelationalBindings.into())
    }

    async fn fetch_latest_entry(
        &self,
        aggregate_id: &str,
        event_type: &str,
    ) -> Result<OutboxEntry, ShakedownError> {
        #[cfg(feature = "postgres")]
        if let Some(postgres_bindings) = self.context.postgres_bindings() {
            return postgres::fetch_latest_entry(
                postgres_bindings.connection_url.as_str(),
                aggregate_id,
                event_type,
            )
            .await;
        }

        #[cfg(feature = "mariadb")]
        if let Some(mariadb_bindings) = self.context.mariadb_bindings() {
            return mysql_family::fetch_latest_entry(
                mariadb_bindings.connection_url.as_str(),
                aggregate_id,
                event_type,
            )
            .await;
        }

        #[cfg(feature = "mysql")]
        if let Some(mysql_bindings) = self.context.mysql_bindings() {
            return mysql_family::fetch_latest_entry(
                mysql_bindings.connection_url.as_str(),
                aggregate_id,
                event_type,
            )
            .await;
        }

        #[cfg(feature = "sqlite")]
        if let Some(sqlite_bindings) = self.context.sqlite_bindings() {
            return sqlite::fetch_latest_entry(
                sqlite_bindings.database_url.as_str(),
                aggregate_id,
                event_type,
            )
            .await;
        }

        #[cfg(feature = "mongodb")]
        if let Some(mongo_bindings) = self.context.mongo_bindings() {
            return mongo::fetch_latest_entry(mongo_bindings, aggregate_id, event_type).await;
        }

        #[cfg(feature = "duckdb")]
        if let Some(duckdb_bindings) = self.context.duckdb_bindings() {
            return duckdb::fetch_latest_entry(
                duckdb_bindings.database_url.as_str(),
                aggregate_id,
                event_type,
            )
            .await;
        }

        Err(OutboxError::NoRelationalBindings.into())
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::{Outbox, OutboxEntry, OutboxRowState};
    use crate::error::ShakedownError;
    use crate::lifecycle::Context;
    use crate::resources::{BuiltinResource, SqliteResource};
    use crate::OutboxError;
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
                    assert!(matches!(
                        wrong_state,
                        ShakedownError::Outbox(OutboxError::WrongState { .. })
                    ));

                    let payload_assertion_failed = outbox
                        .assert_pending("order-1", "OrderPlaced")
                        .await
                        .expect("pending row should exist")
                        .with_payload::<OrderPlaced>(|payload| payload.total == 1.0)
                        .await
                        .expect_err("payload predicate should fail");
                    assert!(matches!(
                        payload_assertion_failed,
                        ShakedownError::Outbox(OutboxError::PayloadAssertionFailed { .. })
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
                        ShakedownError::Outbox(OutboxError::JsonPayload(_))
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
