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
}

fn as_backend_error(error: impl std::error::Error + Send + Sync + 'static) -> ShakedownError {
    ShakedownError::Infrastructure(InfrastructureError::Backend(Box::new(error)))
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::{Outbox, OutboxEntry, OutboxRowState};
    use crate::lifecycle::Context;
    use crate::resources::{BuiltinResource, SqliteResource};

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
