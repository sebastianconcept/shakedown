#![cfg(all(feature = "outbox", feature = "sqlite"))]

//! SQLite outbox proof without Docker.

use rusqlite::{Connection, OpenFlags};
use shakedown::{run_scenario, BuiltinResource, Context, EventLog, SqliteResource};

#[tokio::test]
async fn sqlite_outbox_setup_and_state_assertions_work_without_docker() {
    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Sqlite(SqliteResource::new()));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "sqlite_outbox_assertions",
        |scenario_context| {
            Box::pin(async move {
                scenario_context
                    .outbox()
                    .setup()
                    .await
                    .expect("setup creates outbox_event");

                let database_url = scenario_context
                    .sqlite_bindings()
                    .expect("sqlite bindings should exist")
                    .database_url
                    .clone();

                tokio::task::spawn_blocking(move || {
                    let connection = Connection::open_with_flags(
                        database_url.as_str(),
                        OpenFlags::SQLITE_OPEN_URI
                            | OpenFlags::SQLITE_OPEN_READ_WRITE
                            | OpenFlags::SQLITE_OPEN_CREATE,
                    )
                    .expect("open sqlite database");

                    connection
                        .execute(
                            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            (
                                "order",
                                "order-11",
                                "OrderPlaced",
                                r#"{"aggregate_id":"order-11","total":33.0}"#,
                                "pending",
                                2_i32,
                            ),
                        )
                        .expect("insert pending row");

                    connection
                        .execute(
                            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            (
                                "order",
                                "order-22",
                                "OrderPlaced",
                                r#"{"aggregate_id":"order-22","total":66.0}"#,
                                "failed",
                                5_i32,
                            ),
                        )
                        .expect("insert failed row");

                    connection
                        .execute(
                            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            (
                                "order",
                                "order-33",
                                "OrderPlaced",
                                r#"{"aggregate_id":"order-33","total":99.0}"#,
                                "sent",
                                1_i32,
                            ),
                        )
                        .expect("insert sent row");
                })
                .await
                .expect("blocking write task");

                scenario_context
                    .outbox()
                    .assert_pending("order-11", "OrderPlaced")
                    .await
                    .expect("pending row should be asserted");
                scenario_context
                    .outbox()
                    .assert_failed("order-22", "OrderPlaced")
                    .await
                    .expect("failed row should be asserted");
                scenario_context
                    .outbox()
                    .assert_sent("order-33", "OrderPlaced")
                    .await
                    .expect("sent row should be asserted");
                scenario_context
                    .outbox()
                    .assert_retry("order-22", "OrderPlaced", 3)
                    .await
                    .expect("retry threshold should be asserted");
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario should pass");
}
