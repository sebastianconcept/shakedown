#![cfg(all(feature = "outbox", feature = "duckdb"))]

//! DuckDB outbox proof without Docker (in-process).

use std::path::Path;

use duckdb::Connection;
use shakedown::error::ShakedownError;
use shakedown::OutboxError;
use shakedown::{run_scenario, BuiltinResource, Context, DuckDbResource, EventLog};

#[tokio::test]
async fn duckdb_outbox_setup_and_state_assertions_work_without_docker() {
    let mut context = Context::new();
    context.register_builtin(BuiltinResource::DuckDb(DuckDbResource::new()));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "duckdb_outbox_assertions",
        |scenario_context| {
            Box::pin(async move {
                scenario_context
                    .outbox()
                    .setup()
                    .await
                    .expect("setup creates outbox_event");

                let database_url = scenario_context
                    .duckdb_bindings()
                    .expect("duckdb bindings should exist")
                    .database_url
                    .clone();

                tokio::task::spawn_blocking(move || {
                    let connection = Connection::open(Path::new(database_url.as_str()))
                        .expect("open duckdb database");

                    connection
                        .execute(
                            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            duckdb::params![
                                "order",
                                "order-11",
                                "OrderPlaced",
                                r#"{"aggregate_id":"order-11","total":33.0}"#,
                                "pending",
                                2_i32,
                            ],
                        )
                        .expect("insert pending row");

                    connection
                        .execute(
                            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            duckdb::params![
                                "order",
                                "order-22",
                                "OrderPlaced",
                                r#"{"aggregate_id":"order-22","total":66.0}"#,
                                "failed",
                                5_i32,
                            ],
                        )
                        .expect("insert failed row");

                    connection
                        .execute(
                            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            duckdb::params![
                                "order",
                                "order-33",
                                "OrderPlaced",
                                r#"{"aggregate_id":"order-33","total":99.0}"#,
                                "sent",
                                1_i32,
                            ],
                        )
                        .expect("insert sent row");

                    connection
                        .execute(
                            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            duckdb::params![
                                "order",
                                "order-44",
                                "OrderPlaced",
                                "{this-is-not-valid-json",
                                "sent",
                                1_i32,
                            ],
                        )
                        .expect("insert malformed JSON row");

                    connection
                        .execute(
                            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            duckdb::params![
                                "order",
                                "order-55",
                                "OrderPlaced",
                                r#"{"aggregate_id":"order-55","total":77.0}"#,
                                "retrying",
                                9_i32,
                            ],
                        )
                        .expect("insert unknown status row");

                    connection
                        .execute(
                            "INSERT INTO outbox_event (id, aggregate_type, aggregate_id, type, payload, status, attempts, created_at)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                            duckdb::params![
                                "evt-001",
                                "order",
                                "order-66",
                                "OrderPlaced",
                                r#"{"aggregate_id":"order-66","total":11.0}"#,
                                "sent",
                                1_i32,
                                "2026-01-01T00:00:00Z",
                            ],
                        )
                        .expect("insert older deterministic row");

                    connection
                        .execute(
                            "INSERT INTO outbox_event (id, aggregate_type, aggregate_id, type, payload, status, attempts, created_at)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                            duckdb::params![
                                "evt-999",
                                "order",
                                "order-66",
                                "OrderPlaced",
                                r#"{"aggregate_id":"order-66","total":22.0}"#,
                                "sent",
                                4_i32,
                                "2026-01-01T00:00:00Z",
                            ],
                        )
                        .expect("insert newer deterministic row by id");
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

                scenario_context
                    .outbox()
                    .assert_retry("order-66", "OrderPlaced", 3)
                    .await
                    .expect("retry should use latest deterministic row");

                let malformed_payload_error = scenario_context
                    .outbox()
                    .assert_sent("order-44", "OrderPlaced")
                    .await
                    .expect_err("malformed JSON payload must fail");
                assert!(matches!(
                    malformed_payload_error,
                    ShakedownError::Outbox(OutboxError::JsonPayload(_))
                ));

                let unknown_status_error = scenario_context
                    .outbox()
                    .assert_retry("order-55", "OrderPlaced", 1)
                    .await
                    .expect_err("unknown status must fail explicitly");
                assert!(matches!(
                    unknown_status_error,
                    ShakedownError::Outbox(OutboxError::UnknownState { .. })
                ));
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario should pass");
}
