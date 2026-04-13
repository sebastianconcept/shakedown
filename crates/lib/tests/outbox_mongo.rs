#![cfg(all(feature = "outbox", feature = "mongodb"))]

//! MongoDB outbox collection + row assertions (Docker).
//!
//! ```text
//! cargo test -p shakedown --features outbox,mongodb --test outbox_mongo -- --ignored
//! ```

use serde::Deserialize;
use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, MongoResource, OutboxError, ShakedownError,
};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct OrderPlaced {
    total: f64,
}

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features outbox,mongodb --test outbox_mongo -- --ignored"]
async fn mongo_outbox_setup_and_row_assertions() {
    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Mongo(
        MongoResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "outbox_mongo_proof",
        |scenario_context| {
            Box::pin(async move {
                let outbox = scenario_context.outbox();
                outbox
                    .setup()
                    .await
                    .expect("setup creates outbox_event collection");
                outbox.setup().await.expect("setup is idempotent");

                let id1 = Uuid::new_v4().to_string();
                let id2 = Uuid::new_v4().to_string();
                let id3 = Uuid::new_v4().to_string();

                scenario_context
                    .mongo()
                    .expect("mongodb helpers")
                    .insert_document(
                        "outbox_event",
                        serde_json::json!({
                            "_id": id1,
                            "aggregate_type": "order",
                            "aggregate_id": "order-1",
                            "type": "OrderPlaced",
                            "payload": r#"{"aggregate_id":"order-1","total":99.0}"#,
                            "status": "pending",
                            "attempts": 2,
                            "next_retry_at": null,
                            "created_at": "2026-03-26T12:00:00Z",
                            "sent_at": null
                        }),
                    )
                    .await
                    .expect("insert pending");

                scenario_context
                    .mongo()
                    .expect("mongodb helpers")
                    .insert_document(
                        "outbox_event",
                        serde_json::json!({
                            "_id": id2,
                            "aggregate_type": "order",
                            "aggregate_id": "order-2",
                            "type": "OrderPlaced",
                            "payload": r#"{"aggregate_id":"order-2","total":42.0}"#,
                            "status": "failed",
                            "attempts": 5,
                            "next_retry_at": null,
                            "created_at": "2026-03-26T12:00:01Z",
                            "sent_at": null
                        }),
                    )
                    .await
                    .expect("insert failed");

                scenario_context
                    .mongo()
                    .expect("mongodb helpers")
                    .insert_document(
                        "outbox_event",
                        serde_json::json!({
                            "_id": id3,
                            "aggregate_type": "order",
                            "aggregate_id": "order-3",
                            "type": "OrderPlaced",
                            "payload": r#"{"not_total":"oops"}"#,
                            "status": "sent",
                            "attempts": 1,
                            "next_retry_at": null,
                            "created_at": "2026-03-26T12:00:02Z",
                            "sent_at": "2026-03-26T12:00:03Z"
                        }),
                    )
                    .await
                    .expect("insert sent");

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
