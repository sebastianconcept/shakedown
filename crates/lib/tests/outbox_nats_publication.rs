#![cfg(all(feature = "outbox", feature = "sqlite", feature = "nats"))]

//! SQLite outbox + NATS JetStream publication proof (NATS Docker; SQLite in-process).
//!
//! ```text
//! cargo test -p shakedown --features outbox,sqlite,nats --test outbox_nats_publication -- --ignored
//! ```

use bytes::Bytes;
use shakedown::{run_scenario, BuiltinResource, Context, EventLog, NatsResource, SqliteResource};

#[tokio::test]
#[ignore = "requires Docker for NATS; run: cargo test -p shakedown --features outbox,sqlite,nats --test outbox_nats_publication -- --ignored"]
async fn outbox_nats_publication_verifier_observes_json_aggregate_id() {
    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Sqlite(SqliteResource::new()));
    context.register_builtin(BuiltinResource::Nats(
        NatsResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "outbox_nats_publication_proof",
        |scenario_context| {
            Box::pin(async move {
                scenario_context
                    .outbox()
                    .setup()
                    .await
                    .expect("setup creates outbox_event");

                let base_subject = "orders.events";
                let aggregate_id = "order-nats-1";
                let payload = format!(r#"{{"aggregate_id":"{aggregate_id}","total":1.0}}"#);

                let nats_bindings = scenario_context
                    .nats_bindings()
                    .expect("nats bindings after provision");
                let connection_url = nats_bindings.connection_url.clone();
                let namespaced_subject = nats_bindings.subject_namespace(base_subject);

                let publisher = async_nats::connect(connection_url.as_str())
                    .await
                    .expect("connect publisher client");
                publisher
                    .publish(
                        namespaced_subject.clone(),
                        Bytes::from(payload.into_bytes()),
                    )
                    .await
                    .expect("publish to namespaced NATS subject");
                publisher.flush().await.expect("flush NATS publish");
                let _ = publisher.drain().await;

                scenario_context
                    .outbox()
                    .assert_nats_published(base_subject, aggregate_id)
                    .await
                    .expect("JetStream should retain the published message");
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario should pass");
}
