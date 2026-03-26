#![cfg(all(feature = "outbox", feature = "postgres", feature = "kafka"))]

//! Postgres + Kafka outbox end-to-end proof (Docker-backed, ignored by default).
//!
//! Run explicitly with Docker:
//!
//! ```text
//! cargo test -p shakedown --features outbox,postgres,kafka --test outbox_postgres -- --ignored
//! ```

use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, KafkaResource, PostgresResource,
};
use tokio_postgres::NoTls;

#[derive(Debug, serde::Deserialize)]
struct OrderPlacedPayload {
    aggregate_id: String,
    total: f64,
}

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features outbox,postgres,kafka --test outbox_postgres -- --ignored"]
async fn outbox_postgres_to_kafka_flow_proves_sent_and_published() {
    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Postgres(
        PostgresResource::new().with_image_tag_from_env(),
    ));
    context.register_builtin(BuiltinResource::Kafka(
        KafkaResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "outbox_postgres_end_to_end",
        |scenario_context| {
            Box::pin(async move {
                scenario_context
                    .outbox()
                    .setup()
                    .await
                    .expect("setup creates outbox_event");

                let postgres_bindings = scenario_context
                    .postgres_bindings()
                    .expect("postgres bindings should exist");
                let (postgres_client, postgres_connection) =
                    tokio_postgres::connect(postgres_bindings.connection_url.as_str(), NoTls)
                        .await
                        .expect("connect to scenario postgres database");
                tokio::spawn(async move {
                    let _ = postgres_connection.await;
                });

                postgres_client
                    .execute(
                        "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts)
                         VALUES ($1, $2, $3, $4::text::jsonb, $5, $6)",
                        &[
                            &"order",
                            &"order-1",
                            &"OrderPlaced",
                            &r#"{"aggregate_id":"order-1","total":99.0}"#,
                            &"pending",
                            &0_i32,
                        ],
                    )
                    .await
                    .expect("insert pending outbox row");

                scenario_context
                    .outbox()
                    .assert_pending("order-1", "OrderPlaced")
                    .await
                    .expect("pending row should be asserted");

                postgres_client
                    .execute(
                        "UPDATE outbox_event
                         SET status = 'sent', attempts = attempts + 1, sent_at = now()
                         WHERE aggregate_id = $1 AND type = $2",
                        &[&"order-1", &"OrderPlaced"],
                    )
                    .await
                    .expect("mark outbox row as sent");

                let kafka_bindings = scenario_context
                    .kafka_bindings()
                    .expect("kafka bindings should exist");
                let topic_name = kafka_bindings.topic_namespace("shakedown.proof");
                let producer: BaseProducer = ClientConfig::new()
                    .set(
                        "bootstrap.servers",
                        kafka_bindings.bootstrap_servers.as_str(),
                    )
                    .set("client.id", "shakedown_outbox_postgres_test")
                    .create()
                    .expect("create kafka producer");

                producer
                    .send(
                        BaseRecord::to(topic_name.as_str())
                            .key("order-1")
                            .payload(r#"{"aggregate_id":"order-1","total":99.0}"#),
                    )
                    .expect("enqueue kafka record");
                producer
                    .flush(std::time::Duration::from_secs(5))
                    .expect("flush kafka producer");

                scenario_context
                    .outbox()
                    .assert_sent("order-1", "OrderPlaced")
                    .await
                    .expect("sent row should be asserted")
                    .with_payload::<OrderPlacedPayload>(|payload| {
                        payload.aggregate_id == "order-1" && payload.total == 99.0
                    })
                    .await
                    .expect("payload assertion should pass");

                scenario_context
                    .outbox()
                    .assert_kafka_published("shakedown.proof", "order-1")
                    .await
                    .expect("kafka record should be observed");
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario should pass");
}
