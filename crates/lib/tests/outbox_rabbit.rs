#![cfg(all(feature = "outbox", feature = "postgres", feature = "rabbitmq"))]

//! Postgres + RabbitMQ outbox end-to-end proof (Docker-backed, ignored by default).
//!
//! Does **not** enable the `kafka` feature — publication proof uses AMQP only.
//!
//! Run explicitly with Docker:
//!
//! ```text
//! cargo test -p shakedown --features outbox,postgres,rabbitmq --test outbox_rabbit -- --ignored
//! ```

use lapin::options::{
    BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties, ExchangeKind};
use tokio_postgres::NoTls;

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, PostgresResource, RabbitResource,
};

#[derive(Debug, serde::Deserialize)]
struct OrderPlacedPayload {
    aggregate_id: String,
    total: f64,
}

/// Logical exchange and queue names (scenario prefix applied via [`shakedown::RabbitBindings::namespaced_name`]).
const BASE_EXCHANGE: &str = "shakedown.proof";
const BASE_QUEUE: &str = "shakedown.proof";

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features outbox,postgres,rabbitmq --test outbox_rabbit -- --ignored"]
async fn outbox_postgres_to_rabbit_flow_proves_sent_and_published() {
    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Postgres(
        PostgresResource::new().with_image_tag_from_env(),
    ));
    context.register_builtin(BuiltinResource::Rabbit(
        RabbitResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "outbox_rabbit_end_to_end",
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

                let rabbit_bindings = scenario_context
                    .rabbit_bindings()
                    .expect("rabbit bindings should exist");
                let exchange_name = rabbit_bindings.namespaced_name(BASE_EXCHANGE);
                let queue_name = rabbit_bindings.namespaced_name(BASE_QUEUE);

                let connection = Connection::connect(
                    rabbit_bindings.amqp_uri.as_str(),
                    ConnectionProperties::default(),
                )
                .await
                .expect("connect to rabbitmq for topology + publish");
                let channel = connection
                    .create_channel()
                    .await
                    .expect("create amqp channel");

                channel
                    .exchange_declare(
                        exchange_name.as_str().into(),
                        ExchangeKind::Direct,
                        ExchangeDeclareOptions {
                            durable: true,
                            ..ExchangeDeclareOptions::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                    .expect("declare direct exchange");

                channel
                    .queue_declare(
                        queue_name.as_str().into(),
                        QueueDeclareOptions {
                            durable: true,
                            ..QueueDeclareOptions::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                    .expect("declare queue");

                channel
                    .queue_bind(
                        queue_name.as_str().into(),
                        exchange_name.as_str().into(),
                        "order-1".into(),
                        QueueBindOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                    .expect("bind queue to exchange with aggregate routing key");

                let payload_json = r#"{"aggregate_id":"order-1","total":99.0}"#;
                channel
                    .basic_publish(
                        exchange_name.as_str().into(),
                        "order-1".into(),
                        BasicPublishOptions::default(),
                        payload_json.as_bytes(),
                        BasicProperties::default(),
                    )
                    .await
                    .expect("basic publish")
                    .await
                    .expect("publisher confirm");

                let _ = connection.close(200, "shakedown_test_pub_done".into()).await;

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
                    .assert_rabbit_published(BASE_EXCHANGE, BASE_QUEUE, "order-1")
                    .await
                    .expect("amqp message should be observed");
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario should pass");
}
