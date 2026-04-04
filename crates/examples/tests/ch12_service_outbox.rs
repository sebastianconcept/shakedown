//! Chapter 12: Outbox row state + Kafka and RabbitMQ publication proof
//!
//! **Resources:** Postgres, Redis, Kafka, RabbitMQ
//! **Service:** `example-order-service` with `OUTBOX_ENABLED=true` (writes outbox, no direct Kafka produce)
//! **Assertions:** `outbox().assert_pending`, `with_payload`, `assert_kafka_published`,
//!   `assert_rabbit_published`
//! **Concepts:** transactional outbox in Postgres; declare Rabbit topology with a **per-order**
//!   binding (routing key = aggregate id) before the simulated publisher runs; publication
//!   verifiers prove both transports observed the same aggregate id
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! The sample service only inserts the outbox row. This chapter **simulates the outbox relay**
//! (mark sent + publish) the way integration tests often do when the relay is a separate process —
//! then proves delivery with Shakedown publication verifiers.
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,redis,kafka,rabbitmq,outbox --test ch12_service_outbox -- --ignored`

use shakedown::ExchangeKind;
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL, OUTBOX_SCHEMA_SQL};

#[shakedown::test(resources = [Postgres, Redis, Kafka, Rabbit])]
#[ignore = "requires Docker + locally-built example-order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,redis,kafka,rabbitmq,outbox --test ch12_service_outbox -- --ignored"]
async fn outbox_pending_then_publication_proof_kafka_and_rabbit(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("orders schema");
    ctx.postgres()
        .execute(OUTBOX_SCHEMA_SQL, &[])
        .await
        .expect("outbox schema");

    ctx.outbox()
        .setup()
        .await
        .expect("canonical outbox table (matches relational DDL)");

    let order_spec = shakedown::ServiceUnderTest::docker("example-order-service:latest")
        .name("order-service")
        .port(3000)
        .health_check(shakedown::HealthCheck::http("/health"))
        .env("OUTBOX_ENABLED", "true")
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("order service spec");

    ctx.register_service(order_spec)
        .expect("register order-service");

    let base_url = ctx.service_url("order-service").expect("order service URL");

    let http_client = reqwest::Client::new();
    let response = http_client
        .post(format!("{base_url}/orders"))
        .json(&sample_order("customer-ch12-outbox"))
        .send()
        .await
        .expect("POST /orders");

    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let body: serde_json::Value = response.json().await.expect("response JSON");
    let order_id = body["id"].as_str().expect("order id");

    let outbox = ctx.outbox();
    outbox
        .assert_pending(order_id, "OrderCreated")
        .await
        .expect("outbox row should start in pending state")
        .with_payload::<serde_json::Value>(|payload| {
            payload.get("aggregate_id").and_then(|value| value.as_str()) == Some(order_id)
        })
        .await
        .expect("payload should contain aggregate_id matching the order");

    ctx.rabbit()
        .declare_topology(
            "shakedown.proof",
            ExchangeKind::Direct,
            "shakedown.proof",
            order_id,
        )
        .await
        .expect("proof queue bound with routing key = aggregate id (verifier expectation)");

    let payload_value = ctx
        .postgres()
        .query_one(
            "SELECT payload::text FROM outbox_event WHERE aggregate_id = $1 AND type = $2",
            &[&order_id, &"OrderCreated"],
        )
        .await
        .expect("read outbox payload for relay simulation");
    let payload_text: String = payload_value.get(0);

    ctx.postgres()
        .execute(
            "UPDATE outbox_event SET status = 'sent', attempts = attempts + 1, sent_at = now() \
             WHERE aggregate_id = $1 AND type = $2",
            &[&order_id, &"OrderCreated"],
        )
        .await
        .expect("mark outbox row sent (simulated relay completion)");

    let payload_json: serde_json::Value =
        serde_json::from_str(payload_text.as_str()).expect("outbox payload JSON");

    ctx.kafka()
        .produce_json("orders", order_id, &payload_json)
        .await
        .expect("simulated publisher produces to orders");

    ctx.rabbit()
        .publish_json("shakedown.proof", order_id, &payload_json)
        .await
        .expect("simulated publisher publishes to proof exchange");

    outbox
        .assert_kafka_published("orders", order_id)
        .await
        .expect("Kafka publication proof");

    outbox
        .assert_rabbit_published("shakedown.proof", "shakedown.proof", order_id)
        .await
        .expect("RabbitMQ publication proof");
}
