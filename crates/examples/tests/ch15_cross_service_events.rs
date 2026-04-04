//! Chapter 15: Cross-service event chain — Kafka → Fulfillment → MongoDB → RabbitMQ
//!
//! **Resources:** Postgres, Redis, Kafka, MongoDB, RabbitMQ
//! **Services:** Order Service + Fulfillment Service
//! **Assertions:** `assert_row_exists`, `kafka().consume_until`, `mongo().assert_document_exists`,
//!   `rabbit().consume_until`
//! **Concepts:** `depends_on` for startup order (Fulfillment after Order), declare Rabbit topology
//!   before services publish, async consumer path with bounded wait
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,mongodb,kafka,rabbitmq,redis --test ch15_cross_service_events -- --ignored`

use mongodb::bson::doc;
use shakedown::ExchangeKind;
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::test(resources = [Postgres, Redis, Kafka, Mongo, Rabbit])]
#[ignore = "requires Docker + both example service images; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,mongodb,kafka,rabbitmq,redis --test ch15_cross_service_events -- --ignored"]
async fn order_kafka_event_triggers_fulfillment_mongo_and_rabbit_fanout(
    ctx: &mut shakedown::Context,
) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema");

    ctx.rabbit()
        .declare_topology(
            "fulfillments",
            ExchangeKind::Direct,
            "fulfillments-assert",
            "#",
        )
        .await
        .expect("declare RabbitMQ topology for assertions");

    let order_spec = shakedown::ServiceUnderTest::docker("order-service:latest")
        .name("order-service")
        .port(3000)
        .health_check(shakedown::HealthCheck::http("/health"))
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("order service spec");

    let fulfillment_spec = shakedown::ServiceUnderTest::docker("fulfillment-service:latest")
        .name("fulfillment-service")
        .port(3001)
        .health_check(shakedown::HealthCheck::http("/health"))
        .depends_on("order-service")
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("fulfillment service spec");

    ctx.register_service(order_spec)
        .expect("register order-service");
    ctx.register_service(fulfillment_spec)
        .expect("register fulfillment-service");

    let order_base_url = ctx.service_url("order-service").expect("order service URL");

    let http_client = reqwest::Client::new();
    let create_response = http_client
        .post(format!("{order_base_url}/orders"))
        .json(&sample_order("customer-ch15"))
        .send()
        .await
        .expect("POST /orders");

    assert_eq!(create_response.status(), reqwest::StatusCode::CREATED);
    let created: serde_json::Value = create_response.json().await.expect("created JSON");
    let order_id = created["id"].as_str().expect("order id").to_string();

    ctx.postgres()
        .assert_row_exists("SELECT 1 FROM orders WHERE id = $1", &[&order_id])
        .await
        .expect("order row in Postgres");

    let order_id_for_kafka = order_id.clone();
    let _kafka_observation = ctx
        .kafka()
        .consume_until(
            "orders",
            move |message| {
                message
                    .payload_json::<serde_json::Value>()
                    .map(|value| {
                        value
                            .get("aggregate_id")
                            .and_then(|id| id.as_str())
                            .is_some_and(|candidate| candidate == order_id_for_kafka.as_str())
                    })
                    .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("OrderCreated observable on Kafka");

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    ctx.mongo()
        .assert_document_exists("fulfillments", doc! { "order_id": &order_id })
        .await
        .expect("fulfillment document after Kafka consumer processing");

    let order_id_for_rabbit = order_id.clone();
    let _rabbit_observation = ctx
        .rabbit()
        .consume_until(
            "fulfillments-assert",
            move |message| {
                message
                    .payload
                    .as_ref()
                    .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
                    .and_then(|value| {
                        value
                            .get("order_id")
                            .and_then(|entry| entry.as_str())
                            .map(|candidate| candidate == order_id_for_rabbit.as_str())
                    })
                    .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("fulfillment fan-out on RabbitMQ");
}
