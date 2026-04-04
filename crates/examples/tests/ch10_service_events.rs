//! Chapter 10: Kafka events emitted by the Order Service
//!
//! **Resources:** Postgres, Redis, Kafka
//! **Service:** `order-service`
//! **Assertions:** `consume_until` on logical topic `orders`, payload contains `aggregate_id`
//! **Concepts:** same `ServiceUnderTest` setup as Chapter 9; assert async messaging from a real
//!   service using harness Kafka helpers (logical topic names, scenario namespacing)
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,redis,kafka --test ch10_service_events -- --ignored`

use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::test(resources = [Postgres, Redis, Kafka])]
#[ignore = "requires Docker + locally-built order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,redis,kafka --test ch10_service_events -- --ignored"]
async fn order_service_kafka_order_created_event(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema");

    let order_spec = shakedown::ServiceUnderTest::docker("order-service:latest")
        .name("order-service")
        .port(3000)
        .health_check(shakedown::HealthCheck::http("/health"))
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("order service spec");

    ctx.register_service(order_spec)
        .expect("register order-service");

    let base_url = ctx.service_url("order-service").expect("order service URL");
    let handle = ctx
        .service_handle("order-service")
        .expect("order service handle");
    assert!(handle.is_healthy());

    let http_client = reqwest::Client::new();
    let response = http_client
        .post(format!("{base_url}/orders"))
        .json(&sample_order("customer-ch10"))
        .send()
        .await
        .expect("POST /orders");

    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let body: serde_json::Value = response.json().await.expect("response JSON");
    let order_id = body["id"].as_str().expect("order id").to_string();

    let order_id_for_consume = order_id.clone();
    let consumed = ctx
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
                            .is_some_and(|candidate| candidate == order_id_for_consume.as_str())
                    })
                    .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("consume_until should observe OrderCreated on Kafka");

    let payload: serde_json::Value = consumed
        .payload_json()
        .expect("Kafka payload should deserialize as JSON");
    assert_eq!(
        payload.get("aggregate_id").and_then(|value| value.as_str()),
        Some(order_id.as_str()),
        "published payload should carry the new order id"
    );
}
