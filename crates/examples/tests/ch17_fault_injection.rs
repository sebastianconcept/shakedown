//! Chapter 17: Fault injection on the Kafka path (Toxiproxy)
//!
//! **Resources:** Postgres, Redis, Kafka, Toxiproxy
//! **Service:** `example-order-service`
//! **Assertions:** Postgres row after HTTP create; `consume_until` after proxy reset
//! **Concepts:** `#[shakedown::faulty_test]`, `FaultHandle`, `add_toxic` (latency on `shakedown_kafka`),
//!   graceful degradation (HTTP + DB succeed while Kafka produce times out), `reset_proxy`
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! The Order Service treats Kafka produce errors as non-fatal (`tracing::warn`); with extreme
//! downstream latency the delivery future exceeds `message.timeout.ms` (5s) while the API still
//! returns `201 Created` and persists the order — that is the degradation we assert here. After
//! `reset_proxy`, a second order proves the publish path works again.
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,redis,kafka,fault-injection --test ch17_fault_injection -- --ignored`

use shakedown::{StreamDirection, Toxic, ToxicKind};
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::faulty_test(resources = [Postgres, Redis, Kafka, Toxiproxy])]
#[ignore = "requires Docker + locally-built example-order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,redis,kafka,fault-injection --test ch17_fault_injection -- --ignored"]
async fn kafka_latency_toxic_order_api_still_created_then_path_recovers(
    ctx: &mut shakedown::Context,
) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema");

    let order_spec = shakedown::ServiceUnderTest::docker("example-order-service:latest")
        .name("order-service")
        .port(3000)
        .health_check(shakedown::HealthCheck::http("/health"))
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("order service spec");

    ctx.register_service(order_spec)
        .expect("register order-service");

    let base_url = ctx
        .service_url("order-service")
        .expect("order service URL after readiness");

    let latency_toxic = Toxic {
        kind: ToxicKind::Latency {
            latency: 8_000,
            jitter: 0,
        },
        name: "ch17_kafka_downstream_latency".to_string(),
        toxicity: 1.0,
        direction: StreamDirection::Downstream,
    };

    ctx.fault_handle_mut()
        .expect("FaultHandle after Kafka + Toxiproxy provision")
        .add_toxic("shakedown_kafka", &latency_toxic)
        .await
        .expect("add latency toxic on kafka proxy");

    let http_client = reqwest::Client::new();
    let degraded_response = http_client
        .post(format!("{base_url}/orders"))
        .json(&sample_order("customer-ch17-degraded"))
        .send()
        .await
        .expect("POST /orders under kafka toxic");

    assert_eq!(
        degraded_response.status(),
        reqwest::StatusCode::CREATED,
        "order API should stay available while Kafka delivery is impaired (graceful degradation)"
    );
    let degraded_body: serde_json::Value = degraded_response.json().await.expect("response JSON");
    let order_id_degraded = degraded_body["id"]
        .as_str()
        .expect("order id under degradation")
        .to_string();

    ctx.postgres()
        .assert_row_exists(
            "SELECT 1 FROM orders WHERE id = $1 AND customer_id = $2",
            &[&order_id_degraded, &"customer-ch17-degraded"],
        )
        .await
        .expect("order row committed despite kafka timeout");

    ctx.fault_handle_mut()
        .expect("FaultHandle for reset")
        .reset_proxy("shakedown_kafka")
        .await
        .expect("reset kafka proxy clears toxics");

    let recovered_response = http_client
        .post(format!("{base_url}/orders"))
        .json(&sample_order("customer-ch17-recovered"))
        .send()
        .await
        .expect("POST /orders after proxy reset");

    assert_eq!(recovered_response.status(), reqwest::StatusCode::CREATED);
    let recovered_body: serde_json::Value = recovered_response.json().await.expect("response JSON");
    let order_id_recovered = recovered_body["id"]
        .as_str()
        .expect("order id after recovery")
        .to_string();

    ctx.postgres()
        .assert_row_exists(
            "SELECT 1 FROM orders WHERE id = $1 AND customer_id = $2",
            &[&order_id_recovered, &"customer-ch17-recovered"],
        )
        .await
        .expect("second order row in Postgres");

    let order_id_for_consume = order_id_recovered.clone();
    ctx.kafka()
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
        .expect("Kafka should deliver OrderCreated after toxics cleared");
}
