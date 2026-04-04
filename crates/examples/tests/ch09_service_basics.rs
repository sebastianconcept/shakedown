//! Chapter 9: Order Service under test — health, injection, Postgres proof
//!
//! **Resources:** Postgres, Redis, Kafka
//! **Service:** `example-order-service` (Docker image `example-order-service:latest`)
//! **Assertions:** `service_url`, `ServiceHandle::is_healthy`, `assert_row_exists`
//! **Concepts:** `ServiceUnderTest::docker`, `HealthCheck::http`, convention env injection,
//!   seed schema before `register_service`, HTTP act → database assert
//! **Prerequisites:** `just build-example-services` (local image), Docker for infra
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,redis,kafka --test ch09_service_basics -- --ignored`

use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::test(resources = [Postgres, Redis, Kafka])]
#[ignore = "requires Docker + locally-built example-order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,redis,kafka --test ch09_service_basics -- --ignored"]
async fn order_service_postgres_row_after_create_order(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema from shared fixture");

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
        .expect("service URL after readiness");
    assert!(
        base_url.starts_with("http://127.0.0.1:"),
        "harness should expose the service on loopback for host-side HTTP clients (got {base_url})"
    );

    let handle = ctx
        .service_handle("order-service")
        .expect("service handle after registration");
    assert!(
        handle.is_healthy(),
        "order-service should pass HTTP health checks before the scenario body proceeds"
    );

    let http_client = reqwest::Client::new();
    let order_body = sample_order("customer-ch09");
    let response = http_client
        .post(format!("{base_url}/orders"))
        .json(&order_body)
        .send()
        .await
        .expect("POST /orders");

    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let body: serde_json::Value = response.json().await.expect("response JSON");
    let order_id = body["id"].as_str().expect("order id in response");

    ctx.postgres()
        .assert_row_exists(
            "SELECT 1 FROM orders WHERE id = $1 AND customer_id = $2",
            &[&order_id, &"customer-ch09"],
        )
        .await
        .expect("order row visible in Postgres after HTTP create");
}
