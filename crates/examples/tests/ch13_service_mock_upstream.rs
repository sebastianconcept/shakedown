//! Chapter 13: Order Service calling an HTTP mock upstream (fulfillment)
//!
//! **Resources:** Postgres, Redis, Kafka + `HttpMockService`
//! **Service:** `example-order-service` with `FULFILLMENT_URL` pointing at the mock
//! **Assertions:** `MockServiceHandle::assert_path_called_times`, `assert_called_with`
//! **Concepts:** `prelude` registers the mock **before** listeners start; obtain `mock_service_url`,
//!   pass it through `.env` on `ServiceUnderTest`, then assert outbound HTTP from `POST /fulfill`
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,redis,kafka,mock-services --test ch13_service_mock_upstream -- --ignored`

use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::test(
    resources = [Postgres, Redis, Kafka],
    prelude = {
        let fulfillment_mock = shakedown::HttpMockService::new("fulfillment-mock")
            .on("POST", "/fulfill")
            .respond_json(&serde_json::json!({ "accepted": true, "reservation_id": "res-ch13" }))
            .expect("fulfillment stub JSON")
            .default_response(404);
        ctx.register_http_mock_service("fulfillment-mock", fulfillment_mock)
            .expect("register fulfillment HTTP mock");
    }
)]
#[ignore = "requires Docker + locally-built example-order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,redis,kafka,mock-services --test ch13_service_mock_upstream -- --ignored"]
async fn order_service_calls_fulfillment_mock_on_manual_fulfill(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema");

    let mock_base_url = ctx
        .mock_service_url("fulfillment-mock")
        .expect("fulfillment mock base URL");

    let order_spec = shakedown::ServiceUnderTest::docker("example-order-service:latest")
        .name("order-service")
        .port(3000)
        .health_check(shakedown::HealthCheck::http("/health"))
        .env("FULFILLMENT_URL", mock_base_url)
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("order service spec");

    ctx.register_service(order_spec)
        .expect("register order-service");

    let base_url = ctx.service_url("order-service").expect("order service URL");

    let http_client = reqwest::Client::new();
    let create_response = http_client
        .post(format!("{base_url}/orders"))
        .json(&sample_order("customer-ch13"))
        .send()
        .await
        .expect("POST /orders");

    assert_eq!(create_response.status(), reqwest::StatusCode::CREATED);
    let created: serde_json::Value = create_response.json().await.expect("created JSON");
    let order_id = created["id"].as_str().expect("order id");

    let fulfill_response = http_client
        .post(format!("{base_url}/orders/{order_id}/fulfill"))
        .send()
        .await
        .expect("POST /orders/:id/fulfill");

    assert_eq!(fulfill_response.status(), reqwest::StatusCode::OK);

    let mock_handle: &shakedown::MockServiceHandle = ctx
        .mock_service("fulfillment-mock")
        .expect("fulfillment mock handle");

    mock_handle
        .assert_path_called_times("POST", "/fulfill", 1)
        .expect("downstream should be called exactly once for fulfill");

    mock_handle
        .assert_called_with("POST", "/fulfill", |call| {
            match call.json::<serde_json::Value>() {
                Ok(value) => value
                    .get("order_id")
                    .and_then(|entry| entry.as_str())
                    .is_some_and(|id| id == order_id),
                Err(_) => false,
            }
        })
        .expect("fulfillment body should include the order_id");
}
