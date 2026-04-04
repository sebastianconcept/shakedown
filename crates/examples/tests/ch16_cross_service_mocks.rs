//! Chapter 16: Two services + external payment mock
//!
//! **Resources:** Postgres, Redis, Kafka, MongoDB + `HttpMockService`
//! **Services:** Order Service, Fulfillment Service, `payment-mock` (HTTP)
//! **Assertions:** `MockServiceHandle::assert_path_called_times`, Postgres row, MongoDB document
//! **Concepts:** `prelude` registers the mock before listeners start; `EnvMapping::mock_service`
//!   maps the stub to `PAYMENT_GATEWAY_URL`; same `FULFILLMENT_GRPC_URI` / named-port ordering as ch14
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,mongodb,kafka,redis,mock-services --test ch16_cross_service_mocks -- --ignored`

use mongodb::bson::doc;
use shakedown::EnvMapping;
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::test(
    resources = [Postgres, Redis, Kafka, Mongo],
    prelude = {
        let payment_mock = shakedown::HttpMockService::new("payment-mock")
            .on("POST", "/charge")
            .respond_json(&serde_json::json!({ "status": "captured", "reference": "pay-ch16" }))
            .expect("payment stub JSON")
            .default_response(404);
        ctx.register_http_mock_service("payment-mock", payment_mock)
            .expect("register payment-gateway mock");
    }
)]
#[ignore = "requires Docker + both example service images; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,mongodb,kafka,redis,mock-services --test ch16_cross_service_mocks -- --ignored"]
async fn fulfillment_calls_payment_mock_during_multi_service_fulfill(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema");

    let fulfillment_spec = shakedown::ServiceUnderTest::docker("fulfillment-service:latest")
        .name("fulfillment-service")
        .port(3001)
        .port_named("grpc", 50051)
        .health_check(shakedown::HealthCheck::http("/health"))
        .env_mapping(EnvMapping::mock_service(
            "payment-mock",
            "PAYMENT_GATEWAY_URL",
        ))
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("fulfillment service spec");

    ctx.register_service(fulfillment_spec)
        .expect("register fulfillment-service");

    let order_spec = shakedown::ServiceUnderTest::docker("order-service:latest")
        .name("order-service")
        .port(3000)
        .health_check(shakedown::HealthCheck::http("/health"))
        .depends_on("fulfillment-service")
        .env_mapping(EnvMapping::service_named_port(
            "fulfillment-service",
            "grpc",
            "FULFILLMENT_GRPC_URI",
        ))
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("order service spec");

    ctx.register_service(order_spec)
        .expect("register order-service");

    let order_base_url = ctx.service_url("order-service").expect("order service URL");

    let http_client = reqwest::Client::new();
    let create_response = http_client
        .post(format!("{order_base_url}/orders"))
        .json(&sample_order("customer-ch16"))
        .send()
        .await
        .expect("POST /orders");

    assert_eq!(create_response.status(), reqwest::StatusCode::CREATED);
    let created: serde_json::Value = create_response.json().await.expect("created JSON");
    let order_id = created["id"].as_str().expect("order id");

    let fulfill_response = http_client
        .post(format!("{order_base_url}/orders/{order_id}/fulfill"))
        .send()
        .await
        .expect("POST /orders/:id/fulfill");

    assert_eq!(fulfill_response.status(), reqwest::StatusCode::OK);

    ctx.postgres()
        .assert_row_exists(
            "SELECT 1 FROM orders WHERE id = $1 AND customer_id = $2",
            &[&order_id, &"customer-ch16"],
        )
        .await
        .expect("order row still present in Postgres");

    ctx.mongo()
        .assert_document_exists("fulfillments", doc! { "order_id": order_id })
        .await
        .expect("fulfillment persisted in MongoDB");

    let payment_handle: &shakedown::MockServiceHandle = ctx
        .mock_service("payment-mock")
        .expect("payment mock handle");

    payment_handle
        .assert_path_called_times("POST", "/charge", 1)
        .expect("fulfillment-service should charge via payment gateway once per fulfill");
}
