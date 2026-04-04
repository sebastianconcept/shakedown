//! Chapter 13: Order Service calling a **gRPC** mock upstream (Fulfillment)
//!
//! **Resources:** Postgres, Redis, Kafka + `GrpcMockService`
//! **Service:** `order-service` with `FULFILLMENT_GRPC_URI` pointing at the mock
//! **Assertions:** `MockServiceHandle::assert_path_called_times`, `assert_called_with` on protobuf body
//! **Concepts:** `prelude` registers the mock **before** listeners start; obtain `mock_service_url`,
//!   pass it through `.env` on `ServiceUnderTest`, then assert outbound gRPC from `POST /orders/:id/fulfill`
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,redis,kafka,mock-services,grpc-mock-services --test ch13_service_mock_upstream -- --ignored`

use prost::Message;
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};
use shared_grpc::{FulfillRequest, FulfillResponse, FULFILL_METHOD_PATH};

#[shakedown::test(
    resources = [Postgres, Redis, Kafka],
    prelude = {
        let fulfillment_mock = shakedown::GrpcMockService::new("fulfillment-mock")
            .on_method(FULFILL_METHOD_PATH.trim_start_matches('/'))
            .respond_proto(&FulfillResponse {
                fulfillment_id: "fulfill-ch13".to_string(),
            })
            .expect("fulfillment stub proto");
        ctx.register_grpc_mock_service("fulfillment-mock", fulfillment_mock)
            .expect("register fulfillment gRPC mock");
    }
)]
#[ignore = "requires Docker + locally-built order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,redis,kafka,mock-services,grpc-mock-services --test ch13_service_mock_upstream -- --ignored"]
async fn order_service_calls_fulfillment_mock_on_manual_fulfill(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema");

    let mock_base_url = ctx
        .mock_service_url("fulfillment-mock")
        .expect("fulfillment mock base URL");

    let order_spec = shakedown::ServiceUnderTest::docker("order-service:latest")
        .name("order-service")
        .port(3000)
        .health_check(shakedown::HealthCheck::http("/health"))
        .env("FULFILLMENT_GRPC_URI", mock_base_url)
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
        .assert_path_called_times(FULFILL_METHOD_PATH, FULFILL_METHOD_PATH, 1)
        .expect("downstream should be called exactly once for fulfill");

    mock_handle
        .assert_called_with(FULFILL_METHOD_PATH, FULFILL_METHOD_PATH, |call| {
            FulfillRequest::decode(call.request_body().as_ref())
                .map(|decoded| decoded.order_id == order_id)
                .unwrap_or(false)
        })
        .expect("fulfillment body should decode to FulfillRequest with order_id");
}
