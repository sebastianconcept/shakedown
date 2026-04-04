//! Chapter 8: HTTP and gRPC mock services with call assertions
//!
//! **Resources:** none (localhost mocks only)
//! **Assertions:** `assert_called_times`, `assert_called_with`, `assert_not_called`,
//!   `assert_path_called_times`
//! **Concepts:** `prelude = { ... }` registers mocks **before** `run_scenario` starts listeners;
//!   payment gateway (HTTP) + warehouse allocation (gRPC) + unused webhook (never called)
//! **Teaching intent:** Stub upstream payment and inventory dependencies with real wire traffic,
//!   then prove who was called — the same pattern Part II uses against `ServiceUnderTest`.
//!
//! Run locally (no Docker): `cargo test -p shakedown-examples --features mock-services,grpc-mock-services --test ch08_mock_services -- --ignored`

use http::uri::PathAndQuery;
use prost::Message;
use shakedown::MockServiceHandle;
use tonic::client::Grpc;
use tonic::transport::Endpoint;
use tonic_prost::ProstCodec;

const WAREHOUSE_ALLOC_PATH: &str = "/inventory.Warehouse/AllocatePick";

/// Unary request body for `inventory.Warehouse/AllocatePick`.
#[derive(Clone, PartialEq, prost::Message)]
struct AllocatePickRequest {
    #[prost(string, tag = "1")]
    order_id: String,
}

/// Unary response body from the warehouse mock.
#[derive(Clone, PartialEq, prost::Message)]
struct AllocatePickResponse {
    #[prost(string, tag = "1")]
    pick_ticket_id: String,
}

#[shakedown::test(prelude = {
    let payment_gateway = shakedown::HttpMockService::new("payment-gateway")
        .on("POST", "/charge")
        .respond_json(&serde_json::json!({
            "captured": true,
            "payment_reference": "pay-ch08-mock"
        }))
        .expect("payment charge json response")
        .default_response(404);
    ctx.register_http_mock_service("payment-gateway", payment_gateway)
        .expect("register payment-gateway mock");

    let unused_webhook = shakedown::HttpMockService::new("unused-settlement-webhook")
        .default_response(404);
    ctx.register_http_mock_service("unused-settlement-webhook", unused_webhook)
        .expect("register unused webhook mock (never called)");

    let warehouse = shakedown::GrpcMockService::new("warehouse")
        .on_method("inventory.Warehouse/AllocatePick")
        .respond_proto(&AllocatePickResponse {
            pick_ticket_id: "pick-ch08-1".to_string(),
        })
        .expect("allocate pick proto response");
    ctx.register_grpc_mock_service("warehouse", warehouse)
        .expect("register warehouse gRPC mock");
})]
#[ignore = "uses ignored integration pattern; run: cargo test -p shakedown-examples --features mock-services,grpc-mock-services --test ch08_mock_services -- --ignored"]
async fn http_and_grpc_mocks_assert_call_patterns(ctx: &mut shakedown::Context) {
    let http_client = reqwest::Client::new();
    let payment_base = ctx
        .mock_service_url("payment-gateway")
        .expect("payment mock base URL");

    let charge = http_client
        .post(format!("{payment_base}/charge"))
        .json(&serde_json::json!({
            "order_id": "order-ch08-1",
            "amount_cents": 9999,
        }))
        .send()
        .await
        .expect("POST /charge to payment mock");

    assert_eq!(charge.status(), reqwest::StatusCode::OK);

    let grpc_base = ctx
        .mock_service_url("warehouse")
        .expect("warehouse gRPC mock base URL");
    let origin_uri: http::Uri = grpc_base.parse().expect("gRPC mock URL parses");
    let channel = Endpoint::from_shared(grpc_base)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect channel");

    let mut grpc_client = Grpc::with_origin(channel, origin_uri);
    grpc_client.ready().await.expect("grpc ready");
    let path = PathAndQuery::from_static(WAREHOUSE_ALLOC_PATH);
    let codec = ProstCodec::<AllocatePickRequest, AllocatePickResponse>::default();

    let response = grpc_client
        .unary(
            tonic::Request::new(AllocatePickRequest {
                order_id: "order-ch08-1".to_string(),
            }),
            path,
            codec,
        )
        .await
        .expect("unary AllocatePick");

    assert_eq!(response.into_inner().pick_ticket_id, "pick-ch08-1");

    let payment_handle: &MockServiceHandle = ctx
        .mock_service("payment-gateway")
        .expect("payment mock handle");

    payment_handle
        .assert_called_times(1)
        .expect("single charge attempt");

    payment_handle
        .assert_called_with("POST", "/charge", |call| {
            match call.json::<serde_json::Value>() {
                Ok(value) => value.get("order_id").and_then(|v| v.as_str()) == Some("order-ch08-1"),
                Err(_) => false,
            }
        })
        .expect("charge body mentions the order id");

    let unused_handle: &MockServiceHandle = ctx
        .mock_service("unused-settlement-webhook")
        .expect("unused webhook handle");

    unused_handle
        .assert_not_called()
        .expect("settlement webhook should stay silent when not wired");

    let warehouse_handle: &MockServiceHandle = ctx
        .mock_service("warehouse")
        .expect("warehouse mock handle");

    warehouse_handle
        .assert_path_called_times(WAREHOUSE_ALLOC_PATH, WAREHOUSE_ALLOC_PATH, 1)
        .expect("exactly one AllocatePick");

    warehouse_handle
        .assert_called_with(WAREHOUSE_ALLOC_PATH, WAREHOUSE_ALLOC_PATH, |call| {
            AllocatePickRequest::decode(call.request_body().as_ref())
                .map(|decoded| decoded.order_id == "order-ch08-1")
                .unwrap_or(false)
        })
        .expect("recorded AllocatePick request carries the order id");
}
