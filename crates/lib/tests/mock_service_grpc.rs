//! gRPC bridge + [`run_scenario`] integration (`grpc-bridges` feature).
//!
//! Reference scenario: register a [`GrpcMockService`], issue a real unary gRPC call via **tonic** and
//! **tonic-prost** using hand-written [`prost::Message`] types (no `.proto` / build script), assert
//! on [`shakedown::MockServiceHandle`] recordings. Localhost only; no Docker.

use http::uri::PathAndQuery;
use prost::Message;
use shakedown::{run_scenario, Context, EventLog, GrpcMockService, MockServiceHandle};
use tonic::client::Grpc;
use tonic::transport::Endpoint;
use tonic_prost::ProstCodec;

/// Minimal request type for `echo.EchoService/SayHello` (field `1` = name).
#[derive(Clone, PartialEq, prost::Message)]
struct SayHelloRequest {
    #[prost(string, tag = "1")]
    name: String,
}

/// Minimal response type for `echo.EchoService/SayHello` (field `1` = message).
#[derive(Clone, PartialEq, prost::Message)]
struct SayHelloResponse {
    #[prost(string, tag = "1")]
    message: String,
}

const GRPC_METHOD_PATH: &str = "/echo.EchoService/SayHello";

#[tokio::test]
async fn grpc_stub_bridge_unary_register_client_assert_recorded_call() {
    // **Given** a [`GrpcMockService`] that answers `SayHello` with a fixed protobuf payload.
    let stub = GrpcMockService::new("echo")
        .on_method("echo.EchoService/SayHello")
        .respond_proto(&SayHelloResponse {
            message: String::from("hello alice"),
        })
        .expect("respond_proto");

    let mut ctx = Context::new();
    ctx.register_grpc_bridge("echo", stub)
        .expect("register_grpc_bridge");

    let mut event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "grpc_stub_bridge_unary_register_client_assert_recorded_call",
        |context| {
            Box::pin(async move {
                let base_url = context.mock_service_url("echo").expect("echo bridge url");
                let origin_uri: http::Uri = base_url.parse().expect("bridge url parses as URI");
                let channel = Endpoint::from_shared(base_url)
                    .expect("valid endpoint")
                    .connect()
                    .await
                    .expect("channel connect");

                let mut grpc_client = Grpc::with_origin(channel, origin_uri);
                grpc_client.ready().await.expect("grpc channel ready");
                let path = PathAndQuery::from_static("/echo.EchoService/SayHello");
                let codec = ProstCodec::<SayHelloRequest, SayHelloResponse>::default();

                // **When** the scenario sends a unary gRPC request with `name = "alice"`.
                let response = grpc_client
                    .unary(
                        tonic::Request::new(SayHelloRequest {
                            name: String::from("alice"),
                        }),
                        path,
                        codec,
                    )
                    .await
                    .expect("unary rpc");

                assert_eq!(response.into_inner().message, "hello alice");

                let handle: &MockServiceHandle =
                    context.mock_service("echo").expect("echo bridge handle");

                // **Then** one call is recorded; harness stores the full gRPC path on both
                // `method` and `path`, and the request body round-trips through prost.
                handle
                    .assert_path_called_times(GRPC_METHOD_PATH, GRPC_METHOD_PATH, 1)
                    .expect("exactly one SayHello");
                handle
                    .assert_called_with(GRPC_METHOD_PATH, GRPC_METHOD_PATH, |call| {
                        SayHelloRequest::decode(call.request_body().as_ref())
                            .map(|decoded| decoded.name == "alice")
                            .unwrap_or(false)
                    })
                    .expect("recorded request body decodes to SayHelloRequest { name: alice }");
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");

    let messages: Vec<String> = event_log
        .events()
        .map(|event| event.message.clone())
        .collect();
    assert!(
        messages
            .iter()
            .any(|message| message.contains("bridge 'echo' started on")),
        "start event missing: {messages:?}"
    );
    assert!(
        messages.iter().any(|message| {
            message.contains("bridge 'echo' stopped") && message.contains("calls recorded")
        }),
        "stop event missing: {messages:?}"
    );
}
