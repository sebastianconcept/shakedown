//! Bridge handler traits.

use async_trait::async_trait;

#[cfg(feature = "grpc-bridges")]
use super::grpc_types::{GrpcBridgeRequest, GrpcBridgeResponse};
use super::types::{BridgeContext, BridgeRequest, BridgeResponse};

/// Handles inbound HTTP requests for a shakedown bridge.
///
/// Recording and timing happen in the server wrapper; this trait stays pure and unit-testable.
#[async_trait]
pub trait HttpBridge: Send + Sync + 'static {
    /// Produce an HTTP response for the given request.
    async fn handle(&self, request: BridgeRequest, context: &BridgeContext) -> BridgeResponse;
}

/// Handles inbound unary gRPC requests for a shakedown bridge (`grpc-bridges`).
///
/// Recording and framing happen in the server wrapper; this trait stays free of transport details.
#[cfg(feature = "grpc-bridges")]
#[async_trait]
pub trait GrpcBridge: Send + Sync + 'static {
    /// Produce a gRPC response for the given request.
    async fn handle(
        &self,
        request: GrpcBridgeRequest,
        context: &BridgeContext,
    ) -> GrpcBridgeResponse;
}

#[cfg(all(test, feature = "bridges"))]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;

    use super::HttpBridge;
    use crate::bridge::types::{BridgeContext, BridgeRequest, BridgeResources, BridgeResponse};

    struct EchoBody;

    #[async_trait]
    impl HttpBridge for EchoBody {
        async fn handle(&self, request: BridgeRequest, _context: &BridgeContext) -> BridgeResponse {
            BridgeResponse::builder()
                .status(200)
                .body_bytes(request.bytes().clone())
        }
    }

    #[tokio::test]
    async fn http_bridge_trait_smoke() {
        let bridge = EchoBody;
        let resources = Arc::new(BridgeResources::default());
        let context = BridgeContext::new("scenario-a", "echo", resources);
        let request = BridgeRequest {
            method: String::from("POST"),
            path: String::from("/echo"),
            query: None,
            headers: Vec::new(),
            body: Bytes::from_static(b"ping"),
        };
        let response = bridge.handle(request, &context).await;
        assert_eq!(response.status, 200);
        assert_eq!(response.body.as_ref(), b"ping");
    }
}
