//! Declarative gRPC stub bridge (`GrpcStubBridge`) implementing [`GrpcBridge`](super::traits::GrpcBridge).

use async_trait::async_trait;
use bytes::Bytes;
use tonic::Code;

use super::grpc_types::{GrpcBridgeRequest, GrpcBridgeResponse};
use super::traits::GrpcBridge;
use super::types::{BridgeContext, BridgeError};

#[derive(Debug, Clone)]
enum GrpcStubRoute {
    OkBody(Bytes),
    Status(Code, String),
}

/// Builder for method → response mappings; implements [`GrpcBridge`].
#[derive(Debug, Clone)]
pub struct GrpcStubBridge {
    name: String,
    routes: Vec<(String, GrpcStubRoute)>,
}

/// Fluent step after [`GrpcStubBridge::on_method`].
#[derive(Debug)]
pub struct GrpcStubMethodPending {
    stub: GrpcStubBridge,
    method_path: String,
}

impl GrpcStubBridge {
    /// Starts a gRPC stub bridge builder. Unmatched methods return **UNIMPLEMENTED**.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            routes: Vec::new(),
        }
    }

    /// Logical bridge name (for [`BridgeContext::bridge_name`] and diagnostics).
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Register a route on full gRPC method path `service/method` (no leading slash), e.g.
    /// `inventory.InventoryService/CheckStock`.
    pub fn on_method(self, method_path: impl Into<String>) -> GrpcStubMethodPending {
        GrpcStubMethodPending {
            stub: self,
            method_path: method_path.into(),
        }
    }

    fn match_route(&self, request: &GrpcBridgeRequest) -> Option<&GrpcStubRoute> {
        let key = format!("{}/{}", request.service, request.method);
        self.routes
            .iter()
            .find(|(registered_path, _)| registered_path == &key)
            .map(|(_, route)| route)
    }
}

impl GrpcStubMethodPending {
    /// Respond with a serialized protobuf message body.
    pub fn respond_proto(
        mut self,
        message: &impl prost::Message,
    ) -> Result<GrpcStubBridge, BridgeError> {
        let response = GrpcBridgeResponse::ok_proto(message)?;
        self.stub
            .routes
            .push((self.method_path, GrpcStubRoute::OkBody(response.body)));
        Ok(self.stub)
    }

    /// Respond with a non-OK gRPC status.
    pub fn respond_status(mut self, code: Code, message: impl Into<String>) -> GrpcStubBridge {
        self.stub.routes.push((
            self.method_path,
            GrpcStubRoute::Status(code, message.into()),
        ));
        self.stub
    }
}

#[async_trait]
impl GrpcBridge for GrpcStubBridge {
    async fn handle(
        &self,
        request: GrpcBridgeRequest,
        _context: &BridgeContext,
    ) -> GrpcBridgeResponse {
        let Some(route) = self.match_route(&request) else {
            return GrpcBridgeResponse::status(Code::Unimplemented, "method not implemented");
        };
        match route {
            GrpcStubRoute::OkBody(body) => GrpcBridgeResponse::ok(body.clone()),
            GrpcStubRoute::Status(code, message) => {
                GrpcBridgeResponse::status(*code, message.as_str())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use prost::Message;

    #[derive(Clone, PartialEq, prost::Message)]
    struct SampleMessage {
        #[prost(int32, tag = "1")]
        pub value: i32,
    }

    #[tokio::test]
    async fn stub_matches_registered_method() {
        // Given a stub that responds on `test.Service/Do` with a fixed protobuf payload
        let stub = GrpcStubBridge::new("inventory")
            .on_method("test.Service/Do")
            .respond_proto(&SampleMessage { value: 7 })
            .expect("respond_proto");

        let resources = Arc::new(crate::bridge::types::BridgeResources::default());
        let context = BridgeContext::new("s1", "inventory", resources);
        let request = GrpcBridgeRequest {
            service: String::from("test.Service"),
            method: String::from("Do"),
            metadata: Vec::new(),
            body: Bytes::new(),
        };
        // When the bridge handles a matching request
        let response = stub.handle(request, &context).await;
        // Then the status is OK and the body round-trips through prost decode (test-only)
        assert_eq!(response.status_code, Code::Ok);
        let decoded = SampleMessage::decode(response.body.as_ref())
            .expect("decode response body produced by stub");
        assert_eq!(decoded.value, 7);
    }

    #[tokio::test]
    async fn stub_unimplemented_when_unmatched() {
        // Given a stub with no routes registered
        let stub = GrpcStubBridge::new("inventory");
        let resources = Arc::new(crate::bridge::types::BridgeResources::default());
        let context = BridgeContext::new("s1", "inventory", resources);
        let request = GrpcBridgeRequest {
            service: String::from("test.Service"),
            method: String::from("Missing"),
            metadata: Vec::new(),
            body: Bytes::new(),
        };
        // When the method does not match any route
        let response = stub.handle(request, &context).await;
        // Then the bridge returns UNIMPLEMENTED
        assert_eq!(response.status_code, Code::Unimplemented);
    }
}
