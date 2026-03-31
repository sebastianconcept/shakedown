//! Declarative gRPC mock service (`GrpcMockService`) implementing [`GrpcMockServiceTrait`](super::super::GrpcMockServiceTrait).

use async_trait::async_trait;
use bytes::Bytes;
use tonic::Code;

use super::r#trait::GrpcMockServiceTrait;
use super::super::types::{MockServiceContext, MockServiceError};
use super::types::{GrpcMockServiceRequest, GrpcMockServiceResponse};

#[derive(Debug, Clone)]
enum GrpcMockRoute {
    OkBody(Bytes),
    Status(Code, String),
}

/// Builder for method → response mappings; implements [`GrpcMockServiceTrait`].
#[derive(Debug, Clone)]
pub struct GrpcMockService {
    name: String,
    routes: Vec<(String, GrpcMockRoute)>,
}

/// Fluent step after [`GrpcMockService::on_method`].
#[derive(Debug)]
pub struct GrpcMockServiceMethodPending {
    mock_service: GrpcMockService,
    method_path: String,
}

impl GrpcMockService {
    /// Starts a gRPC mock service builder. Unmatched methods return **UNIMPLEMENTED**.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            routes: Vec::new(),
        }
    }

    /// Logical mock service name (for [`MockServiceContext::mock_service_name`] and diagnostics).
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Register a route on full gRPC method path `service/method` (no leading slash), e.g.
    /// `inventory.InventoryService/CheckStock`.
    pub fn on_method(self, method_path: impl Into<String>) -> GrpcMockServiceMethodPending {
        GrpcMockServiceMethodPending {
            mock_service: self,
            method_path: method_path.into(),
        }
    }

    fn match_route(&self, request: &GrpcMockServiceRequest) -> Option<&GrpcMockRoute> {
        let key = format!("{}/{}", request.service, request.method);
        self.routes
            .iter()
            .find(|(registered_path, _)| registered_path == &key)
            .map(|(_, route)| route)
    }
}

impl GrpcMockServiceMethodPending {
    /// Respond with a serialized protobuf message body.
    pub fn respond_proto(
        mut self,
        message: &impl prost::Message,
    ) -> Result<GrpcMockService, MockServiceError> {
        let response = GrpcMockServiceResponse::ok_proto(message)?;
        self.mock_service
            .routes
            .push((self.method_path, GrpcMockRoute::OkBody(response.body)));
        Ok(self.mock_service)
    }

    /// Respond with a non-OK gRPC status.
    pub fn respond_status(mut self, code: Code, message: impl Into<String>) -> GrpcMockService {
        self.mock_service.routes.push((
            self.method_path,
            GrpcMockRoute::Status(code, message.into()),
        ));
        self.mock_service
    }
}

#[async_trait]
impl GrpcMockServiceTrait for GrpcMockService {
    async fn handle(
        &self,
        request: GrpcMockServiceRequest,
        _context: &MockServiceContext,
    ) -> GrpcMockServiceResponse {
        let Some(route) = self.match_route(&request) else {
            return GrpcMockServiceResponse::status(Code::Unimplemented, "method not implemented");
        };
        match route {
            GrpcMockRoute::OkBody(body) => GrpcMockServiceResponse::ok(body.clone()),
            GrpcMockRoute::Status(code, message) => {
                GrpcMockServiceResponse::status(*code, message.as_str())
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
        // Given a mock service that responds on `test.Service/Do` with a fixed protobuf payload
        let mock_service = GrpcMockService::new("inventory")
            .on_method("test.Service/Do")
            .respond_proto(&SampleMessage { value: 7 })
            .expect("respond_proto");

        let resources = Arc::new(crate::mock_service::types::MockServiceResources::default());
        let context = MockServiceContext::new("s1", "inventory", resources);
        let request = GrpcMockServiceRequest {
            service: String::from("test.Service"),
            method: String::from("Do"),
            metadata: Vec::new(),
            body: Bytes::new(),
        };
        // When the mock service handles a matching request
        let response = mock_service.handle(request, &context).await;
        // Then the status is OK and the body round-trips through prost decode (test-only)
        assert_eq!(response.status_code, Code::Ok);
        let decoded = SampleMessage::decode(response.body.as_ref())
            .expect("decode response body produced by mock service");
        assert_eq!(decoded.value, 7);
    }

    #[tokio::test]
    async fn stub_unimplemented_when_unmatched() {
        // Given a mock service with no routes registered
        let mock_service = GrpcMockService::new("inventory");
        let resources = Arc::new(crate::mock_service::types::MockServiceResources::default());
        let context = MockServiceContext::new("s1", "inventory", resources);
        let request = GrpcMockServiceRequest {
            service: String::from("test.Service"),
            method: String::from("Missing"),
            metadata: Vec::new(),
            body: Bytes::new(),
        };
        // When the method does not match any route
        let response = mock_service.handle(request, &context).await;
        // Then the mock service returns UNIMPLEMENTED
        assert_eq!(response.status_code, Code::Unimplemented);
    }
}
