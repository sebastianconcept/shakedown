//! Declarative gRPC mock service (`GrpcMockService`) implementing [`GrpcMockServiceTrait`](super::super::GrpcMockServiceTrait).
//!
//! Use it in integration tests or scenarios to stand in for a real gRPC backend with fixed unary
//! responses (no real server logic), so callers see predictable protobuf payloads and status codes.
//!
//! Example:
//!
//! ```ignore
//! use std::sync::Arc;
//!
//! use shakedown::mock_service::{
//!     GrpcMockService, GrpcMockServiceRequest, GrpcMockServiceTrait, MockServiceContext,
//!     MockServiceResources,
//! };
//!
//! # #[tokio::main]
//! # async fn main() {
//! let mock = GrpcMockService::new("inventory")
//!     .on_method("test.Service/Do")
//!     .respond_status(tonic::Code::Unavailable, "try again later");
//!
//! let ctx = MockServiceContext::new(
//!     "scenario-1",
//!     "inventory",
//!     Arc::new(MockServiceResources::default()),
//! );
//!
//! let req = GrpcMockServiceRequest {
//!     service: "test.Service".to_string(),
//!     method: "Do".to_string(),
//!     metadata: vec![],
//!     body: bytes::Bytes::new(),
//! };
//!
//! let resp = mock.handle(req, &ctx).await;
//! assert_eq!(resp.status_code, tonic::Code::Unavailable);
//! # }
//! ```

use async_trait::async_trait;
use bytes::Bytes;
use tonic::Code;

use super::super::types::{MockServiceContext, MockServiceError};
use super::types::{GrpcMockServiceRequest, GrpcMockServiceResponse};
use super::GrpcMockServiceTrait;

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
