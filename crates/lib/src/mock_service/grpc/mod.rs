//! gRPC mock service types and implementations (`grpc-mock-services` feature).
//!
//! ## When to use [`GrpcMockService`] vs [`GrpcMockServiceTrait`]
//!
//! Prefer the declarative [`GrpcMockService`] builder (for example [`GrpcMockService::on_method`],
//! [`GrpcMockServiceMethodPending::respond_proto`], [`GrpcMockServiceMethodPending::respond_status`])
//! when unary methods are static: fixed `service/method` paths, known OK protobuf bodies or gRPC
//! status codes. You get compile-time method tables without writing a type that implements the trait.
//!
//! Implement [`GrpcMockServiceTrait`] when the stub must depend on the incoming request, keep state
//! across calls, branch on metadata or request body, or coordinate with shared infrastructure through
//! [`crate::mock_service::types::MockServiceContext`] — behavior that does not fit a fixed declarative
//! table.

mod protocol_error;
#[cfg(feature = "grpc-mock-services")]
pub(crate) mod server;
mod service;
#[cfg(test)]
mod tests;
mod types;

use async_trait::async_trait;

use super::types::MockServiceContext;
pub use service::{GrpcMockService, GrpcMockServiceMethodPending};
pub use types::{GrpcMockServiceRequest, GrpcMockServiceResponse};

/// Handles inbound unary gRPC requests for a shakedown mock service.
///
/// Recording and timing happen in the server wrapper; this trait stays pure and unit-testable.
#[async_trait]
pub trait GrpcMockServiceTrait: Send + Sync + 'static {
    /// Produce a gRPC response for the given request.
    async fn handle(
        &self,
        request: GrpcMockServiceRequest,
        context: &MockServiceContext,
    ) -> GrpcMockServiceResponse;
}
