//! gRPC mock service types and implementations (`grpc-mock-services` feature).

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

/// Handles inbound unary gRPC requests for a shakedown mock service (`grpc-mock-services`).
///
/// Recording and framing happen in the server wrapper; this trait stays free of transport details.
#[async_trait]
pub trait GrpcMockServiceTrait: Send + Sync + 'static {
    /// Produce a gRPC response for the given request.
    async fn handle(
        &self,
        request: GrpcMockServiceRequest,
        context: &MockServiceContext,
    ) -> GrpcMockServiceResponse;
}
