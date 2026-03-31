//! gRPC mock service handler trait (`grpc-mock-services`).

use async_trait::async_trait;

use super::super::types::MockServiceContext;
use super::types::{GrpcMockServiceRequest, GrpcMockServiceResponse};

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
