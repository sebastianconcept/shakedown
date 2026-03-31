//! HTTP mock service handler trait.

use async_trait::async_trait;

use super::super::types::MockServiceContext;
use super::request::HttpMockServiceRequest;
use super::response::HttpMockServiceResponse;

/// Handles inbound HTTP requests for a shakedown mock service.
///
/// Recording and timing happen in the server wrapper; this trait stays pure and unit-testable.
#[async_trait]
pub trait HttpMockServiceTrait: Send + Sync + 'static {
    /// Produce an HTTP response for the given request.
    async fn handle(&self, request: HttpMockServiceRequest, context: &MockServiceContext) -> HttpMockServiceResponse;
}
