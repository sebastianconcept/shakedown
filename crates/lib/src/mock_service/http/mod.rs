//! HTTP mock service types and implementations.

mod request;
mod response;
mod response_builder;
#[cfg(feature = "mock-services")]
pub(crate) mod server;
mod service;
#[cfg(all(test, feature = "mock-services"))]
mod tests;

use async_trait::async_trait;

use crate::mock_service::types::MockServiceContext;
pub use request::HttpMockServiceRequest;
pub use response::HttpMockServiceResponse;
pub use response_builder::HttpMockServiceResponseBuilder;
pub use service::{HttpMockService, HttpMockServiceRoutePending, RouteMatch};

/// Handles inbound HTTP requests for a shakedown mock service.
///
/// Recording and timing happen in the server wrapper; this trait stays pure and unit-testable.
#[async_trait]
pub trait HttpMockServiceTrait: Send + Sync + 'static {
    /// Produce an HTTP response for the given request.
    async fn handle(
        &self,
        request: HttpMockServiceRequest,
        context: &MockServiceContext,
    ) -> HttpMockServiceResponse;
}
