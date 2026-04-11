//! HTTP mock service types and implementations.
//!
//! ## When to use [`HttpMockService`] vs [`HttpMockServiceTrait`]
//!
//! Prefer the declarative [`HttpMockService`] builder (for example [`HttpMockService::on`], [`HttpMockServiceRoutePending::respond_with`], [`HttpMockServiceRoutePending::respond_with_headers`], [`HttpMockServiceRoutePending::respond_json`])
//! when routes are static: fixed methods and paths, known status codes, JSON or byte bodies, and
//! extra headers. You get compile-time route tables without writing a type that implements the trait.
//!
//! Implement [`HttpMockServiceTrait`] when the stub must depend on the incoming request, keep state
//! across calls, branch on headers or query shape, or coordinate with shared infrastructure through
//! [`crate::mock_service::types::MockServiceContext`] — behavior that does not fit a fixed declarative
//! table.

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
