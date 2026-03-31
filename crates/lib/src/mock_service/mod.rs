//! Service virtualization for integration tests.
//!
//! Mock services act as controllable stand-ins for downstream HTTP or gRPC services. Each mock
//! service runs as a local server, gets its URL injected into service containers via
//! [`EnvMapping`](crate::service::EnvMapping), and records all calls for post-scenario assertions.
//!
//! Use [`HttpMockService`] / [`GrpcMockService`] for quick predefined responses, or implement
//! [`HttpMockServiceTrait`] / [`GrpcMockServiceTrait`] for dynamic behavior.
//!
//! Enabled with the **`mock-services`** feature (default). Add **`grpc-mock-services`** for gRPC
//! support (`tonic` + `prost`).

#[cfg(feature = "mock-services")]
pub(crate) mod handle;
#[cfg(feature = "mock-services")]
pub(crate) mod http;
#[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
pub(crate) mod grpc;
#[cfg(feature = "mock-services")]
pub(crate) mod types;

#[cfg(feature = "mock-services")]
pub use handle::{MockServiceHandle, RecordedCall, RecordedExchange};

pub use http::{HttpMockService, HttpMockServiceRequest, HttpMockServiceResponse, HttpMockServiceResponseBuilder, HttpMockServiceRoutePending, RouteMatch};

#[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
pub use grpc::{GrpcMockService, GrpcMockServiceMethodPending, GrpcMockServiceRequest, GrpcMockServiceResponse};

#[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
pub use grpc::GrpcMockServiceTrait;
#[cfg(feature = "mock-services")]
pub use http::HttpMockServiceTrait;
#[cfg(feature = "mock-services")]
pub use types::{MockServiceContext, MockServiceError, MockServiceResources};
