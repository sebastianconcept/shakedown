//! Service virtualization for integration tests — HTTP, gRPC, and (with **`tcp-mock-services`** / **`udp-mock-services`**) TCP/UDP mock services.
//!
//! A service under test often calls external APIs (payment processors, inventory
//! services, notification gateways) that are unavailable in a test environment. Replacing those
//! dependencies with trait mocks swaps serialization and real HTTP error handling for fake ones,
//! masking the bugs you most want to catch. Shakedown mock services run as real local servers on real
//! network ports so your service's HTTP client hits the same code path it would in production.
//!
//! Create an [`HttpMockService`] (or [`GrpcMockService`] with the `grpc`
//! feature, [`TcpMockService`] / [`TcpMockCustom`] with **`tcp-mock-services`**, or [`UdpMockService`] with **`udp-mock-services`**), register scenario-specific routes or handlers, then inject its URL into the service
//! container via [`crate::service::EnvMapping`]. The harness starts and stops the mock server as
//! part of the normal lifecycle. After the scenario, assert call counts and request bodies through
//! the [`MockServiceHandle`].
//!
//! Implement [`HttpMockServiceTrait`] / [`GrpcMockServiceTrait`] / [`TcpMockServiceTrait`] / [`UdpMockServiceTrait`] for stateful or branching
//! per-request logic.
//!
//! Enabled with the **`mock-services`** feature (default). Add **`grpc`** for gRPC
//! support (`tonic` + `prost`). Add **`tcp-mock-services`** for TCP mocks (`TcpCodec` + optional TLS).
//! Add **`udp-mock-services`** for UDP datagram mocks.
//!
//! # Book reference
//!
//! Part I, *Chapter 8: Mock Services* — introduces `HttpMockService`, route registration,
//! per-scenario response overrides, and call-count assertions.

#[cfg(all(feature = "mock-services", feature = "grpc"))]
pub(crate) mod grpc;
#[cfg(feature = "mock-services")]
pub(crate) mod handle;
#[cfg(feature = "mock-services")]
pub(crate) mod http;
#[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
pub mod tcp;
#[cfg(feature = "mock-services")]
pub(crate) mod types;
#[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
pub mod udp;

#[cfg(feature = "mock-services")]
pub use handle::{MockServiceHandle, RecordedCall, RecordedExchange};

pub use http::{
    HttpMockService, HttpMockServiceRequest, HttpMockServiceResponse,
    HttpMockServiceResponseBuilder, HttpMockServiceRoutePending, HttpMockStatus, Method,
    RouteMatch, StatusCode,
};

#[cfg(all(feature = "mock-services", feature = "grpc"))]
pub use grpc::{
    GrpcMockService, GrpcMockServiceMethodPending, GrpcMockServiceRequest, GrpcMockServiceResponse,
};

#[cfg(all(feature = "mock-services", feature = "grpc"))]
pub use grpc::GrpcMockServiceTrait;
#[cfg(feature = "mock-services")]
pub use http::HttpMockServiceTrait;
#[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
pub use tcp::{
    DeclarativeTcpCodec, DeclarativeTcpRequest, TcpCodec, TcpMockCustom, TcpMockService,
    TcpMockServiceRegistration, TcpMockServiceRoutePending, TcpMockServiceTrait, TlsAcceptor,
};
#[cfg(feature = "mock-services")]
pub use types::{MockServiceContext, MockServiceError, MockServiceResources};
#[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
pub use udp::{UdpMockService, UdpMockServiceRegistration, UdpMockServiceTrait};
