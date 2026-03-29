//! HTTP and gRPC service bridges (trait-based stub servers).
//!
//! Enabled with the **`bridges`** Cargo feature (default). Optional **`grpc-bridges`** pulls in
//! `tonic` and `prost` for gRPC bridge servers.

#[cfg(feature = "bridges")]
mod handle;
#[cfg(feature = "bridges")]
pub(crate) mod server;
#[cfg(feature = "bridges")]
mod stub;
#[cfg(feature = "bridges")]
mod traits;
#[cfg(feature = "bridges")]
mod types;

#[cfg(all(feature = "bridges", feature = "grpc-bridges"))]
pub(crate) mod grpc_server;
#[cfg(all(feature = "bridges", feature = "grpc-bridges"))]
mod grpc_stub;
#[cfg(all(feature = "bridges", feature = "grpc-bridges"))]
mod grpc_types;

#[cfg(all(feature = "bridges", feature = "grpc-bridges"))]
pub use grpc_stub::{GrpcStubBridge, GrpcStubMethodPending};
#[cfg(all(feature = "bridges", feature = "grpc-bridges"))]
pub use grpc_types::{GrpcBridgeRequest, GrpcBridgeResponse};
#[cfg(feature = "bridges")]
pub use handle::{BridgeHandle, RecordedCall, RecordedExchange};
#[cfg(feature = "bridges")]
pub use stub::{RouteMatch, StubBridge, StubRoutePending};
#[cfg(all(feature = "bridges", feature = "grpc-bridges"))]
pub use traits::GrpcBridge;
#[cfg(feature = "bridges")]
pub use traits::HttpBridge;
#[cfg(feature = "bridges")]
pub use types::{
    BridgeContext, BridgeError, BridgeRequest, BridgeResources, BridgeResponse,
    BridgeResponseBuilder,
};
