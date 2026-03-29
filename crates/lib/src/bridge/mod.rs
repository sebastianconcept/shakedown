//! HTTP and gRPC service bridges (trait-based stub servers).
//!
//! Enabled with the **`bridges`** Cargo feature (default). Optional **`grpc-bridges`** pulls in
//! `tonic` and `prost` for gRPC bridge work in later milestones.

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

#[cfg(feature = "bridges")]
pub use handle::{BridgeHandle, RecordedCall, RecordedExchange};
#[cfg(feature = "bridges")]
pub use stub::{RouteMatch, StubBridge, StubRoutePending};
#[cfg(feature = "bridges")]
pub use traits::HttpBridge;
#[cfg(feature = "bridges")]
pub use types::{
    BridgeContext, BridgeError, BridgeRequest, BridgeResources, BridgeResponse,
    BridgeResponseBuilder,
};

/// When **`grpc-bridges`** is enabled, `tonic` and `prost` resolve for upcoming gRPC bridge work.
#[cfg(feature = "grpc-bridges")]
pub mod grpc {
    //! Minimal surface so optional dependencies stay linked when the feature is enabled.

    pub use prost::Message;
    pub use tonic::Status;
}
