//! gRPC mock service types and implementations (`grpc-mock-services` feature).

mod protocol_error;
#[cfg(feature = "grpc-mock-services")]
pub(crate) mod server;
mod stub;
mod types;
mod r#trait;

pub use r#trait::GrpcMockServiceTrait;
pub use stub::{GrpcMockService, GrpcMockServiceMethodPending};
pub use types::{GrpcMockServiceRequest, GrpcMockServiceResponse};
