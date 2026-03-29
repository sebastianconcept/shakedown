//! Bridge request/response types, context, resources, and errors.

mod context;
mod error;
mod request;
mod resources;
mod response;
mod response_builder;

#[cfg(all(test, feature = "bridges"))]
mod tests;

pub use context::BridgeContext;
pub use error::BridgeError;
pub use request::BridgeRequest;
pub use resources::BridgeResources;
pub use response::BridgeResponse;
pub use response_builder::BridgeResponseBuilder;

pub(crate) use response::internal_server_error_http_response;
