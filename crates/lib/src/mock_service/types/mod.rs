//! Shared mock service types: context, error, and resources.

mod context;
mod error;
mod resources;

#[cfg(all(test, feature = "mock-services"))]
mod tests;

pub use context::MockServiceContext;
pub use error::MockServiceError;
pub use resources::MockServiceResources;
