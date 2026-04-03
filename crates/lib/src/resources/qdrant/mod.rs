//! Built-in Qdrant resource and typed helpers (`qdrant` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::QdrantHelpers;
pub use resource::QdrantResource;
