//! Built-in Weaviate resource and typed helpers (`weaviate` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::WeaviateHelpers;
pub use resource::WeaviateResource;
