//! Built-in OpenSearch resource and typed helpers (`opensearch` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::OpenSearchHelpers;
pub use resource::OpenSearchResource;
