//! Built-in Elasticsearch resource and typed helpers (`elasticsearch` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::ElasticsearchHelpers;
pub use resource::ElasticsearchResource;
