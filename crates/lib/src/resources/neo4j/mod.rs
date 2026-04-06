//! Built-in Neo4j resource and typed helpers (`neo4j` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::Neo4jHelpers;
pub(crate) use resource::graph_system;
pub use resource::Neo4jResource;
