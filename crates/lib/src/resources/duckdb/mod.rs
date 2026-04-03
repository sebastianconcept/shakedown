//! Built-in DuckDB resource and typed helpers (`duckdb` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::DuckDbHelpers;
pub use resource::DuckDbResource;
