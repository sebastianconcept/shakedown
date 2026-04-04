//! Built-in PostgreSQL resource and typed helpers (`postgres` feature).
//!
//! The `helpers` submodule is also built with the `timescaledb` feature alone (shared wire helpers).
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
mod helpers;
#[cfg(feature = "postgres")]
mod resource;
#[cfg(all(test, any(feature = "postgres", feature = "timescaledb")))]
mod tests;

#[cfg(feature = "postgres")]
pub use resource::PostgresResource;

#[cfg(feature = "postgres")]
pub use helpers::PostgresHelpers;

// Re-export for `timescaledb` helpers only (`postgres` callers use `helpers` directly).
#[cfg(feature = "timescaledb")]
pub(crate) use helpers::{
    postgres_assert_row_count, postgres_assert_row_exists, postgres_execute, postgres_query_all,
};
