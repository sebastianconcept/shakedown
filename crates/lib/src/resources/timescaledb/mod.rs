//! Built-in TimescaleDB resource and typed helpers (`timescaledb` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::TimescaledbHelpers;
pub use resource::TimescaledbResource;
