//! Built-in Oracle MySQL resource and typed helpers (`mysql` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::MysqlHelpers;
pub use resource::MysqlResource;
