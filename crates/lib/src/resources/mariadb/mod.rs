//! Built-in MariaDB resource and typed helpers (`mariadb` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::MariaDbHelpers;
pub use resource::MariaDBResource;
