//! Built-in NATS resource and typed helpers (`nats` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::NatsHelpers;
pub use resource::NatsResource;
