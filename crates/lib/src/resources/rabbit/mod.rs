//! Built-in RabbitMQ resource and typed helpers (`rabbitmq` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::{ExchangeKind, RabbitHelpers};
pub use resource::RabbitResource;
