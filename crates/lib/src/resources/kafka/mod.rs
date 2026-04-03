//! Built-in Kafka resource and typed helpers (`kafka` feature).
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::KafkaHelpers;
pub use resource::KafkaResource;
