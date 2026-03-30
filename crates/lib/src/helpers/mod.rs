//! Typed helpers for produce, consume, and assert operations on [`crate::lifecycle::Context`] bindings.

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "kafka")]
pub use kafka::KafkaHelpers;
