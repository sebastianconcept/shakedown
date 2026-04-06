//! Built-in Kafka resource and typed helpers (`kafka` feature).
//!
//! # Kafka testing patterns
//!
//! ## Subscribe before producing on a fresh topic
//!
//! When writing Kafka tests, always subscribe the consumer **before** producing messages on a
//! fresh (newly created) topic. Even when `auto.offset.reset` is set to `earliest`, a message
//! produced before any consumer has subscribed may be missed — the consumer group may not yet have
//! committed to partition assignment, causing the consumer to begin reading only from messages
//! produced after it joined.
//!
//! The safe ordering is:
//!
//! 1. Subscribe the consumer (call `subscribe` and wait for partition assignment).
//! 2. Produce the message.
//! 3. Poll the consumer.
//!
//! ```rust,no_run
//! # use rdkafka::config::ClientConfig;
//! # use rdkafka::consumer::{BaseConsumer, Consumer};
//! # use rdkafka::producer::{FutureProducer, FutureRecord};
//! # use rdkafka::util::Timeout;
//! # async fn example(bootstrap_servers: &str, full_topic: &str) {
//! // 1. Create and subscribe the consumer first.
//! let consumer: BaseConsumer = ClientConfig::new()
//!     .set("bootstrap.servers", bootstrap_servers)
//!     .set("group.id", "my-consumer-group")
//!     .set("auto.offset.reset", "earliest")
//!     .create()
//!     .expect("consumer created");
//! consumer.subscribe(&[full_topic]).expect("subscribed");
//!
//! // 2. Then produce.
//! let producer: FutureProducer = ClientConfig::new()
//!     .set("bootstrap.servers", bootstrap_servers)
//!     .create()
//!     .expect("producer created");
//! producer
//!     .send(
//!         FutureRecord::to(full_topic).key("k").payload("v"),
//!         Timeout::After(std::time::Duration::from_secs(5)),
//!     )
//!     .await
//!     .expect("message delivered");
//! # }
//! ```
//!
//! ## Isolating consumer groups in parallel tests
//!
//! Parallel Shakedown scenarios share the same Kafka broker. If two scenarios use the same
//! consumer group ID, they join the **same** consumer group and steal each other's messages,
//! producing non-deterministic failures.
//!
//! Use [`crate::resources::KafkaBindings::consumer_group_namespace`] to prefix every consumer
//! group ID with the scenario-scoped topic prefix, so each scenario has its own isolated group:
//!
//! ```rust,no_run
//! # use shakedown::KafkaBindings;
//! # fn example(bindings: &KafkaBindings) {
//! // Prefixes the group ID with the same scenario prefix used for topic names.
//! let group_id = bindings.consumer_group_namespace("orders-consumer");
//! # }
//! ```
//!
//! The pinned `rdkafka` version used by Shakedown is listed in `Cargo.toml` under
//! `[dependencies]`. Re-export it via `shakedown::rdkafka` (`kafka` feature) to inherit the
//! same pinned version in your test crate without a separate dependency declaration.
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::KafkaHelpers;
pub use resource::KafkaResource;
