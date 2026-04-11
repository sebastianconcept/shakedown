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
//! ## Topic namespacing and the outbox pattern
//!
//! Shakedown prefixes Kafka topic names per scenario (for example `shakedown_{scenario_id}_input`)
//! so parallel tests do not collide on the broker. The **transactional outbox** pattern stores a
//! destination topic name in a database column; a publisher later reads that column and produces
//! to the named topic. If the outbox row contains only the **logical** name (`"input"`) while the
//! publisher sends to that literal string, the message may go to a topic **without** Shakedown’s
//! prefix—bypassing the scenario namespace and breaking isolation.
//!
//! ### Pattern A (recommended): inject the namespaced topic
//!
//! Use `EnvMapping::kafka_topic` on `ServiceUnderTest` (see the `services` feature) so the service
//! receives the **full** topic name (including the scenario prefix) in an environment variable. The
//! service should write that value into the outbox column; the publisher then targets the same
//! namespaced topic that the harness and helpers use.
//!
//! The string injected for a logical topic is the same as [`KafkaBindings::topic_namespace`]
//! for that name—your test can assert the pipeline using the logical topic on helpers while
//! trusting that the env-injected name matches:
//!
//! ```rust,no_run
//! # use shakedown::Context;
//! # use std::time::Duration;
//! # async fn example(ctx: &mut Context) -> Result<(), shakedown::ShakedownError> {
//! let bindings = ctx.kafka_bindings().expect("Kafka resource registered for this scenario");
//! let _namespaced_topic = bindings.topic_namespace("input");
//! // Same string the harness injects when you use `EnvMapping::kafka_topic("INPUT_TOPIC", "input")`.
//! let _consumed = ctx
//!     .kafka()
//!     .consume_one("input", Some(Duration::from_secs(20)))
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! With the `services` feature, wire the service container like this (not run as a doctest here
//! because `EnvMapping` requires `services`):
//!
//! ```ignore
//! use shakedown::{EnvMapping, ServiceUnderTest};
//!
//! let _spec = ServiceUnderTest::docker("myservice:latest")
//!     .env_mapping(EnvMapping::kafka_topic("INPUT_TOPIC", "input"))
//!     .build()
//!     .expect("service spec");
//! ```
//!
//! ### Pattern B: literal topic names
//!
//! If topic names are compile-time constants and the publisher always targets those fixed names,
//! you may keep literal topic strings in the outbox and rely on **database-level** isolation: a
//! scenario-scoped database ensures outbox rows do not leak across parallel runs. Kafka topic
//! isolation for that fixed name is then weaker unless you serialize tests or accept shared topics;
//! this can be acceptable for narrow tests or when only one scenario touches the topic.
//!
//! ### Decision guide
//!
//! - Prefer **Pattern A** when the service reads topic names from **configuration** (for example
//!   12-factor env vars): inject the namespaced name once via `EnvMapping::kafka_topic` and keep a
//!   single source of truth for the harness, the outbox, and the publisher.
//! - Use **Pattern B** when topic names are **fixed in code** and changing them is impractical;
//!   treat the scenario database as the isolation boundary and accept the Kafka naming trade-off.
//!
//! ## Inheriting the pinned `rdkafka` version
//!
//! Shakedown pins `rdkafka` at **`=0.38`** to guarantee a reproducible build. When the `kafka`
//! feature is enabled, the crate re-exports it as `shakedown::rdkafka`, so you can write:
//!
//! ```rust,no_run
//! use shakedown::rdkafka::producer::FutureProducer;
//! ```
//!
//! …without declaring a separate `rdkafka` dependency in your own `Cargo.toml`. This prevents
//! version skew between the harness and your test code.
mod helpers;
mod resource;
#[cfg(test)]
mod tests;

pub use helpers::KafkaHelpers;
pub use resource::KafkaResource;
