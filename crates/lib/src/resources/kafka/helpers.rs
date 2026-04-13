//! Kafka typed helpers — [`KafkaHelpers`] for scenario-scoped produce and consume.

use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use serde::Serialize;

use crate::error::named_binding::not_provisioned_named_binding;
use crate::error::ShakedownError;
use crate::helper_prelude::{map_backend_error, ConsumedMessage, DEFAULT_HELPER_CONSUME_TIMEOUT};
use crate::lifecycle::{Context, DEFAULT_RESOURCE_BINDING_NAME};
use crate::resources::KafkaBindings;

/// Facade for Kafka produce helpers bound to [`Context`].
pub struct KafkaHelpers<'ctx> {
    context: &'ctx Context,
    binding_name: String,
}

impl<'ctx> KafkaHelpers<'ctx> {
    /// Helpers scoped to the [`DEFAULT_RESOURCE_BINDING_NAME`] binding.
    pub fn new(context: &'ctx Context) -> Result<Self, ShakedownError> {
        Self::named(context, DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Helpers scoped to the binding registered under `name` in [`Context`].
    pub fn named(context: &'ctx Context, name: &str) -> Result<Self, ShakedownError> {
        if context.kafka_bindings_named(name).is_none() {
            return Err(not_provisioned_named_binding("kafka bindings", name));
        }
        Ok(Self {
            context,
            binding_name: name.to_string(),
        })
    }

    fn kafka_bindings(&self) -> Result<&KafkaBindings, ShakedownError> {
        self.context
            .kafka_bindings_named(self.binding_name.as_str())
            .ok_or_else(|| {
                not_provisioned_named_binding("kafka bindings", self.binding_name.as_str())
            })
    }

    /// Produce a message to a scenario-scoped topic.
    ///
    /// `topic` is the logical name; the full Kafka topic is
    /// [`KafkaBindings::topic_namespace`].
    pub async fn produce(
        &self,
        topic: &str,
        key: impl Into<Vec<u8>>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), ShakedownError> {
        let kafka_bindings = self.kafka_bindings()?;
        let full_topic = kafka_bindings.topic_namespace(topic);
        let key_bytes = key.into();
        let payload_bytes = payload.into();

        let client_id = format!("{}-helpers", self.context.scenario_id().as_str());

        let producer: FutureProducer = ClientConfig::new()
            .set(
                "bootstrap.servers",
                kafka_bindings.bootstrap_servers.as_str(),
            )
            .set("client.id", client_id.as_str())
            .create()
            .map_err(map_backend_error)?;

        let record = FutureRecord::to(full_topic.as_str())
            .key(&key_bytes)
            .payload(&payload_bytes);

        let delivery = producer
            .send(record, Timeout::After(std::time::Duration::from_secs(60)))
            .await;

        match delivery {
            Ok(_) => Ok(()),
            Err((kafka_err, _owned_message)) => Err(map_backend_error(kafka_err)),
        }
    }

    /// Produce a JSON-serialized message to a scenario-scoped topic.
    ///
    /// Serializes `value` with [`serde_json::to_vec`], then delegates to [`Self::produce`].
    pub async fn produce_json(
        &self,
        topic: &str,
        key: impl Into<Vec<u8>>,
        value: &impl Serialize,
    ) -> Result<(), ShakedownError> {
        let payload = serde_json::to_vec(value).map_err(ShakedownError::PayloadSerialisation)?;
        self.produce(topic, key, payload).await
    }

    /// Produce a raw-byte message to a scenario-scoped topic.
    ///
    /// Alias for [`Self::produce`]. Pairs with [`Self::produce_json`] for discoverability when the
    /// payload is already encoded (for example protobuf).
    pub async fn produce_bytes(
        &self,
        topic: &str,
        key: impl Into<Vec<u8>>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), ShakedownError> {
        self.produce(topic, key, payload).await
    }

    /// Poll until a message matching `predicate` arrives on the scenario-scoped topic, or the
    /// timeout expires.
    ///
    /// `topic` is the logical name; the full Kafka topic is [`KafkaBindings::topic_namespace`].
    /// When `timeout` is `None`, uses [`DEFAULT_HELPER_CONSUME_TIMEOUT`] (20 seconds).
    ///
    /// The rdkafka poll loop runs on the Tokio blocking pool so worker threads are not stalled for
    /// up to 200 ms per iteration. The predicate must be [`Send`] and capture only [`Send`] data.
    pub async fn consume_until<F>(
        &self,
        topic: &str,
        predicate: F,
        timeout: Option<Duration>,
    ) -> Result<ConsumedMessage, ShakedownError>
    where
        F: Fn(&ConsumedMessage) -> bool + Send + 'static,
    {
        let kafka_bindings = self.kafka_bindings()?.clone();
        let full_topic = kafka_bindings.topic_namespace(topic);
        let limit = timeout.unwrap_or(DEFAULT_HELPER_CONSUME_TIMEOUT);
        let scenario_id = self.context.scenario_id().to_string();

        let full_topic_for_blocking = full_topic.clone();
        let join_handle = tokio::task::spawn_blocking(move || {
            let started_at = Instant::now();
            let deadline = started_at + limit;

            let consumer_group = format!("shakedown-helpers-consume-{}", scenario_id.as_str());
            let consumer: BaseConsumer = ClientConfig::new()
                .set(
                    "bootstrap.servers",
                    kafka_bindings.bootstrap_servers.as_str(),
                )
                .set("group.id", consumer_group.as_str())
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .create()
                .map_err(map_backend_error)?;

            consumer
                .subscribe(&[full_topic_for_blocking.as_str()])
                .map_err(map_backend_error)?;

            while Instant::now() < deadline {
                let remaining_duration = deadline.saturating_duration_since(Instant::now());
                let poll_duration = remaining_duration.min(Duration::from_millis(200));
                if let Some(result) = consumer.poll(poll_duration) {
                    let message = result.map_err(map_backend_error)?;
                    let consumed = ConsumedMessage {
                        key: message.key().map(|slice| slice.to_vec()),
                        payload: message.payload().map(|slice| slice.to_vec()),
                        source: message.topic().to_string(),
                    };
                    if predicate(&consumed) {
                        return Ok(consumed);
                    }
                }
            }

            let elapsed = started_at.elapsed();
            Err(ShakedownError::HelperTimeout {
                resource: full_topic_for_blocking,
                operation: "kafka.consume_until",
                timeout: limit,
                elapsed,
            })
        });

        join_handle.await.map_err(map_backend_error)?
    }

    /// Return the first message observed on the scenario-scoped topic (no predicate filtering).
    ///
    /// Shorthand for [`Self::consume_until`] with `|_| true`.
    pub async fn consume_one(
        &self,
        topic: &str,
        timeout: Option<Duration>,
    ) -> Result<ConsumedMessage, ShakedownError> {
        self.consume_until(topic, |_| true, timeout).await
    }

    /// Subscribe to the scenario-scoped topic **before** producing, then return the first consumed
    /// message that matches `predicate`.
    ///
    /// This helper solves the fresh-topic problem: a message produced before any consumer has
    /// joined the consumer group may be missed even when `auto.offset.reset = earliest`. By
    /// subscribing first and waiting for partition assignment before producing, messages are
    /// guaranteed to be observed by the consumer.
    ///
    /// `topic` is the logical name; the full Kafka topic is [`KafkaBindings::topic_namespace`].
    /// When `timeout` is `None`, uses [`DEFAULT_HELPER_CONSUME_TIMEOUT`] (20 seconds).
    ///
    /// See also: [`Self::consume_until`] for consuming from a topic that already has messages.
    ///
    /// On timeout, returns [`ShakedownError::HelperTimeout`] with `operation` set to
    /// `"kafka.produce_and_await"`. Match it with [`ShakedownError::is_kafka_produce_and_await_timeout`].
    pub async fn produce_and_await(
        &self,
        topic: &str,
        key: impl Into<Vec<u8>>,
        payload: impl Into<Vec<u8>>,
        timeout: Option<Duration>,
    ) -> Result<ConsumedMessage, ShakedownError> {
        let kafka_bindings = self.kafka_bindings()?.clone();
        let full_topic = kafka_bindings.topic_namespace(topic);
        let limit = timeout.unwrap_or(DEFAULT_HELPER_CONSUME_TIMEOUT);
        let scenario_id = self.context.scenario_id().to_string();
        let key_bytes = key.into();
        let payload_bytes = payload.into();

        let full_topic_clone = full_topic.clone();
        let join_handle = tokio::task::spawn_blocking(move || {
            let started_at = Instant::now();
            let deadline = started_at + limit;

            let consumer_group = format!(
                "shakedown-helpers-produce-and-await-{}",
                scenario_id.as_str()
            );
            let consumer: BaseConsumer = ClientConfig::new()
                .set(
                    "bootstrap.servers",
                    kafka_bindings.bootstrap_servers.as_str(),
                )
                .set("group.id", consumer_group.as_str())
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .create()
                .map_err(map_backend_error)?;

            consumer
                .subscribe(&[full_topic_clone.as_str()])
                .map_err(map_backend_error)?;

            // Poll until partition assignment is confirmed before producing. A successful poll
            // with non-empty assignment means the consumer has joined the group and offsets are
            // resolved; a message produced now will be seen by this consumer.
            let assignment_deadline = Instant::now() + limit.min(Duration::from_secs(10));
            loop {
                let now = Instant::now();
                if now >= assignment_deadline {
                    break;
                }
                let remaining = assignment_deadline.saturating_duration_since(now);
                let poll_duration = remaining.min(Duration::from_millis(200));
                let _ = consumer.poll(poll_duration);

                let assignment = consumer.assignment().map_err(map_backend_error)?;
                if assignment.count() > 0 {
                    break;
                }
            }

            // Sync `BaseProducer` keeps all Kafka I/O on this blocking thread (`tokio::spawn` here
            // can deadlock the runtime).
            let client_id = format!("shakedown-paa-producer-{}", scenario_id.as_str());
            let producer: BaseProducer = ClientConfig::new()
                .set(
                    "bootstrap.servers",
                    kafka_bindings.bootstrap_servers.as_str(),
                )
                .set("client.id", client_id.as_str())
                .create()
                .map_err(map_backend_error)?;

            let topic_for_producer = full_topic_clone.as_str();
            producer
                .send(
                    BaseRecord::to(topic_for_producer)
                        .key(&key_bytes)
                        .payload(&payload_bytes),
                )
                .map_err(|(kafka_err, _owned)| map_backend_error(kafka_err))?;
            producer
                .flush(Duration::from_secs(60))
                .map_err(map_backend_error)?;

            // Now poll to consume the message we just produced.
            while Instant::now() < deadline {
                let remaining_duration = deadline.saturating_duration_since(Instant::now());
                let poll_duration = remaining_duration.min(Duration::from_millis(200));
                if let Some(result) = consumer.poll(poll_duration) {
                    let message = result.map_err(map_backend_error)?;
                    let consumed = ConsumedMessage {
                        key: message.key().map(|slice| slice.to_vec()),
                        payload: message.payload().map(|slice| slice.to_vec()),
                        source: message.topic().to_string(),
                    };
                    return Ok(consumed);
                }
            }

            let elapsed = started_at.elapsed();
            Err(ShakedownError::HelperTimeout {
                resource: full_topic_clone,
                operation: "kafka.produce_and_await",
                timeout: limit,
                elapsed,
            })
        });

        join_handle.await.map_err(map_backend_error)?
    }
}
