//! Kafka typed helpers — [`KafkaHelpers`] for scenario-scoped produce and consume.

use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::Serialize;

use crate::error::{InfrastructureError, ShakedownError};
use crate::helper_prelude::{map_backend_error, ConsumedMessage, DEFAULT_HELPER_CONSUME_TIMEOUT};
use crate::lifecycle::Context;
use crate::resources::KafkaBindings;

/// Facade for Kafka produce helpers bound to [`Context`].
pub struct KafkaHelpers<'ctx> {
    context: &'ctx Context,
}

impl<'ctx> KafkaHelpers<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    fn kafka_bindings(&self) -> Result<&KafkaBindings, ShakedownError> {
        self.context.kafka_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "kafka bindings".to_string(),
            ))
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
}
