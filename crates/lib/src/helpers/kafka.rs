//! Kafka typed helpers — [`KafkaHelpers`] for scenario-scoped produce.

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::Serialize;

use crate::error::{InfrastructureError, ShakedownError};
use crate::lifecycle::Context;
use crate::resources::KafkaBindings;

fn map_backend_error(error: impl std::error::Error + Send + Sync + 'static) -> ShakedownError {
    ShakedownError::Infrastructure(InfrastructureError::Backend(Box::new(error)))
}

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
        let payload = serde_json::to_vec(value)
            .map_err(|error| ShakedownError::PayloadDeserialisation(error.to_string()))?;
        self.produce(topic, key, payload).await
    }
}
