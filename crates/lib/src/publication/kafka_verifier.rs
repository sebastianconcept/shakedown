//! [`KafkaPublicationVerifier`] — Kafka implementation of [`super::PublicationVerifier`].

use std::time::{Duration, Instant};

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;

use crate::error::{InfrastructureError, IntoInfrastructureError, ShakedownError};
use crate::lifecycle::Context;
use crate::OutboxError;

use super::matching::message_matches_aggregate_id;
use super::spec::PublicationObserveSpec;
use super::PublicationVerifier;

fn as_backend_error(error: impl std::error::Error + Send + Sync + 'static) -> ShakedownError {
    error.into_infrastructure_error().into()
}

/// First-party Kafka publication verifier using [`Context::kafka_bindings`] and topic namespacing.
#[derive(Debug, Default, Clone, Copy)]
pub struct KafkaPublicationVerifier;

#[async_trait]
impl PublicationVerifier for KafkaPublicationVerifier {
    type Options = ();

    async fn assert_observed(
        &self,
        context: &Context,
        spec: &PublicationObserveSpec,
        _options: &Self::Options,
    ) -> Result<(), ShakedownError> {
        let kafka_bindings = context.kafka_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "kafka bindings".to_string(),
            ))
        })?;

        let topic = kafka_bindings.topic_namespace(spec.logical_channel.as_str());
        let consumer_group = format!("shakedown-outbox-assert-{}", context.scenario_id().as_str());
        let consumer: BaseConsumer = ClientConfig::new()
            .set(
                "bootstrap.servers",
                kafka_bindings.bootstrap_servers.as_str(),
            )
            .set("group.id", consumer_group.as_str())
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(as_backend_error)?;

        consumer
            .subscribe(&[topic.as_str()])
            .map_err(as_backend_error)?;

        let deadline = Instant::now() + spec.resolved_timeout();
        while Instant::now() < deadline {
            let remaining_duration = deadline.saturating_duration_since(Instant::now());
            let poll_duration = remaining_duration.min(Duration::from_millis(200));
            if let Some(result) = consumer.poll(poll_duration) {
                let message = result.map_err(as_backend_error)?;
                if message_matches_aggregate_id(
                    message.key(),
                    message.payload(),
                    spec.aggregate_id.as_str(),
                ) {
                    return Ok(());
                }
            }
        }

        Err(OutboxError::PublicationNotFound {
            transport: "kafka",
            channel: topic,
            aggregate_id: spec.aggregate_id.clone(),
        }
        .into())
    }
}
