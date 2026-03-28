//! [`RabbitMqPublicationVerifier`] — RabbitMQ implementation of [`super::PublicationVerifier`].
//!
//! Consumes from a **scenario-namespaced queue** (see [`PublicationObserveSpec::logical_channel`])
//! that must exist and be bound to your exchange **before** messages are published (typical seed
//! setup). Matching uses [`super::matching::message_matches_aggregate_id`] with the AMQP
//! [`lapin::message::Delivery::routing_key`] as the message key bytes.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use lapin::options::{BasicAckOptions, BasicGetOptions, BasicNackOptions};
use lapin::types::ShortString;
use lapin::{Connection, ConnectionProperties};

use crate::error::{InfrastructureError, ShakedownError};
use crate::lifecycle::Context;

use super::matching::message_matches_aggregate_id;
use super::spec::PublicationObserveSpec;
use super::PublicationVerifier;

fn as_backend_error(error: impl std::error::Error + Send + Sync + 'static) -> ShakedownError {
    ShakedownError::Infrastructure(InfrastructureError::Backend(Box::new(error)))
}

/// Options for [`RabbitMqPublicationVerifier`].
///
/// Use a **namespaced** exchange name (via [`crate::resources::RabbitBindings::namespaced_name`])
/// and the routing key your publisher uses (often equal to the aggregate id).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RabbitMqPublicationVerifierOptions {
    /// Exchange name (including scenario prefix) — used for error context and documentation.
    pub exchange: String,
    /// Expected AMQP routing key on published messages (often the aggregate id).
    pub routing_key: String,
}

/// First-party RabbitMQ publication verifier using [`Context::rabbit_bindings`] and a bound queue.
#[derive(Debug, Default, Clone, Copy)]
pub struct RabbitMqPublicationVerifier;

#[async_trait]
impl PublicationVerifier for RabbitMqPublicationVerifier {
    type Options = RabbitMqPublicationVerifierOptions;

    async fn assert_observed(
        &self,
        context: &Context,
        spec: &PublicationObserveSpec,
        options: &Self::Options,
    ) -> Result<(), ShakedownError> {
        let rabbit_bindings = context.rabbit_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "rabbitmq bindings".to_string(),
            ))
        })?;

        let queue_name = rabbit_bindings.namespaced_name(spec.logical_channel.as_str());
        let channel_label = format!("{} → {}", options.exchange, queue_name);

        let connection = Connection::connect(
            rabbit_bindings.amqp_uri.as_str(),
            ConnectionProperties::default(),
        )
        .await
        .map_err(as_backend_error)?;
        let channel = connection
            .create_channel()
            .await
            .map_err(as_backend_error)?;

        let deadline = Instant::now() + spec.resolved_timeout();

        while Instant::now() < deadline {
            let maybe_message = channel
                .basic_get(
                    ShortString::from(queue_name.as_str()),
                    BasicGetOptions::default(),
                )
                .await
                .map_err(as_backend_error)?;

            if let Some(basic_get) = maybe_message {
                let delivery = basic_get.delivery;
                let routing_key = delivery.routing_key.as_str();
                let payload = delivery.data.as_slice();

                let routing_ok = routing_key == options.routing_key.as_str();
                let aggregate_ok = message_matches_aggregate_id(
                    Some(routing_key.as_bytes()),
                    Some(payload),
                    spec.aggregate_id.as_str(),
                );

                if routing_ok && aggregate_ok {
                    channel
                        .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                        .await
                        .map_err(as_backend_error)?;
                    let _ = connection.close(200, "shakedown_pub_ok".into()).await;
                    return Ok(());
                }

                channel
                    .basic_nack(
                        delivery.delivery_tag,
                        BasicNackOptions {
                            multiple: false,
                            requeue: true,
                        },
                    )
                    .await
                    .map_err(as_backend_error)?;
            } else {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let wait_slice = remaining.min(Duration::from_millis(200));
                if wait_slice.is_zero() {
                    break;
                }
                tokio::time::sleep(wait_slice).await;
            }
        }

        let _ = connection.close(200, "shakedown_pub_timeout".into()).await;

        Err(ShakedownError::OutboxPublicationNotFound {
            transport: "rabbitmq",
            channel: channel_label,
            aggregate_id: spec.aggregate_id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RabbitMqPublicationVerifierOptions;

    #[test]
    fn rabbit_options_roundtrip() {
        // Given Rabbit verifier options with an exchange and routing key
        let options = RabbitMqPublicationVerifierOptions {
            exchange: "orders_ex".to_string(),
            routing_key: "agg-1".to_string(),
        };
        // When we read the fields back
        // Then they match the values used for publication proof context
        assert_eq!(options.exchange, "orders_ex");
        assert_eq!(options.routing_key, "agg-1");
    }
}
