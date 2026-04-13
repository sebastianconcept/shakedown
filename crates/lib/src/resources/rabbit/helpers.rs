//! RabbitMQ typed helpers — [`RabbitHelpers`] for scenario-scoped publish and topology setup.

use std::time::{Duration, Instant};

use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicNackOptions, BasicPublishOptions,
    ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{FieldTable, ShortString};
use lapin::{BasicProperties, Connection, ConnectionProperties};
use serde::Serialize;

pub use lapin::ExchangeKind;

use crate::error::named_binding::not_provisioned_named_binding;
use crate::error::ShakedownError;
use crate::helper_prelude::{map_backend_error, ConsumedMessage, DEFAULT_HELPER_CONSUME_TIMEOUT};
use crate::lifecycle::{Context, DEFAULT_RESOURCE_BINDING_NAME};
use crate::resources::RabbitBindings;

/// Facade for RabbitMQ publish helpers bound to [`Context`].
pub struct RabbitHelpers<'ctx> {
    context: &'ctx Context,
    binding_name: String,
}

impl<'ctx> RabbitHelpers<'ctx> {
    /// Helpers scoped to the [`DEFAULT_RESOURCE_BINDING_NAME`] binding.
    pub fn new(context: &'ctx Context) -> Result<Self, ShakedownError> {
        Self::named(context, DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Helpers scoped to the binding registered under `name` in [`Context`].
    pub fn named(context: &'ctx Context, name: &str) -> Result<Self, ShakedownError> {
        if context.rabbit_bindings_named(name).is_none() {
            return Err(not_provisioned_named_binding("rabbitmq bindings", name));
        }
        Ok(Self {
            context,
            binding_name: name.to_string(),
        })
    }

    fn rabbit_bindings(&self) -> Result<&RabbitBindings, ShakedownError> {
        self.context
            .rabbit_bindings_named(self.binding_name.as_str())
            .ok_or_else(|| {
                not_provisioned_named_binding("rabbitmq bindings", self.binding_name.as_str())
            })
    }

    /// Publish a message to a scenario-scoped exchange.
    ///
    /// `exchange` is the logical name; the AMQP exchange name is [`RabbitBindings::namespaced_name`].
    /// `routing_key` is sent as-is (not namespaced).
    pub async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), ShakedownError> {
        let rabbit_bindings = self.rabbit_bindings()?;
        let exchange_name = rabbit_bindings.namespaced_name(exchange);
        let payload_bytes = payload.into();

        let connection = Connection::connect(
            rabbit_bindings.amqp_uri.as_str(),
            ConnectionProperties::default(),
        )
        .await
        .map_err(map_backend_error)?;
        let channel = connection
            .create_channel()
            .await
            .map_err(map_backend_error)?;

        channel
            .basic_publish(
                exchange_name.as_str().into(),
                routing_key.into(),
                BasicPublishOptions::default(),
                payload_bytes.as_slice(),
                BasicProperties::default(),
            )
            .await
            .map_err(map_backend_error)?
            .await
            .map_err(map_backend_error)?;

        let _ = connection
            .close(200, "shakedown_helpers_publish".into())
            .await;
        Ok(())
    }

    /// Publish a JSON-serialized message to a scenario-scoped exchange.
    ///
    /// Serializes `value` with [`serde_json::to_vec`], then delegates to [`Self::publish`].
    pub async fn publish_json(
        &self,
        exchange: &str,
        routing_key: &str,
        value: &impl Serialize,
    ) -> Result<(), ShakedownError> {
        let payload = serde_json::to_vec(value).map_err(ShakedownError::PayloadSerialisation)?;
        self.publish(exchange, routing_key, payload).await
    }

    /// Declare exchange, queue, and binding in one call.
    ///
    /// Names are namespaced via [`RabbitBindings::namespaced_name`]. The queue must be declared
    /// before [`Self::publish`] or consume helpers.
    pub async fn declare_topology(
        &self,
        exchange: &str,
        exchange_kind: ExchangeKind,
        queue: &str,
        routing_key: &str,
    ) -> Result<(), ShakedownError> {
        let rabbit_bindings = self.rabbit_bindings()?;
        let exchange_name = rabbit_bindings.namespaced_name(exchange);
        let queue_name = rabbit_bindings.namespaced_name(queue);

        let connection = Connection::connect(
            rabbit_bindings.amqp_uri.as_str(),
            ConnectionProperties::default(),
        )
        .await
        .map_err(map_backend_error)?;
        let channel = connection
            .create_channel()
            .await
            .map_err(map_backend_error)?;

        channel
            .exchange_declare(
                exchange_name.as_str().into(),
                exchange_kind,
                ExchangeDeclareOptions {
                    durable: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(map_backend_error)?;

        channel
            .queue_declare(
                queue_name.as_str().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(map_backend_error)?;

        channel
            .queue_bind(
                queue_name.as_str().into(),
                exchange_name.as_str().into(),
                routing_key.into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(map_backend_error)?;

        let _ = connection
            .close(200, "shakedown_helpers_topology".into())
            .await;
        Ok(())
    }

    /// Poll until a message matching `predicate` arrives on the scenario-scoped queue, or the
    /// timeout expires.
    ///
    /// `queue` is the logical name; the AMQP queue name is [`RabbitBindings::namespaced_name`].
    /// The queue must already exist (see [`Self::declare_topology`]). When `timeout` is `None`,
    /// uses [`DEFAULT_HELPER_CONSUME_TIMEOUT`] (20 seconds).
    ///
    /// Non-matching messages are nacked with requeue so they remain available for other consumers
    /// or a later poll (same behaviour as [`crate::publication::RabbitMqPublicationVerifier`]).
    pub async fn consume_until<F>(
        &self,
        queue: &str,
        predicate: F,
        timeout: Option<Duration>,
    ) -> Result<ConsumedMessage, ShakedownError>
    where
        F: Fn(&ConsumedMessage) -> bool,
    {
        let rabbit_bindings = self.rabbit_bindings()?;
        let queue_name = rabbit_bindings.namespaced_name(queue);
        let limit = timeout.unwrap_or(DEFAULT_HELPER_CONSUME_TIMEOUT);
        let started_at = Instant::now();
        let deadline = started_at + limit;

        let connection = Connection::connect(
            rabbit_bindings.amqp_uri.as_str(),
            ConnectionProperties::default(),
        )
        .await
        .map_err(map_backend_error)?;
        let channel = connection
            .create_channel()
            .await
            .map_err(map_backend_error)?;

        while Instant::now() < deadline {
            let maybe_message = channel
                .basic_get(
                    ShortString::from(queue_name.as_str()),
                    BasicGetOptions::default(),
                )
                .await
                .map_err(map_backend_error)?;

            if let Some(basic_get) = maybe_message {
                let delivery = basic_get.delivery;
                let routing_key = delivery.routing_key.as_str();
                let consumed = ConsumedMessage {
                    key: Some(routing_key.as_bytes().to_vec()),
                    payload: Some(delivery.data.to_vec()),
                    source: queue_name.clone(),
                };

                if predicate(&consumed) {
                    channel
                        .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                        .await
                        .map_err(map_backend_error)?;
                    let _ = connection
                        .close(200, "shakedown_helpers_consume_ok".into())
                        .await;
                    return Ok(consumed);
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
                    .map_err(map_backend_error)?;

                let remaining = deadline.saturating_duration_since(Instant::now());
                let wait_slice = remaining.min(Duration::from_millis(50));
                if wait_slice.is_zero() {
                    break;
                }
                tokio::time::sleep(wait_slice).await;
            } else {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let wait_slice = remaining.min(Duration::from_millis(200));
                if wait_slice.is_zero() {
                    break;
                }
                tokio::time::sleep(wait_slice).await;
            }
        }

        let _ = connection
            .close(200, "shakedown_helpers_consume_timeout".into())
            .await;

        let elapsed = started_at.elapsed();
        Err(ShakedownError::HelperTimeout {
            resource: queue_name,
            operation: "rabbit.consume_until",
            timeout: limit,
            elapsed,
        })
    }

    /// Return the first message observed on the scenario-scoped queue (no predicate filtering).
    ///
    /// Shorthand for [`Self::consume_until`] with `|_| true`.
    pub async fn consume_one(
        &self,
        queue: &str,
        timeout: Option<Duration>,
    ) -> Result<ConsumedMessage, ShakedownError> {
        self.consume_until(queue, |_| true, timeout).await
    }

    /// Declare exchange/queue topology, register a consumer (`basic_consume`) **before**
    /// `basic_publish`, and return the first delivery.
    ///
    /// This helper solves the fresh-exchange/queue problem: a message published before a consumer
    /// has declared and bound the queue will be dropped. By declaring the topology and registering
    /// a consumer before publishing, messages are guaranteed to be routed and delivered.
    ///
    /// `exchange` and `queue` are logical names; AMQP names are namespaced via
    /// [`RabbitBindings::namespaced_name`]. `routing_key` is sent as-is.
    /// When `timeout` is `None`, uses [`DEFAULT_HELPER_CONSUME_TIMEOUT`] (20 seconds).
    ///
    /// See also: [`Self::declare_topology`] + [`Self::publish`] + [`Self::consume_until`] for
    /// explicit control over each step.
    ///
    /// On timeout, returns [`ShakedownError::HelperTimeout`] with `operation` `"rabbit.publish_and_await"`.
    /// Match with [`ShakedownError::is_rabbit_publish_and_await_timeout`].
    pub async fn publish_and_await(
        &self,
        exchange: &str,
        exchange_kind: ExchangeKind,
        queue: &str,
        routing_key: &str,
        payload: impl Into<Vec<u8>>,
        timeout: Option<Duration>,
    ) -> Result<ConsumedMessage, ShakedownError> {
        let rabbit_bindings = self.rabbit_bindings()?;
        let exchange_name = rabbit_bindings.namespaced_name(exchange);
        let queue_name = rabbit_bindings.namespaced_name(queue);
        let limit = timeout.unwrap_or(DEFAULT_HELPER_CONSUME_TIMEOUT);
        let payload_bytes = payload.into();
        let routing_key_owned = routing_key.to_string();
        let consumer_tag = format!("shakedown-paa-{}", self.context.scenario_id().as_str());

        // Step 1: Declare topology (exchange + queue + binding).
        let connection = Connection::connect(
            rabbit_bindings.amqp_uri.as_str(),
            ConnectionProperties::default(),
        )
        .await
        .map_err(map_backend_error)?;
        let channel = connection
            .create_channel()
            .await
            .map_err(map_backend_error)?;

        channel
            .exchange_declare(
                exchange_name.as_str().into(),
                exchange_kind,
                ExchangeDeclareOptions {
                    durable: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(map_backend_error)?;

        channel
            .queue_declare(
                queue_name.as_str().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(map_backend_error)?;

        channel
            .queue_bind(
                queue_name.as_str().into(),
                exchange_name.as_str().into(),
                routing_key_owned.as_str().into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(map_backend_error)?;

        // Step 2: Register a consumer (subscribe) before publishing so routing has a ready sink.
        let mut consumer = channel
            .basic_consume(
                ShortString::from(queue_name.as_str()),
                ShortString::from(consumer_tag.as_str()),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(map_backend_error)?;

        // Step 3: Publish after the consumer is registered.
        channel
            .basic_publish(
                exchange_name.as_str().into(),
                routing_key_owned.as_str().into(),
                BasicPublishOptions::default(),
                payload_bytes.as_slice(),
                BasicProperties::default(),
            )
            .await
            .map_err(map_backend_error)?
            .await
            .map_err(map_backend_error)?;

        // Step 4: Await the first delivery on the consumer stream.
        let started_at = Instant::now();
        let deadline = started_at + limit;

        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, consumer.next()).await {
                Ok(Some(Ok(delivery))) => {
                    let delivery_routing_key = delivery.routing_key.as_str();
                    let consumed = ConsumedMessage {
                        key: Some(delivery_routing_key.as_bytes().to_vec()),
                        payload: Some(delivery.data.to_vec()),
                        source: queue_name.clone(),
                    };
                    channel
                        .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                        .await
                        .map_err(map_backend_error)?;
                    let _ = connection
                        .close(200, "shakedown_helpers_publish_and_await_ok".into())
                        .await;
                    return Ok(consumed);
                }
                Ok(Some(Err(error))) => return Err(map_backend_error(error)),
                Ok(None) => break,
                Err(_) => {}
            }
        }

        let _ = connection
            .close(200, "shakedown_helpers_publish_and_await_timeout".into())
            .await;

        let elapsed = started_at.elapsed();
        Err(ShakedownError::HelperTimeout {
            resource: queue_name,
            operation: "rabbit.publish_and_await",
            timeout: limit,
            elapsed,
        })
    }
}
