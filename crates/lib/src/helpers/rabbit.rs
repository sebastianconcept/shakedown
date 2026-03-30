//! RabbitMQ typed helpers — [`RabbitHelpers`] for scenario-scoped publish and topology setup.

use lapin::options::{
    BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use serde::Serialize;

pub use lapin::ExchangeKind;

use crate::error::{InfrastructureError, ShakedownError};
use crate::lifecycle::Context;
use crate::resources::RabbitBindings;

fn map_backend_error(error: impl std::error::Error + Send + Sync + 'static) -> ShakedownError {
    ShakedownError::Infrastructure(InfrastructureError::Backend(Box::new(error)))
}

/// Facade for RabbitMQ publish helpers bound to [`Context`].
pub struct RabbitHelpers<'ctx> {
    context: &'ctx Context,
}

impl<'ctx> RabbitHelpers<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    fn rabbit_bindings(&self) -> Result<&RabbitBindings, ShakedownError> {
        self.context.rabbit_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "rabbitmq bindings".to_string(),
            ))
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
        let payload = serde_json::to_vec(value)
            .map_err(|error| ShakedownError::PayloadDeserialisation(error.to_string()))?;
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
}
