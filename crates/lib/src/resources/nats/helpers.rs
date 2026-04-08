//! NATS typed helpers — [`NatsHelpers`] for scenario-scoped publish and subscribe via the async client.
//! The suite [`super::resource::NatsResource`](crate::resources::nats::NatsResource) enables JetStream on the server so outbox publication checks can read persisted messages.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::stream::StreamExt;

use crate::error::{InfrastructureError, ShakedownError};
use crate::helper_prelude::{map_backend_error, ConsumedMessage, DEFAULT_HELPER_CONSUME_TIMEOUT};
use crate::lifecycle::Context;
use crate::resources::NatsBindings;

/// Facade for NATS helpers bound to [`Context`].
pub struct NatsHelpers<'ctx> {
    context: &'ctx Context,
}

impl<'ctx> NatsHelpers<'ctx> {
    pub(crate) fn new(context: &'ctx Context) -> Self {
        Self { context }
    }

    fn nats_bindings(&self) -> Result<&NatsBindings, ShakedownError> {
        self.context.nats_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "nats bindings".to_string(),
            ))
        })
    }

    /// Publish a payload to a logical subject (auto-prefixed with the scenario namespace).
    pub async fn publish(&self, subject: &str, payload: &[u8]) -> Result<(), ShakedownError> {
        let nats_bindings = self.nats_bindings()?;
        let full_subject = nats_bindings.subject_namespace(subject);
        let client = async_nats::connect(nats_bindings.connection_url.as_str())
            .await
            .map_err(map_backend_error)?;
        client
            .publish(full_subject, Bytes::copy_from_slice(payload))
            .await
            .map_err(map_backend_error)?;
        let _ = client.drain().await;
        Ok(())
    }

    /// Subscribe to the namespaced subject **before** publishing, then return the first received
    /// message.
    ///
    /// This helper solves the fresh-subject problem: a message published before a subscriber
    /// exists is dropped by the NATS server (core NATS is fire-and-forget). By subscribing first
    /// and then publishing, the message is guaranteed to be delivered.
    ///
    /// `subject` is the logical name; it is prefixed with the scenario namespace via
    /// [`NatsBindings::subject_namespace`]. When `timeout` is `None`, uses
    /// [`DEFAULT_HELPER_CONSUME_TIMEOUT`] (20 seconds).
    ///
    /// See also: [`Self::subscribe_until`] for consuming from a subject using a predicate.
    ///
    /// On timeout, returns [`ShakedownError::HelperTimeout`] with `operation` `"nats.publish_and_await"`.
    /// Match with [`ShakedownError::is_nats_publish_and_await_timeout`].
    pub async fn publish_and_await(
        &self,
        subject: &str,
        payload: &[u8],
        timeout: Option<Duration>,
    ) -> Result<ConsumedMessage, ShakedownError> {
        let nats_bindings = self.nats_bindings()?;
        let full_subject = nats_bindings.subject_namespace(subject);
        let limit = timeout.unwrap_or(DEFAULT_HELPER_CONSUME_TIMEOUT);
        let started_at = Instant::now();
        let deadline = started_at + limit;

        let client = async_nats::connect(nats_bindings.connection_url.as_str())
            .await
            .map_err(map_backend_error)?;

        // Subscribe before publishing so the subscription is registered with the server.
        let mut subscription = client
            .subscribe(full_subject.clone())
            .await
            .map_err(map_backend_error)?;

        client
            .publish(full_subject.clone(), Bytes::copy_from_slice(payload))
            .await
            .map_err(map_backend_error)?;

        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait_slice = remaining.min(Duration::from_millis(200));
            if wait_slice.is_zero() {
                break;
            }

            match tokio::time::timeout(wait_slice, subscription.next()).await {
                Ok(Some(message)) => {
                    let consumed = ConsumedMessage {
                        key: None,
                        payload: Some(message.payload.to_vec()),
                        source: message.subject.to_string(),
                    };
                    let _ = client.drain().await;
                    return Ok(consumed);
                }
                Ok(None) => break,
                Err(_) => {}
            }
        }

        let _ = client.drain().await;
        let elapsed = started_at.elapsed();
        Err(ShakedownError::HelperTimeout {
            resource: full_subject,
            operation: "nats.publish_and_await",
            timeout: limit,
            elapsed,
        })
    }

    /// Subscribe to the namespaced subject and collect messages until `predicate` returns true for
    /// the **latest** received message, or until `timeout` elapses.
    ///
    /// Returns every message received in order, including the first that satisfied `predicate`.
    /// When `timeout` is `None`, uses [`DEFAULT_HELPER_CONSUME_TIMEOUT`] (20 seconds).
    pub async fn subscribe_until<F>(
        &self,
        subject: &str,
        predicate: F,
        timeout: Option<Duration>,
    ) -> Result<Vec<ConsumedMessage>, ShakedownError>
    where
        F: Fn(&ConsumedMessage) -> bool,
    {
        let nats_bindings = self.nats_bindings()?;
        let full_subject = nats_bindings.subject_namespace(subject);
        let limit = timeout.unwrap_or(DEFAULT_HELPER_CONSUME_TIMEOUT);
        let started_at = Instant::now();
        let deadline = started_at + limit;

        let client = async_nats::connect(nats_bindings.connection_url.as_str())
            .await
            .map_err(map_backend_error)?;
        let mut subscription = client
            .subscribe(full_subject.clone())
            .await
            .map_err(map_backend_error)?;

        let mut collected: Vec<ConsumedMessage> = Vec::new();

        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait_slice = remaining.min(Duration::from_millis(200));
            if wait_slice.is_zero() {
                break;
            }

            match tokio::time::timeout(wait_slice, subscription.next()).await {
                Ok(Some(message)) => {
                    let consumed = ConsumedMessage {
                        key: None,
                        payload: Some(message.payload.to_vec()),
                        source: message.subject.to_string(),
                    };
                    let matches = predicate(&consumed);
                    collected.push(consumed);
                    if matches {
                        let _ = client.drain().await;
                        return Ok(collected);
                    }
                }
                Ok(None) => break,
                Err(_) => {}
            }
        }

        let _ = client.drain().await;
        let elapsed = started_at.elapsed();
        Err(ShakedownError::HelperTimeout {
            resource: full_subject,
            operation: "nats.subscribe_until",
            timeout: limit,
            elapsed,
        })
    }
}
