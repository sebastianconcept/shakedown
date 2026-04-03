//! NATS typed helpers — [`NatsHelpers`] for scenario-scoped publish and subscribe (core pub/sub).

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
