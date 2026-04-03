//! [`NatsPublicationVerifier`] — NATS core pub/sub implementation of [`super::PublicationVerifier`].
//!
//! Subscribes to the scenario-namespaced subject before polling. NATS core does not persist messages;
//! the publisher must send **after** this verifier has subscribed — typically by calling
//! [`crate::outbox::Outbox::assert_nats_published`] concurrently with (and after a short yield to)
//! your publish step, or by scheduling publication on another task.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::stream::StreamExt;

use crate::error::{IntoInfrastructureError, ShakedownError};
use crate::lifecycle::Context;
use crate::OutboxError;

use super::matching::message_matches_aggregate_id;
use super::spec::PublicationObserveSpec;
use super::PublicationVerifier;

fn as_backend_error(error: impl std::error::Error + Send + Sync + 'static) -> ShakedownError {
    error.into_infrastructure_error().into()
}

/// Options for [`NatsPublicationVerifier`].
///
/// Core pub/sub needs no tuning today; fields will appear here if JetStream or connection tuning
/// is added later.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NatsPublicationVerifierOptions;

/// First-party NATS publication verifier using [`Context::nats_bindings`] and subject namespacing.
#[derive(Debug, Default, Clone, Copy)]
pub struct NatsPublicationVerifier;

#[async_trait]
impl PublicationVerifier for NatsPublicationVerifier {
    type Options = NatsPublicationVerifierOptions;

    async fn assert_observed(
        &self,
        context: &Context,
        spec: &PublicationObserveSpec,
        _options: &Self::Options,
    ) -> Result<(), ShakedownError> {
        let nats_bindings = context.nats_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "nats bindings".to_string(),
            ))
        })?;

        let subject = nats_bindings.subject_namespace(spec.logical_channel.as_str());
        let client = async_nats::connect(nats_bindings.connection_url.as_str())
            .await
            .map_err(as_backend_error)?;
        let mut subscription = client
            .subscribe(subject.clone())
            .await
            .map_err(as_backend_error)?;

        let deadline = Instant::now() + spec.resolved_timeout();
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait_slice = remaining.min(Duration::from_millis(200));
            if wait_slice.is_zero() {
                break;
            }

            match tokio::time::timeout(wait_slice, subscription.next()).await {
                Ok(Some(message)) => {
                    if message_matches_aggregate_id(
                        None,
                        Some(message.payload.as_ref()),
                        spec.aggregate_id.as_str(),
                    ) {
                        let _ = client.drain().await;
                        return Ok(());
                    }
                }
                Ok(None) => break,
                Err(_) => {}
            }
        }

        let _ = client.drain().await;

        Err(OutboxError::PublicationNotFound {
            transport: "nats",
            channel: subject,
            aggregate_id: spec.aggregate_id.clone(),
        }
        .into())
    }
}
