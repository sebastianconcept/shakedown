//! [`NatsPublicationVerifier`] — NATS JetStream implementation of [`super::PublicationVerifier`].
//!
//! The verifier ensures a JetStream stream captures the scenario-namespaced subject, then reads
//! persisted messages with an ephemeral pull consumer (`DeliverPolicy::All`). Because JetStream
//! stores publishes, the natural flow is **publish first, verify after** — no subscribe-first timing
//! constraint.

use std::time::{Duration, Instant};

use async_nats::jetstream;
use async_nats::jetstream::consumer::DeliverPolicy;
use async_trait::async_trait;
use futures_util::StreamExt;

use crate::error::{InfrastructureError, IntoInfrastructureError, ShakedownError};
use crate::lifecycle::Context;
use crate::OutboxError;

use super::matching::message_matches_aggregate_id;
use super::spec::PublicationObserveSpec;
use super::PublicationVerifier;

/// Maximum JetStream stream name length enforced by NATS.
const MAX_JETSTREAM_STREAM_NAME_LEN: usize = 255;

fn as_backend_error(error: impl std::error::Error + Send + Sync + 'static) -> ShakedownError {
    error.into_infrastructure_error().into()
}

fn as_async_nats_boxed_error(error: async_nats::Error) -> ShakedownError {
    ShakedownError::Infrastructure(InfrastructureError::Backend(error))
}

/// Default JetStream stream name for a scenario-namespaced subject: `SD_` + alphanumeric segments
/// (other characters become `_`), truncated to the server name limit.
pub(crate) fn default_stream_name_for_namespaced_subject(namespaced_subject: &str) -> String {
    let sanitized: String = namespaced_subject
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect();
    let mut base = if sanitized.is_empty() {
        "shakedown".to_string()
    } else {
        sanitized
    };
    let prefix = "SD_";
    if prefix.len() + base.len() > MAX_JETSTREAM_STREAM_NAME_LEN {
        let max_base = MAX_JETSTREAM_STREAM_NAME_LEN.saturating_sub(prefix.len());
        base.truncate(max_base.max(1));
    }
    format!("{prefix}{base}")
}

/// Options for [`NatsPublicationVerifier`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NatsPublicationVerifierOptions {
    /// JetStream stream name. If empty (after trim), [`NatsPublicationVerifier`] derives a name
    /// via [`default_stream_name_for_namespaced_subject`] from the namespaced subject.
    pub stream_name: String,
}

/// First-party NATS publication verifier using JetStream on [`Context::nats_bindings`] subjects.
#[derive(Debug, Default, Clone, Copy)]
pub struct NatsPublicationVerifier;

#[async_trait]
impl PublicationVerifier for NatsPublicationVerifier {
    type Options = NatsPublicationVerifierOptions;

    async fn assert_observed(
        &self,
        context: &Context,
        spec: &PublicationObserveSpec,
        options: &Self::Options,
    ) -> Result<(), ShakedownError> {
        let nats_bindings = context.nats_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "nats bindings".to_string(),
            ))
        })?;

        let subject = nats_bindings.subject_namespace(spec.logical_channel.as_str());
        let stream_name = if options.stream_name.trim().is_empty() {
            default_stream_name_for_namespaced_subject(&subject)
        } else {
            options.stream_name.clone()
        };

        let client = async_nats::connect(nats_bindings.connection_url.as_str())
            .await
            .map_err(as_backend_error)?;
        let jetstream = jetstream::new(client);

        let stream = jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name,
                subjects: vec![subject.clone()],
                ..Default::default()
            })
            .await
            .map_err(as_backend_error)?;

        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: None,
                deliver_policy: DeliverPolicy::All,
                ..Default::default()
            })
            .await
            .map_err(as_backend_error)?;

        let mut batch = consumer
            .stream()
            .max_messages_per_batch(100)
            .messages()
            .await
            .map_err(as_backend_error)?;

        let deadline = Instant::now() + spec.resolved_timeout();
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait_slice = remaining.min(Duration::from_millis(200));
            if wait_slice.is_zero() {
                break;
            }

            match tokio::time::timeout(wait_slice, batch.next()).await {
                Ok(Some(Ok(message))) => {
                    if message_matches_aggregate_id(
                        None,
                        Some(message.payload.as_ref()),
                        spec.aggregate_id.as_str(),
                    ) {
                        message.ack().await.map_err(as_async_nats_boxed_error)?;
                        return Ok(());
                    }
                    message.ack().await.map_err(as_async_nats_boxed_error)?;
                }
                Ok(Some(Err(error))) => return Err(as_backend_error(error)),
                Ok(None) => {}
                Err(_) => {}
            }
        }

        Err(OutboxError::PublicationNotFound {
            transport: "nats",
            channel: subject,
            aggregate_id: spec.aggregate_id.clone(),
        }
        .into())
    }
}
