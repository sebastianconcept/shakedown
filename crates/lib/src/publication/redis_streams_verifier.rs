//! [`RedisStreamsPublicationVerifier`] — Redis Streams implementation of [`super::PublicationVerifier`].
//!
//! Reads from a **scenario-namespaced stream key** (see [`PublicationObserveSpec::logical_channel`]
//! plus [`crate::resources::RedisBindings::key_prefix`]). Matching uses
//! [`super::matching::message_matches_aggregate_id`] on the configured field (default `payload`),
//! which must hold JSON with an `aggregate_id` field (or match by key — see matching helper).

use std::time::{Duration, Instant};

use async_trait::async_trait;
use redis::streams::{StreamId, StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;

use crate::error::{InfrastructureError, ShakedownError};
use crate::helpers::map_backend_error;
use crate::lifecycle::Context;

use super::matching::message_matches_aggregate_id;
use super::spec::PublicationObserveSpec;
use super::PublicationVerifier;

const REDIS_STREAMS_VERIFIER_CONSUMER: &str = "shakedown-verifier";

fn as_backend_error(error: impl std::error::Error + Send + Sync + 'static) -> ShakedownError {
    ShakedownError::Infrastructure(InfrastructureError::Backend(Box::new(error)))
}

fn payload_field_bytes(entry: &StreamId, field_name: &str) -> Option<Vec<u8>> {
    if let Some(bytes) = entry.get::<Vec<u8>>(field_name) {
        return Some(bytes);
    }
    entry.get::<String>(field_name).map(String::into_bytes)
}

/// Options for [`RedisStreamsPublicationVerifier`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisStreamsPublicationVerifierOptions {
    /// Field name within the stream entry that contains the JSON payload.
    /// Default: `"payload"`.
    pub payload_field: String,
    /// Optional consumer group for `XREADGROUP` semantics.
    /// When `None`, uses plain `XREAD` from the start of the stream (`0-0`).
    pub consumer_group: Option<String>,
}

impl Default for RedisStreamsPublicationVerifierOptions {
    fn default() -> Self {
        Self {
            payload_field: "payload".to_string(),
            consumer_group: None,
        }
    }
}

/// First-party Redis Streams publication verifier using [`Context::redis_bindings`].
#[derive(Debug, Default, Clone, Copy)]
pub struct RedisStreamsPublicationVerifier;

async fn ensure_consumer_group(
    connection: &mut redis::aio::MultiplexedConnection,
    stream_key: &str,
    group_name: &str,
) -> Result<(), ShakedownError> {
    let result: Result<(), redis::RedisError> = connection
        .xgroup_create_mkstream(stream_key, group_name, "0")
        .await;
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            let message = error.to_string();
            if message.contains("BUSYGROUP") {
                Ok(())
            } else {
                Err(as_backend_error(error))
            }
        }
    }
}

#[async_trait]
impl PublicationVerifier for RedisStreamsPublicationVerifier {
    type Options = RedisStreamsPublicationVerifierOptions;

    async fn assert_observed(
        &self,
        context: &Context,
        spec: &PublicationObserveSpec,
        options: &Self::Options,
    ) -> Result<(), ShakedownError> {
        let redis_bindings = context.redis_bindings().ok_or_else(|| {
            ShakedownError::Infrastructure(InfrastructureError::NotProvisioned(
                "redis bindings".to_string(),
            ))
        })?;

        let stream_key = format!(
            "{}{}",
            redis_bindings.key_prefix,
            spec.logical_channel.as_str()
        );

        let client = redis::Client::open(redis_bindings.connection_url.as_str())
            .map_err(map_backend_error)?;
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(map_backend_error)?;

        let payload_field = options.payload_field.as_str();
        let deadline = Instant::now() + spec.resolved_timeout();

        if let Some(group_name) = options.consumer_group.as_deref() {
            ensure_consumer_group(&mut connection, stream_key.as_str(), group_name).await?;

            while Instant::now() < deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let block_ms = remaining.as_millis().min(200) as usize;
                if block_ms == 0 {
                    break;
                }

                let read_options = StreamReadOptions::default()
                    .block(block_ms)
                    .count(100)
                    .group(group_name, REDIS_STREAMS_VERIFIER_CONSUMER);

                let reply: Option<StreamReadReply> = connection
                    .xread_options(&[stream_key.as_str()], &[">"], &read_options)
                    .await
                    .map_err(as_backend_error)?;

                if let Some(reply) = reply {
                    for stream in reply.keys {
                        for entry in stream.ids {
                            let aggregate_ok = payload_field_bytes(&entry, payload_field)
                                .map(|bytes| {
                                    message_matches_aggregate_id(
                                        None,
                                        Some(bytes.as_slice()),
                                        spec.aggregate_id.as_str(),
                                    )
                                })
                                .unwrap_or(false);

                            if aggregate_ok {
                                let _: usize = connection
                                    .xack(stream_key.as_str(), group_name, &[entry.id.as_str()])
                                    .await
                                    .map_err(as_backend_error)?;
                                return Ok(());
                            }

                            let _: usize = connection
                                .xack(stream_key.as_str(), group_name, &[entry.id.as_str()])
                                .await
                                .map_err(as_backend_error)?;
                        }
                    }
                }
            }
        } else {
            while Instant::now() < deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let wait_slice = remaining.min(Duration::from_millis(200));
                let block_ms = wait_slice.as_millis() as usize;
                if block_ms == 0 {
                    break;
                }

                let read_options = StreamReadOptions::default().block(block_ms).count(100);

                let reply: Option<StreamReadReply> = connection
                    .xread_options(&[stream_key.as_str()], &["0-0"], &read_options)
                    .await
                    .map_err(as_backend_error)?;

                if let Some(reply) = reply {
                    for stream in reply.keys {
                        for entry in stream.ids {
                            if let Some(bytes) = payload_field_bytes(&entry, payload_field) {
                                if message_matches_aggregate_id(
                                    None,
                                    Some(bytes.as_slice()),
                                    spec.aggregate_id.as_str(),
                                ) {
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            }
        }

        Err(ShakedownError::OutboxPublicationNotFound {
            transport: "redis_streams",
            channel: stream_key,
            aggregate_id: spec.aggregate_id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RedisStreamsPublicationVerifierOptions;

    #[test]
    fn redis_streams_options_default() {
        let options = RedisStreamsPublicationVerifierOptions::default();
        assert_eq!(options.payload_field, "payload");
        assert!(options.consumer_group.is_none());
    }
}
