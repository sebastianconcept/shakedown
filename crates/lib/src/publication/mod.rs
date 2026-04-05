//! Publication observation types and the [`PublicationVerifier`] trait.
//!
//! **Why:** proving that a message was written to the outbox table is not enough — the relay must
//! also have published it to the broker and the broker must have delivered it to a consumer. This
//! module closes that observability gap with a uniform assertion surface regardless of which
//! message bus your service uses.
//!
//! **How:** after asserting outbox row state with the [`crate::outbox`] helpers, obtain a
//! verifier for your bus (e.g. [`KafkaPublicationVerifier`] when the `kafka` feature is enabled)
//! and call `verifier.observe(spec).await` to assert that the expected messages arrived within the
//! configured timeout. Feature-gated implementations are provided for Kafka, RabbitMQ, NATS, and
//! Redis Streams; the [`PublicationVerifier`] trait lets you add custom transports.
//!
//! # Book reference
//!
//! Part II, *Chapter 12: Outbox Pattern* — covers the full pipeline assertion model: outbox row
//! state, publication observation, and how `PublicationVerifier` composes with outbox helpers.

mod matching;
mod spec;
mod verifier;

#[cfg(feature = "kafka")]
mod kafka_verifier;

#[cfg(feature = "rabbitmq")]
mod rabbit_verifier;

#[cfg(feature = "nats")]
mod nats_verifier;

#[cfg(any(feature = "redis", feature = "valkey"))]
mod redis_streams_verifier;

pub use matching::message_matches_aggregate_id;
pub use spec::{PublicationObserveSpec, DEFAULT_PUBLICATION_OBSERVE_TIMEOUT};
pub use verifier::PublicationVerifier;

#[cfg(feature = "kafka")]
pub use kafka_verifier::KafkaPublicationVerifier;

#[cfg(feature = "rabbitmq")]
pub use rabbit_verifier::{RabbitMqPublicationVerifier, RabbitMqPublicationVerifierOptions};

#[cfg(feature = "nats")]
pub use nats_verifier::{NatsPublicationVerifier, NatsPublicationVerifierOptions};

#[cfg(any(feature = "redis", feature = "valkey"))]
pub use redis_streams_verifier::{
    RedisStreamsPublicationVerifier, RedisStreamsPublicationVerifierOptions,
};

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use crate::error::ShakedownError;
    use crate::lifecycle::Context;

    use super::{PublicationObserveSpec, PublicationVerifier};

    struct NopVerifier;

    #[async_trait]
    impl PublicationVerifier for NopVerifier {
        type Options = ();

        async fn assert_observed(
            &self,
            _context: &Context,
            _spec: &PublicationObserveSpec,
            _options: &Self::Options,
        ) -> Result<(), ShakedownError> {
            Ok(())
        }
    }

    #[test]
    fn publication_verifier_trait_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        let verifier = NopVerifier;
        assert_send_sync(&verifier);
        let _boxed: Box<dyn PublicationVerifier<Options = ()>> = Box::new(verifier);
    }
}
