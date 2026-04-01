//! Publication observation types and the [`PublicationVerifier`] trait.
//!
//! Used by outbox helpers to prove that messages reached a bus after relational outbox state
//! assertions. Implementations are feature-gated (`kafka`, `rabbitmq`, `nats`, `redis` / `valkey`,
//! …); this module defines the shared contract only.

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
