//! Publication observation types and the [`PublicationVerifier`] trait.
//!
//! Used by outbox helpers to prove that messages reached a bus after relational outbox state
//! assertions. Implementations are feature-gated (`kafka`, `rabbitmq`, …); this module defines
//! the shared contract only.

mod spec;
mod verifier;

pub use spec::{PublicationObserveSpec, DEFAULT_PUBLICATION_OBSERVE_TIMEOUT};
pub use verifier::PublicationVerifier;

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
