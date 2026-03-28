//! [`PublicationVerifier`] — stable contract for outbox publication proof.

use async_trait::async_trait;

use crate::error::ShakedownError;
use crate::lifecycle::Context;

use super::spec::PublicationObserveSpec;

/// Pluggable publication proof for arbitrary message transports (Kafka, RabbitMQ, custom buses).
///
/// First-party verifiers live behind optional Cargo features; the trait stays available with
/// `feature = "outbox"` alone so adopters can implement publication checks without enabling
/// Kafka or RabbitMQ.
#[async_trait]
pub trait PublicationVerifier: Send + Sync {
    /// Transport-specific options (exchange, routing key, consumer tuning, etc.).
    type Options: Send + Sync;

    /// Assert that a publication matching `spec` was observed (consumed or inspected) within the
    /// resolved timeout.
    async fn assert_observed(
        &self,
        context: &Context,
        spec: &PublicationObserveSpec,
        options: &Self::Options,
    ) -> Result<(), ShakedownError>;
}
