//! Observation specification for publication proof across message transports.

use std::time::Duration;

/// Default observation window when [`PublicationObserveSpec::timeout`] is [`None`].
///
/// Verifiers should treat this as the ceiling unless they document a different default.
pub const DEFAULT_PUBLICATION_OBSERVE_TIMEOUT: Duration = Duration::from_secs(20);

/// What to look for when asserting that a publication for an aggregate was observed on a bus.
///
/// `logical_channel` is transport-neutral: Kafka base topic name, RabbitMQ queue or exchange
/// name, or another bus-specific routing surface — see each verifier’s documentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationObserveSpec {
    /// Domain aggregate identifier (matches outbox row and message key/body conventions).
    pub aggregate_id: String,
    /// Transport-neutral channel name (topic, queue, exchange label, etc.).
    pub logical_channel: String,
    /// Maximum time to wait for a matching publication, or [`None`] to use
    /// [`DEFAULT_PUBLICATION_OBSERVE_TIMEOUT`].
    pub timeout: Option<Duration>,
}

impl PublicationObserveSpec {
    /// Resolve the observation duration, using [`DEFAULT_PUBLICATION_OBSERVE_TIMEOUT`] when
    /// `timeout` is [`None`].
    pub fn resolved_timeout(&self) -> Duration {
        self.timeout.unwrap_or(DEFAULT_PUBLICATION_OBSERVE_TIMEOUT)
    }
}
