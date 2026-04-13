//! [`InfrastructureError`] — infrastructure setup, readiness, and teardown.

use std::error::Error;

use thiserror::Error;

/// Error types related to infrastructure setup, readiness, and teardown.
///
/// These errors indicate failures in the underlying infrastructure
/// that are not caused by the scenario logic itself.
///
/// Variants that wrap an underlying failure use [`std::error::Error::source`] (via `#[source]`) for
/// chaining. For [`crate::fault_injection::Error`], map at the call site with
/// `.map_err(InfrastructureError::FaultInjection)?` (no `From` impl — see project rust-style).
#[derive(Debug, Error)]
pub enum InfrastructureError {
    /// Infrastructure error: Failed to provision {resource}: {source}
    ///
    /// Cause: Docker, driver, or network failure while starting the resource, or invalid config.
    #[error("Infrastructure error: Failed to provision {resource}: {source}")]
    Provision {
        /// Resource name or type identifier that failed to provision.
        resource: String,
        /// Underlying Docker, driver, or network error.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    /// Infrastructure error: Resource '{resource}' not ready: {source}
    ///
    /// Cause: TCP up but deep readiness failed (migrations, auth, broker metadata, etc.).
    #[error("Infrastructure error: Resource '{resource}' not ready: {source}")]
    Readiness {
        /// Resource name or type identifier that failed the readiness check.
        resource: String,
        /// Underlying readiness probe error.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    /// Infrastructure error: Service '{resource}' pre_start hook failed: {source}
    ///
    /// Cause: a callback registered on the service spec returned an error after seed and before the
    /// service container was started (`services` feature).
    #[error("Infrastructure error: Service '{resource}' pre_start hook failed: {source}")]
    ServicePreStart {
        /// Service label (same form as other service infrastructure errors, e.g. `service:web`).
        resource: String,
        /// Error returned from the user hook.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    /// Infrastructure error: Failed to teardown {resource}: {source}
    ///
    /// Cause: container stop/remove failed, connection leak, or timeout during cleanup.
    #[error("Infrastructure error: Failed to teardown {resource}: {source}")]
    Teardown {
        /// Resource name or type identifier that failed teardown.
        resource: String,
        /// Underlying teardown error.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    /// Infrastructure error: Resource '{0}' already provisioned
    ///
    /// Cause: duplicate registration or double provision for the same logical resource name.
    #[error("Infrastructure error: Resource '{0}' already provisioned")]
    AlreadyProvisioned(String),
    /// Infrastructure error: Resource '{0}' was not provisioned
    ///
    /// Cause: scenario code expects a handle (e.g. Kafka bindings) but that resource never ran provision in this run.
    #[error("Infrastructure error: Resource '{0}' was not provisioned")]
    NotProvisioned(String),
    /// Infrastructure error: Environment backend error: {0}
    ///
    /// Cause: I/O or integration failure in the environment backend (process, Docker socket, etc.).
    #[error("Infrastructure error: Environment backend error: {0}")]
    Backend(#[source] Box<dyn Error + Send + Sync>),
    /// Infrastructure error: Fault injection (Toxiproxy control API): {0}
    ///
    /// Cause: Toxiproxy HTTP API error, missing proxy, or harness invariant (see [`crate::fault_injection::Error`]).
    #[cfg(feature = "fault-injection")]
    #[error("Infrastructure error: Fault injection (Toxiproxy control API): {0}")]
    FaultInjection(#[source] crate::fault_injection::Error),
}
