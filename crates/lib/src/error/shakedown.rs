//! [`ShakedownError`] — top-level harness error type.

use thiserror::Error;

use crate::lifecycle::Phase;
use crate::OutboxError;

use super::{
    AssertionError, InfrastructureError, InternalError, ResourceNotFoundError, ScenarioError,
    StepError,
};

/// Structured fields for [`ShakedownError::PerformanceAssertionFailed`] (`load` feature).
#[cfg(feature = "load")]
#[derive(Debug)]
pub struct PerformanceAssertionDetails {
    /// Load name from [`crate::load::LoadHandle::load_name`] or the author-supplied label.
    pub load_name: String,
    /// Which aggregate was checked (e.g. `p99`, `throughput`).
    pub metric: String,
    /// Observed value (human-readable).
    pub measured: String,
    /// Asserted bound (human-readable).
    pub threshold: String,
    /// Load profile phase used for correlation (not lifecycle phase).
    pub phase: crate::load::Phase,
    /// Per-metric baseline vs current table (US-007 regression checks).
    pub comparison_table: Option<String>,
}

#[cfg(feature = "load")]
impl std::fmt::Display for PerformanceAssertionDetails {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "performance assertion failed for load '{}', metric '{}': measured {}, threshold {} (load phase: {:?})",
            self.load_name, self.metric, self.measured, self.threshold, self.phase
        )?;
        if let Some(table) = &self.comparison_table {
            write!(formatter, "\n\n{table}")?;
        }
        Ok(())
    }
}

/// Top-level error type for all Shakedown errors.
///
/// Errors are categorized into three main types:
/// - [`Scenario`](ShakedownError::Scenario): Errors related to scenario execution
/// - [`Infrastructure`](ShakedownError::Infrastructure): Errors related to infrastructure setup/teardown
/// - [`Internal`](ShakedownError::Internal): Unexpected errors in the framework itself
///
/// Nested [`InfrastructureError`], [`InternalError`], [`ScenarioError`], and [`OutboxError`] use
/// pass-through `{0}` display where appropriate so inner messages keep their prefixes.
#[derive(Debug, Error)]
pub enum ShakedownError {
    /// Scenario error: {0}
    ///
    /// Scenario phase: assertions, explicit step failures, missing resources, timeouts, or boxed
    /// errors from user code.
    #[error("Scenario error: {0}")]
    Scenario(#[source] ScenarioError),
    /// {0}
    ///
    /// Infrastructure phase: Docker, databases, brokers, Toxiproxy, or environment backend (see
    /// [`InfrastructureError`]). Display is delegated — the inner value already starts with
    /// “Infrastructure error:”.
    #[error("{0}")]
    Infrastructure(#[source] InfrastructureError),
    /// {0}
    ///
    /// Harness or framework bug: poisoned mutex, invariant violation, invalid harness config (see
    /// [`InternalError`]). Display is delegated — the inner value already starts with “Internal error:”.
    #[error("{0}")]
    Internal(#[source] InternalError),
    /// {0}
    ///
    /// Cause: outbox storage bindings, row state assertions, publication observation, or payload checks
    /// (see [`crate::OutboxError`]).
    #[error("{0}")]
    Outbox(#[source] OutboxError),
    /// JSON payload serialisation failed: {0}
    ///
    /// Cause: `serde_json::to_vec` failed when building a message body (e.g. typed helper `produce_json` / `publish_json`).
    #[error("JSON payload serialisation failed: {0}")]
    PayloadSerialisation(#[source] serde_json::Error),
    /// Resource assertion failed for container '{container}', metric '{metric}': measured {measured} exceeds threshold {threshold} (phase: {phase:?})
    ///
    /// Cause: diagnostics resource sample (CPU, memory, …) crossed the bound declared in the scenario (see [`crate::event::InfraEvent::ResourceSample`]).
    #[error("Resource assertion failed for container '{container}', metric '{metric}': measured {measured} exceeds threshold {threshold} (phase: {phase:?})")]
    ResourceAssertionFailed {
        container: String,
        metric: String,
        measured: String,
        threshold: String,
        phase: Phase,
    },
    /// Helper timed out: resource '{resource}', operation '{operation}', waited {elapsed:?} (limit {timeout:?})
    ///
    /// Cause: a typed helper (Kafka consume, RabbitMQ consume, …) did not observe a matching message within the wall-clock window.
    #[error("Helper timed out: resource '{resource}', operation '{operation}', waited {elapsed:?} (limit {timeout:?})")]
    HelperTimeout {
        resource: String,
        operation: &'static str,
        timeout: std::time::Duration,
        elapsed: std::time::Duration,
    },
    /// {0}
    ///
    /// Cause: [`crate::load::LoadMetricsAssertBuilder`] SLO check did not pass (see [`crate::load::LoadMetrics::assert`] / [`crate::load::LoadMetrics::assert_with_phase`]).
    /// Boxed so [`ShakedownError`] stays small for [`Result`] (`clippy::result_large_err`).
    #[cfg(feature = "load")]
    #[error("{0}")]
    PerformanceAssertionFailed(Box<PerformanceAssertionDetails>),
}

impl From<ScenarioError> for ShakedownError {
    fn from(err: ScenarioError) -> Self {
        ShakedownError::Scenario(err)
    }
}

impl From<InfrastructureError> for ShakedownError {
    fn from(err: InfrastructureError) -> Self {
        ShakedownError::Infrastructure(err)
    }
}

impl From<InternalError> for ShakedownError {
    fn from(err: InternalError) -> Self {
        ShakedownError::Internal(err)
    }
}

impl From<AssertionError> for ShakedownError {
    fn from(err: AssertionError) -> Self {
        ShakedownError::Scenario(ScenarioError::Assertion(err))
    }
}

impl From<StepError> for ShakedownError {
    fn from(err: StepError) -> Self {
        ShakedownError::Scenario(ScenarioError::StepFailed(err))
    }
}

impl From<ResourceNotFoundError> for ShakedownError {
    fn from(err: ResourceNotFoundError) -> Self {
        ShakedownError::Scenario(ScenarioError::ResourceNotFound(err))
    }
}

impl From<OutboxError> for ShakedownError {
    fn from(err: OutboxError) -> Self {
        ShakedownError::Outbox(err)
    }
}
