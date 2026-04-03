//! [`ScenarioError`] — scenario execution failures.

use std::error::Error;

use thiserror::Error;

use super::{AssertionError, ResourceNotFoundError, StepError};

/// Error types related to scenario execution.
///
/// These errors indicate failures in the user's scenario logic,
/// assertions, or explicit step failures.
///
/// Nested assertion / step / resource errors and [`ScenarioError::Other`] participate in
/// [`std::error::Error::source`] chains via `#[source]`.
#[derive(Debug, Error)]
pub enum ScenarioError {
    /// Scenario assertion error: {0}
    ///
    /// Cause: mock service call counts, outbox/payload checks, or other [`AssertionError`] variants failed.
    #[error("Scenario assertion error: {0}")]
    Assertion(#[source] AssertionError),
    /// Scenario step failed error: {0}
    ///
    /// Cause: scenario code reported an explicit failure (user message in [`StepError`]).
    #[error("Scenario step failed error: {0}")]
    StepFailed(#[source] StepError),
    /// Scenario resource not found error: {0}
    ///
    /// Cause: a named handle or binding was missing (wrong name or provision order).
    #[error("Scenario resource not found error: {0}")]
    ResourceNotFound(#[source] ResourceNotFoundError),
    /// Scenario timeout error: {timeout:?}
    ///
    /// Cause: the scenario body or a step exceeded the configured wall-clock limit.
    #[error("Scenario timeout error: {timeout:?}")]
    Timeout { timeout: std::time::Duration },
    /// Scenario error: {0}
    ///
    /// Cause: boxed error returned from scenario or extension code.
    #[error("Scenario error: {0}")]
    Other(#[source] Box<dyn Error + Send + Sync>),
}
