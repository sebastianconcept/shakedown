//! Error types for the Shakedown harness.
//!
//! When a scenario fails, knowing *why* it failed matters as much as knowing *that* it
//! failed. A bug in your service logic, a flaky container startup, and a framework bug all require
//! different responses. A typed hierarchy lets callers know how to react fast instead of having to
//! understand that after interpreting many logs filtered by string matching.
//! It also lets the failure timeline surface the right root-cause hint automatically.
//!
//! Shakedown lifecycle and resource APIs return `Result<_, ShakedownError>`. The top-level
//! [`ShakedownError`] wraps [`ScenarioError`] (your logic/assertions failed),
//! [`InfrastructureError`] (provision or readiness failed), or [`InternalError`] (framework bug).
//! Use `.map_err(OuterError::Variant)?` at call sites so the conversion point is visible for
//! an ergonomic causability tracing; avoiding `.map_err(..::from)?` that would hide the error source in a false convenience.
//!
//! # Book reference
//!
//! Cross-cutting — the error taxonomy is introduced in Part I and referenced throughout; Part IV,
//! *Chapter 18: Diagnostics* explains how error variants drive the failure timeline and root-cause
//! hints.

mod assertion;
mod infrastructure;
mod internal;
mod into_infrastructure;
mod resource_handle;
mod resource_not_found;
mod scenario;
mod scenario_id;
mod shakedown;
mod step;

pub use assertion::AssertionError;
pub use infrastructure::InfrastructureError;
pub use internal::InternalError;
pub use into_infrastructure::IntoInfrastructureError;
pub use resource_handle::ResourceHandle;
pub use resource_not_found::ResourceNotFoundError;
pub use scenario::ScenarioError;
pub use scenario_id::ScenarioId;
#[cfg(feature = "load")]
pub use shakedown::PerformanceAssertionDetails;
pub use shakedown::ShakedownError;
pub use step::StepError;

/// Result type alias for Shakedown operations.
pub type Result<T> = std::result::Result<T, ShakedownError>;

#[cfg(test)]
mod tests;
