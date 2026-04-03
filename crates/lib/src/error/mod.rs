//! Error types for the Shakedown harness.
//!
//! What: a small hierarchy ([`ShakedownError`] → scenario / infrastructure / internal) so callers
//! classify failures without string matching.
//!
//! Why: scenario bugs, flaky infra, and framework bugs need different handling and reporting.
//!
//! How: return [`Result`] from lifecycle and resource APIs; wrap or map at boundaries when
//! integrating with application code.

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
