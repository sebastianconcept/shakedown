//! [`StepError`] — explicit scenario step failure.

use displaydoc::Display;
use thiserror::Error;

/// Explicit scenario step failure (see [`crate::error::ScenarioError::StepFailed`]).
///
/// Add a new variant when a call site can classify the failure; use [`StepError::Unspecified`]
/// until then.
#[derive(Debug, Display, Error)]
#[ignore_extra_doc_attributes]
#[allow(missing_docs)]
pub enum StepError {
    /// {detail}
    ///
    /// Cause: scenario returned or propagated an explicit failure with this message.
    Unspecified { detail: String },
}
