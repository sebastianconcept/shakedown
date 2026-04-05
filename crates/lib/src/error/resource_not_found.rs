//! [`ResourceNotFoundError`] — required scenario resource was not available.

use displaydoc::Display;
use thiserror::Error;

/// Required scenario resource was not available (see [`crate::error::ScenarioError::ResourceNotFound`]).
///
/// Use [`ResourceNotFoundError::Missing`] when the resource is identified by a registered name;
/// use [`ResourceNotFoundError::Unspecified`] only when no dedicated variant exists yet.
#[derive(Debug, Display, Error)]
#[ignore_extra_doc_attributes]
#[allow(missing_docs)]
pub enum ResourceNotFoundError {
    /// {resource_name}
    ///
    /// Cause: no registered or provisioned resource matches this name in the current scenario.
    Missing { resource_name: String },
    /// {detail}
    ///
    /// Cause: generic message until a dedicated variant exists.
    Unspecified { detail: String },
}
