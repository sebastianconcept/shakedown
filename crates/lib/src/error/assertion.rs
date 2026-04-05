//! [`AssertionError`] — categorized assertion failure during scenario execution.

use displaydoc::Display;
use thiserror::Error;

/// Categorized assertion failure during scenario execution (see [`crate::error::ScenarioError::Assertion`]).
///
/// Mock-service-specific variants carry structured fields; use [`AssertionError::Unspecified`] only when
/// no dedicated variant exists yet.
#[derive(Debug, Display, Error)]
#[ignore_extra_doc_attributes]
#[allow(missing_docs)]
pub enum AssertionError {
    /// mock service `{mock_service_name}`: expected {expected} total calls, got {actual}; recorded: {recorded_calls_diagnostic}
    ///
    /// Cause: total inbound calls to the mock service proxy did not match (see [`crate::mock_service::MockServiceHandle::assert_called_times`]).
    MockServiceCallCountMismatch {
        mock_service_name: String,
        expected: usize,
        actual: usize,
        recorded_calls_diagnostic: String,
    },
    /// mock service `{mock_service_name}`: expected {expected} calls to `{method} {path}`, got {actual}; recorded: {recorded_calls_diagnostic}
    ///
    /// Cause: call count for a specific route did not match (see [`crate::mock_service::MockServiceHandle::assert_path_called_times`]).
    MockServicePathCallCountMismatch {
        mock_service_name: String,
        method: String,
        path: String,
        expected: usize,
        actual: usize,
        recorded_calls_diagnostic: String,
    },
    /// mock service `{mock_service_name}`: no call to `{method} {path}` satisfied the predicate; recorded: {recorded_calls_diagnostic}
    ///
    /// Cause: no recorded call matched the custom predicate (see [`crate::mock_service::MockServiceHandle::assert_called_with`]).
    MockServicePredicateUnmatched {
        mock_service_name: String,
        method: String,
        path: String,
        recorded_calls_diagnostic: String,
    },
    /// mock service `{mock_service_name}`: expected no calls, got {actual}; recorded: {recorded_calls_diagnostic}
    ///
    /// Cause: the mock service should have seen no traffic but calls were recorded.
    MockServiceUnexpectedCalls {
        mock_service_name: String,
        actual: usize,
        recorded_calls_diagnostic: String,
    },
    /// mock service `{mock_service_name}`: expected no calls to `{method} {path}`, got {matching_call_count}; recorded: {recorded_calls_diagnostic}
    ///
    /// Cause: a specific route should have been unused but matching calls exist.
    MockServicePathUnexpectedCalls {
        mock_service_name: String,
        method: String,
        path: String,
        matching_call_count: usize,
        recorded_calls_diagnostic: String,
    },
    /// {detail}
    ///
    /// Cause: generic assertion message until a dedicated variant exists.
    Unspecified { detail: String },
}
