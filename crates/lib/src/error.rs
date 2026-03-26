//! Error types for the Shakedown harness.
//!
//! What: a small hierarchy ([`crate::error::ShakedownError`] → scenario / infrastructure / internal) so callers
//! classify failures without string matching.
//!
//! Why: scenario bugs, flaky infra, and framework bugs need different handling and reporting.
//!
//! How: return [`Result`] from lifecycle and resource APIs; wrap or map at boundaries when
//! integrating with application code.

use std::error::Error;

/// Top-level error type for all Shakedown errors.
///
/// Errors are categorized into three main types:
/// - [`Scenario`](ShakedownError::Scenario): Errors related to scenario execution
/// - [`Infrastructure`](ShakedownError::Infrastructure): Errors related to infrastructure setup/teardown
/// - [`Internal`](ShakedownError::Internal): Unexpected errors in the framework itself
#[derive(Debug)]
pub enum ShakedownError {
    /// Scenario-related errors (assertion failures, explicit step failures)
    Scenario(ScenarioError),
    /// Infrastructure-related errors (provision, readiness, teardown failures)
    Infrastructure(InfrastructureError),
    /// Internal errors (unexpected bugs, invariant violations)
    Internal(InternalError),
    /// Outbox helpers require one relational binding in Context
    NoRelationalBindings,
    /// Outbox assertion expected row was not found
    OutboxRowNotFound {
        aggregate_id: String,
        event_type: String,
        expected_status: String,
    },
    /// Outbox assertion found row in unexpected state
    OutboxWrongState {
        aggregate_id: String,
        event_type: String,
        expected: String,
        actual: String,
    },
    /// Outbox retry assertion failed threshold check
    OutboxRetryBelowThreshold {
        aggregate_id: String,
        attempts: i32,
        min_attempts: i32,
    },
    /// Outbox Kafka assertion did not find a matching record before timeout
    OutboxKafkaRecordNotFound { topic: String, aggregate_id: String },
}

impl std::fmt::Display for ShakedownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShakedownError::Scenario(e) => write!(f, "Scenario error: {}", e),
            ShakedownError::Infrastructure(e) => write!(f, "Infrastructure error: {}", e),
            ShakedownError::Internal(e) => write!(f, "Internal error: {}", e),
            ShakedownError::NoRelationalBindings => {
                write!(
                    f,
                    "No relational bindings are set on Context (enable postgres, mariadb, mysql, or sqlite and provision one resource)"
                )
            }
            ShakedownError::OutboxRowNotFound {
                aggregate_id,
                event_type,
                expected_status,
            } => write!(
                f,
                "Outbox row not found for aggregate_id='{}', event_type='{}', expected status='{}'",
                aggregate_id, event_type, expected_status
            ),
            ShakedownError::OutboxWrongState {
                aggregate_id,
                event_type,
                expected,
                actual,
            } => write!(
                f,
                "Outbox row in wrong state for aggregate_id='{}', event_type='{}': expected '{}', got '{}'",
                aggregate_id, event_type, expected, actual
            ),
            ShakedownError::OutboxRetryBelowThreshold {
                aggregate_id,
                attempts,
                min_attempts,
            } => write!(
                f,
                "Outbox retry below threshold for aggregate_id='{}': attempts={} min_attempts={}",
                aggregate_id, attempts, min_attempts
            ),
            ShakedownError::OutboxKafkaRecordNotFound { topic, aggregate_id } => write!(
                f,
                "Outbox Kafka record not found for aggregate_id='{}' in topic='{}' before timeout",
                aggregate_id, topic
            ),
        }
    }
}

impl Error for ShakedownError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ShakedownError::Scenario(e) => Some(e),
            ShakedownError::Infrastructure(e) => Some(e),
            ShakedownError::Internal(e) => Some(e),
            ShakedownError::NoRelationalBindings => None,
            ShakedownError::OutboxRowNotFound { .. } => None,
            ShakedownError::OutboxWrongState { .. } => None,
            ShakedownError::OutboxRetryBelowThreshold { .. } => None,
            ShakedownError::OutboxKafkaRecordNotFound { .. } => None,
        }
    }
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

/// Error types related to scenario execution.
///
/// These errors indicate failures in the user's scenario logic,
/// assertions, or explicit step failures.
#[derive(Debug)]
pub enum ScenarioError {
    /// An assertion failed during scenario execution
    Assertion(String),
    /// A step in the scenario failed explicitly
    StepFailed(String),
    /// A required resource was not found
    ResourceNotFound(String),
    /// A scenario timeout occurred
    Timeout {
        /// The timeout duration
        timeout: std::time::Duration,
    },
    /// Other scenario-specific errors
    Other(Box<dyn Error + Send + Sync>),
}

impl std::fmt::Display for ScenarioError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScenarioError::Assertion(msg) => write!(f, "Scenario assertion error: {}", msg),
            ScenarioError::StepFailed(msg) => write!(f, "Scenario step failed error: {}", msg),
            ScenarioError::ResourceNotFound(name) => {
                write!(f, "Scenario resource not found error: {}", name)
            }
            ScenarioError::Timeout { timeout } => {
                write!(f, "Scenario timeout error: {:?}", timeout)
            }
            ScenarioError::Other(e) => write!(f, "Scenario error: {}", e),
        }
    }
}

impl Error for ScenarioError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ScenarioError::Assertion(_) => None,
            ScenarioError::StepFailed(_) => None,
            ScenarioError::ResourceNotFound(_) => None,
            ScenarioError::Timeout { .. } => None,
            ScenarioError::Other(e) => Some(e.as_ref()),
        }
    }
}

/// Error types related to infrastructure setup, readiness, and teardown.
///
/// These errors indicate failures in the underlying infrastructure
/// that are not caused by the scenario logic itself.
#[derive(Debug)]
pub enum InfrastructureError {
    /// Failed to provision a resource
    Provision {
        /// The resource that failed to provision
        resource: String,
        /// The underlying error
        source: Box<dyn Error + Send + Sync>,
    },
    /// Failed to check resource readiness
    Readiness {
        /// The resource that failed readiness check
        resource: String,
        /// The underlying error
        source: Box<dyn Error + Send + Sync>,
    },
    /// Failed to teardown a resource
    Teardown {
        /// The resource that failed to teardown
        resource: String,
        /// The underlying error
        source: Box<dyn Error + Send + Sync>,
    },
    /// A resource was already provisioned
    AlreadyProvisioned(String),
    /// A resource was not provisioned when expected
    NotProvisioned(String),
    /// Environment backend error
    Backend(Box<dyn Error + Send + Sync>),
}

impl std::fmt::Display for InfrastructureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InfrastructureError::Provision { resource, source } => write!(
                f,
                "Infrastructure error: Failed to provision {}: {}",
                resource, source
            ),
            InfrastructureError::Readiness { resource, source } => write!(
                f,
                "Infrastructure error: Resource '{}' not ready: {}",
                resource, source
            ),
            InfrastructureError::Teardown { resource, source } => write!(
                f,
                "Infrastructure error: Failed to teardown {}: {}",
                resource, source
            ),
            InfrastructureError::AlreadyProvisioned(name) => write!(
                f,
                "Infrastructure error: Resource '{}' already provisioned",
                name
            ),
            InfrastructureError::NotProvisioned(name) => write!(
                f,
                "Infrastructure error: Resource '{}' was not provisioned",
                name
            ),
            InfrastructureError::Backend(e) => {
                write!(f, "Infrastructure error: Environment backend error: {}", e)
            }
        }
    }
}

impl Error for InfrastructureError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            InfrastructureError::Provision { source, .. } => Some(source.as_ref()),
            InfrastructureError::Readiness { source, .. } => Some(source.as_ref()),
            InfrastructureError::Teardown { source, .. } => Some(source.as_ref()),
            InfrastructureError::AlreadyProvisioned(_) => None,
            InfrastructureError::NotProvisioned(_) => None,
            InfrastructureError::Backend(e) => Some(e.as_ref()),
        }
    }
}

/// Connection details returned after a resource is provisioned for a scenario.
///
/// Exists so scenario code and clients use one typed handle (id, connection string, optional JSON
/// metadata) instead of ad hoc tuples. Produced by [`crate::resources::Resource::provision`] and
/// consumed when configuring clients or asserting isolation (e.g. Kafka bootstrap + topic prefix
/// in `metadata`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResourceHandle {
    /// Unique identifier for this logical slice of the resource.
    /// May include scenario prefix, database name, or other namespace info.
    pub id: String,
    /// Connection string that clients use to connect.
    pub connection_string: String,
    /// Optional extra metadata (e.g., port mapping, container ID).
    pub metadata: Option<serde_json::Value>,
}

/// Identifier for one scenario run, carried through provision and teardown for isolation.
///
/// Why a newtype: distinguishes scenario ids from other strings (logs, resource names) and keeps
/// isolation keys (Kafka prefixes, DB names) in one place.
///
/// How: set by [`crate::lifecycle::run_scenario`] from the scenario name; read via
/// [`crate::lifecycle::Context::scenario_id`] inside the scenario body and in
/// [`crate::resources::Resource::provision`].
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ScenarioId(String);

impl ScenarioId {
    /// Construct an id from any string-like value (typically the Rust test function name).
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Empty id before the harness assigns a scenario name.
    pub fn empty() -> Self {
        Self(String::new())
    }

    /// View the id as `str` for formatting and isolation helpers.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ScenarioId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error types related to internal framework bugs or invariant violations.
///
/// These errors indicate problems with the Shakedown framework itself,
/// not with user scenarios or infrastructure.
#[derive(Debug)]
pub enum InternalError {
    /// An invariant was violated
    InvariantViolation(String),
    /// An invalid configuration was provided
    InvalidConfiguration(String),
    /// An unexpected state was encountered
    UnexpectedState(String),
    /// A bug in the framework
    Bug(String),
    /// An internal error with backtrace for debugging
    WithBacktrace {
        /// The error message
        msg: String,
        #[doc(hidden)]
        backtrace: std::backtrace::Backtrace,
    },
}

impl std::fmt::Display for InternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InternalError::InvariantViolation(msg) => {
                write!(f, "Internal error: Invariant violation: {}", msg)
            }
            InternalError::InvalidConfiguration(msg) => {
                write!(f, "Internal error: Invalid configuration: {}", msg)
            }
            InternalError::UnexpectedState(msg) => {
                write!(f, "Internal error: Unexpected state: {}", msg)
            }
            InternalError::Bug(msg) => write!(f, "Internal error: Framework bug: {}", msg),
            InternalError::WithBacktrace { msg, .. } => write!(f, "Internal error: {}", msg),
        }
    }
}

impl Error for InternalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            InternalError::InvariantViolation(_) => None,
            InternalError::InvalidConfiguration(_) => None,
            InternalError::UnexpectedState(_) => None,
            InternalError::Bug(_) => None,
            InternalError::WithBacktrace { .. } => None,
        }
    }
}

/// Result type alias for Shakedown operations.
pub type Result<T> = std::result::Result<T, ShakedownError>;

/// Ergonomic conversion of `std::error::Error` values into [`InfrastructureError::Backend`].
///
/// Implemented for all `Error + Send + Sync + 'static` types so environment or IO failures can be
/// wrapped without bespoke match arms at every site.
pub trait IntoInfrastructureError: Error + Send + Sync {
    /// Box `self` as the source of an [`InfrastructureError::Backend`].
    fn into_infrastructure_error(self) -> InfrastructureError;
}

impl<T> IntoInfrastructureError for T
where
    T: Error + Send + Sync + 'static,
{
    fn into_infrastructure_error(self) -> InfrastructureError {
        InfrastructureError::Backend(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_error_display() {
        // Given a scenario assertion error carrying a message
        let err = ScenarioError::Assertion("assertion failed".to_string());
        // When we format it for display
        let displayed = format!("{}", err);
        // Then the output includes the scenario assertion prefix and message
        assert_eq!(displayed, "Scenario assertion error: assertion failed");
    }

    #[test]
    fn infrastructure_error_display() {
        // Given a provision failure for a named resource wrapping an I/O error
        let err = InfrastructureError::Provision {
            resource: "kafka".to_string(),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "connection refused",
            )),
        };
        // When we format it for display
        let displayed = format!("{}", err);
        // Then the resource name appears in the message
        assert!(displayed.contains("kafka"));
    }

    #[test]
    fn internal_error_display() {
        // Given an invariant violation internal error
        let err = InternalError::InvariantViolation("state must be valid".to_string());
        // When we format it for display
        let displayed = format!("{}", err);
        // Then the output matches the internal invariant wording
        assert_eq!(
            displayed,
            "Internal error: Invariant violation: state must be valid"
        );
    }

    #[test]
    fn shakedown_error_from_scenario() {
        // Given a scenario-level error
        let scenario_err = ScenarioError::Assertion("failed".to_string());
        // When we lift it into the top-level Shakedown error
        let shakedown_err: ShakedownError = scenario_err.into();
        // Then it is classified as a scenario error
        assert!(matches!(shakedown_err, ShakedownError::Scenario(_)));
    }

    #[test]
    fn shakedown_error_from_infrastructure() {
        // Given an infrastructure provision error
        let infra_err = InfrastructureError::Provision {
            resource: "kafka".to_string(),
            source: Box::new(std::io::Error::other("failed")),
        };
        // When we lift it into the top-level Shakedown error
        let shakedown_err: ShakedownError = infra_err.into();
        // Then it is classified as an infrastructure error
        assert!(matches!(shakedown_err, ShakedownError::Infrastructure(_)));
    }

    #[test]
    fn shakedown_error_from_internal() {
        // Given an internal framework error
        let internal_err = InternalError::Bug("unknown bug".to_string());
        // When we lift it into the top-level Shakedown error
        let shakedown_err: ShakedownError = internal_err.into();
        // Then it is classified as an internal error
        assert!(matches!(shakedown_err, ShakedownError::Internal(_)));
    }

    #[test]
    fn scenario_error_is_send_sync() {
        // Given a scenario error value
        let err = ScenarioError::Assertion("test".to_string());
        // When we require Send + Sync at the type level
        fn check_send_sync<T: Send + Sync>(_: T) {}
        // Then the compiler accepts it
        check_send_sync(err);
    }

    #[test]
    fn infrastructure_error_is_send_sync() {
        // Given an infrastructure teardown error
        let infra_err = InfrastructureError::Teardown {
            resource: "test".to_string(),
            source: Box::new(std::io::Error::other("failed")),
        };
        // When we require Send + Sync at the type level
        fn check_send_sync<T: Send + Sync>(_: T) {}
        // Then the compiler accepts it
        check_send_sync(infra_err);
    }

    #[test]
    fn internal_error_is_send_sync() {
        // Given an internal invariant error
        let internal_err = InternalError::InvariantViolation("test".to_string());
        // When we require Send + Sync at the type level
        fn check_send_sync<T: Send + Sync>(_: T) {}
        // Then the compiler accepts it
        check_send_sync(internal_err);
    }

    #[test]
    fn test_scenario_id() {
        // Given a scenario id built from a string and the empty sentinel
        let id = ScenarioId::new("sc_001");
        let empty_id = ScenarioId::empty();
        // When we display them
        // Then the non-empty id round-trips and the empty id prints as nothing
        assert_eq!(id.to_string(), "sc_001");
        assert_eq!(empty_id.to_string(), "");
    }

    #[test]
    fn resource_handle_serialization() {
        use serde_json::to_value;

        // Given a resource handle with metadata
        let handle = ResourceHandle {
            id: "test_resource".to_string(),
            connection_string: "localhost:5432".to_string(),
            metadata: Some(serde_json::json!({"host": "localhost", "port": 5432})),
        };
        // When we serialize it to JSON
        let serialized = to_value(&handle).expect("serialize resource handle");
        // Then we get a JSON object
        assert!(serialized.as_object().is_some());
    }
}
