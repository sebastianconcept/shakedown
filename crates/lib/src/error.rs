//! Error types for Shakedown testing framework.
//!
//! This module defines a structured error hierarchy that distinguishes
//! between scenario errors, infrastructure errors, and internal errors.

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
}

impl std::fmt::Display for ShakedownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShakedownError::Scenario(e) => write!(f, "Scenario error: {}", e),
            ShakedownError::Infrastructure(e) => write!(f, "Infrastructure error: {}", e),
            ShakedownError::Internal(e) => write!(f, "Internal error: {}", e),
        }
    }
}

impl Error for ShakedownError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ShakedownError::Scenario(e) => Some(e),
            ShakedownError::Infrastructure(e) => Some(e),
            ShakedownError::Internal(e) => Some(e),
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

/// A structured binding handle for a resource (Kafka, Postgres, Redis, etc.).
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

/// Newtype wrapper for scenario identifiers (used for isolation keys, e.g. Kafka topic prefixes).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ScenarioId(String);

impl ScenarioId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn empty() -> Self {
        Self(String::new())
    }

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

/// Convenience trait for converting any error to an infrastructure error.
pub trait IntoInfrastructureError: Error + Send + Sync {
    /// Convert self to an infrastructure error.
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
        let err = ScenarioError::Assertion("assertion failed".to_string());
        assert_eq!(
            format!("{}", err),
            "Scenario assertion error: assertion failed"
        );
    }

    #[test]
    fn infrastructure_error_display() {
        let err = InfrastructureError::Provision {
            resource: "kafka".to_string(),
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "connection refused",
            )),
        };
        assert!(format!("{}", err).contains("kafka"));
    }

    #[test]
    fn internal_error_display() {
        let err = InternalError::InvariantViolation("state must be valid".to_string());
        assert_eq!(
            format!("{}", err),
            "Internal error: Invariant violation: state must be valid"
        );
    }

    #[test]
    fn shakedown_error_from_scenario() {
        let scenario_err = ScenarioError::Assertion("failed".to_string());
        let shakedown_err: ShakedownError = scenario_err.into();
        assert!(matches!(shakedown_err, ShakedownError::Scenario(_)));
    }

    #[test]
    fn shakedown_error_from_infrastructure() {
        let infra_err = InfrastructureError::Provision {
            resource: "kafka".to_string(),
            source: Box::new(std::io::Error::other("failed")),
        };
        let shakedown_err: ShakedownError = infra_err.into();
        assert!(matches!(shakedown_err, ShakedownError::Infrastructure(_)));
    }

    #[test]
    fn shakedown_error_from_internal() {
        let internal_err = InternalError::Bug("unknown bug".to_string());
        let shakedown_err: ShakedownError = internal_err.into();
        assert!(matches!(shakedown_err, ShakedownError::Internal(_)));
    }

    #[test]
    fn scenario_error_is_send_sync() {
        let err = ScenarioError::Assertion("test".to_string());
        fn check_send_sync<T: Send + Sync>(_: T) {}
        check_send_sync(err);
    }

    #[test]
    fn infrastructure_error_is_send_sync() {
        let infra_err = InfrastructureError::Teardown {
            resource: "test".to_string(),
            source: Box::new(std::io::Error::other("failed")),
        };
        fn check_send_sync<T: Send + Sync>(_: T) {}
        check_send_sync(infra_err);
    }

    #[test]
    fn internal_error_is_send_sync() {
        let internal_err = InternalError::InvariantViolation("test".to_string());
        fn check_send_sync<T: Send + Sync>(_: T) {}
        check_send_sync(internal_err);
    }

    #[test]
    fn test_scenario_id() {
        let id = ScenarioId::new("sc_001");
        assert_eq!(id.to_string(), "sc_001");

        let empty_id = ScenarioId::empty();
        assert_eq!(empty_id.to_string(), "");
    }

    #[test]
    fn resource_handle_serialization() {
        use serde_json::to_value;

        let handle = ResourceHandle {
            id: "test_resource".to_string(),
            connection_string: "localhost:5432".to_string(),
            metadata: Some(serde_json::json!({"host": "localhost", "port": 5432})),
        };

        let serialized = to_value(&handle).unwrap();
        assert!(serialized.as_object().is_some());
    }
}
