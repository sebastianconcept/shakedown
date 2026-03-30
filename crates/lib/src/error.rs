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

use displaydoc::Display;
use thiserror::Error;

use crate::phase::Phase;

/// Categorized assertion failure during scenario execution (see [`ScenarioError::Assertion`]).
///
/// Bridge-specific variants carry structured fields; use [`AssertionError::Unspecified`] only when
/// no dedicated variant exists yet.
#[derive(Debug, Display, Error)]
#[ignore_extra_doc_attributes]
pub enum AssertionError {
    /// bridge `{bridge_name}`: expected {expected} total calls, got {actual}; recorded: {recorded_calls_diagnostic}
    ///
    /// Cause: total inbound calls to the bridge proxy did not match (see [`crate::bridge::BridgeHandle::assert_called_times`]).
    BridgeCallCountMismatch {
        bridge_name: String,
        expected: usize,
        actual: usize,
        recorded_calls_diagnostic: String,
    },
    /// bridge `{bridge_name}`: expected {expected} calls to `{method} {path}`, got {actual}; recorded: {recorded_calls_diagnostic}
    ///
    /// Cause: call count for a specific route did not match (see [`crate::bridge::BridgeHandle::assert_path_called_times`]).
    BridgePathCallCountMismatch {
        bridge_name: String,
        method: String,
        path: String,
        expected: usize,
        actual: usize,
        recorded_calls_diagnostic: String,
    },
    /// bridge `{bridge_name}`: no call to `{method} {path}` satisfied the predicate; recorded: {recorded_calls_diagnostic}
    ///
    /// Cause: no recorded call matched the custom predicate (see [`crate::bridge::BridgeHandle::assert_called_with`]).
    BridgePredicateUnmatched {
        bridge_name: String,
        method: String,
        path: String,
        recorded_calls_diagnostic: String,
    },
    /// bridge `{bridge_name}`: expected no calls, got {actual}; recorded: {recorded_calls_diagnostic}
    ///
    /// Cause: the bridge should have seen no traffic but calls were recorded.
    BridgeUnexpectedCalls {
        bridge_name: String,
        actual: usize,
        recorded_calls_diagnostic: String,
    },
    /// bridge `{bridge_name}`: expected no calls to `{method} {path}`, got {matching_call_count}; recorded: {recorded_calls_diagnostic}
    ///
    /// Cause: a specific route should have been unused but matching calls exist.
    BridgePathUnexpectedCalls {
        bridge_name: String,
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

/// Explicit scenario step failure (see [`ScenarioError::StepFailed`]).
///
/// Add a new variant when a call site can classify the failure; use [`StepError::Unspecified`]
/// until then.
#[derive(Debug, Display, Error)]
#[ignore_extra_doc_attributes]
pub enum StepError {
    /// {detail}
    ///
    /// Cause: scenario returned or propagated an explicit failure with this message.
    Unspecified { detail: String },
}

/// Required scenario resource was not available (see [`ScenarioError::ResourceNotFound`]).
///
/// Use [`ResourceNotFoundError::Missing`] when the resource is identified by a registered name;
/// use [`ResourceNotFoundError::Unspecified`] only when no dedicated variant exists yet.
#[derive(Debug, Display, Error)]
#[ignore_extra_doc_attributes]
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

/// Top-level error type for all Shakedown errors.
///
/// Errors are categorized into three main types:
/// - [`Scenario`](ShakedownError::Scenario): Errors related to scenario execution
/// - [`Infrastructure`](ShakedownError::Infrastructure): Errors related to infrastructure setup/teardown
/// - [`Internal`](ShakedownError::Internal): Unexpected errors in the framework itself
///
/// [`Display`] is derived via [`displaydoc`]. Nested [`InfrastructureError`] / [`InternalError`]
/// values use pass-through formatting (`{0}`) so their own messages keep a single
/// “Infrastructure error:” / “Internal error:” prefix (avoiding duplication when also wrapped).
#[derive(Debug, Display)]
#[ignore_extra_doc_attributes]
pub enum ShakedownError {
    /// Scenario error: {0}
    ///
    /// Scenario phase: assertions, explicit step failures, missing resources, timeouts, or boxed
    /// errors from user code.
    Scenario(ScenarioError),
    /// {0}
    ///
    /// Infrastructure phase: Docker, databases, brokers, Toxiproxy, or environment backend (see
    /// [`InfrastructureError`]). Display is delegated — the inner value already starts with
    /// “Infrastructure error:”.
    Infrastructure(InfrastructureError),
    /// {0}
    ///
    /// Harness or framework bug: poisoned mutex, invariant violation, invalid harness config (see
    /// [`InternalError`]). Display is delegated — the inner value already starts with “Internal error:”.
    Internal(InternalError),
    /// No relational database binding on [`crate::lifecycle::Context`]. Outbox row helpers need exactly one of postgres, mariadb, mysql, or sqlite provisioned.
    ///
    /// Cause: `ctx.outbox()` (or similar) was used without registering and provisioning a relational resource.
    NoRelationalBindings,
    /// Outbox row not found for aggregate_id='{aggregate_id}', event_type='{event_type}', expected status='{expected_status}'
    ///
    /// Cause: no row matches the aggregate id and event type, or the row exists but not in the expected terminal status (timing, wrong id, or application never wrote the event).
    OutboxRowNotFound {
        aggregate_id: String,
        event_type: String,
        expected_status: String,
    },
    /// Outbox row in wrong state for aggregate_id='{aggregate_id}', event_type='{event_type}': expected '{expected}', got '{actual}'
    ///
    /// Cause: a row was found but its status column (or equivalent) does not match the assertion.
    OutboxWrongState {
        aggregate_id: String,
        event_type: String,
        expected: String,
        actual: String,
    },
    /// Outbox row has unknown status '{status}' for aggregate_id='{aggregate_id}', event_type='{event_type}'
    ///
    /// Cause: the stored status value is not one the harness recognises — often a schema or application bug.
    OutboxUnknownState {
        status: String,
        aggregate_id: String,
        event_type: String,
    },
    /// Outbox retry below threshold for aggregate_id='{aggregate_id}': attempts={attempts} min_attempts={min_attempts}
    ///
    /// Cause: a retry-count assertion expected at least `min_attempts` attempts but observed fewer (often eventual consistency or backoff).
    OutboxRetryBelowThreshold {
        aggregate_id: String,
        attempts: i32,
        min_attempts: i32,
    },
    /// [{transport}] Outbox publication not found for aggregate_id='{aggregate_id}' (channel='{channel}')
    ///
    /// Cause: no matching message was observed on the bus within the proof window (Kafka topic, Rabbit queue/exchange, etc.) — distinct from relational outbox row assertions.
    OutboxPublicationNotFound {
        transport: &'static str,
        channel: String,
        aggregate_id: String,
    },
    /// [{transport}] Outbox publication observation timed out for aggregate_id='{aggregate_id}' (channel='{channel}')
    ///
    /// Cause: the observation window expired before a matching publication appeared (slow pipeline, wrong channel, or aggregate id mismatch).
    OutboxPublicationTimeout {
        transport: &'static str,
        channel: String,
        aggregate_id: String,
    },
    /// Outbox payload deserialisation failed: {0}
    ///
    /// Cause: bytes from the outbox row could not be parsed as the expected type (schema drift or corruption).
    PayloadDeserialisation(String),
    /// JSON payload serialisation failed: {0}
    ///
    /// Cause: `serde_json::to_vec` failed when building a message body (e.g. typed helper `produce_json` / `publish_json`).
    PayloadSerialisation(String),
    /// Outbox payload assertion failed for aggregate_id='{aggregate_id}', event_type='{event_type}'
    ///
    /// Cause: the payload was deserialised but the scenario’s predicate rejected it.
    PayloadAssertionFailed {
        aggregate_id: String,
        event_type: String,
    },
    /// Resource assertion failed for container '{container}', metric '{metric}': measured {measured} exceeds threshold {threshold} (phase: {phase:?})
    ///
    /// Cause: diagnostics resource sample (CPU, memory, …) crossed the bound declared in the scenario (see [`crate::event::InfraEvent::ResourceSample`]).
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
    HelperTimeout {
        resource: String,
        operation: &'static str,
        timeout: std::time::Duration,
        elapsed: std::time::Duration,
    },
}

// `std::error::Error` is implemented by hand for the top-level harness enums (not via `thiserror`'s
// derive) because `source()` must thread through `Box<dyn Error + Send + Sync>` and mixed variant
// shapes; `#[source]` on those boxed fields does not match the chaining we want for every variant.
impl Error for ShakedownError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ShakedownError::Scenario(e) => Some(e),
            ShakedownError::Infrastructure(e) => Some(e),
            ShakedownError::Internal(e) => Some(e),
            ShakedownError::NoRelationalBindings => None,
            ShakedownError::OutboxRowNotFound { .. } => None,
            ShakedownError::OutboxWrongState { .. } => None,
            ShakedownError::OutboxUnknownState { .. } => None,
            ShakedownError::OutboxRetryBelowThreshold { .. } => None,
            ShakedownError::OutboxPublicationNotFound { .. } => None,
            ShakedownError::OutboxPublicationTimeout { .. } => None,
            ShakedownError::PayloadDeserialisation(_) => None,
            ShakedownError::PayloadSerialisation(_) => None,
            ShakedownError::PayloadAssertionFailed { .. } => None,
            ShakedownError::ResourceAssertionFailed { .. } => None,
            ShakedownError::HelperTimeout { .. } => None,
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

/// Error types related to scenario execution.
///
/// These errors indicate failures in the user's scenario logic,
/// assertions, or explicit step failures.
///
/// [`Display`] is implemented via [`displaydoc`] on each variant’s doc comment (the first paragraph
/// is the format string); following paragraphs document cause and troubleshooting for agents.
#[derive(Debug, Display)]
#[ignore_extra_doc_attributes]
pub enum ScenarioError {
    /// Scenario assertion error: {0}
    ///
    /// Cause: bridge call counts, outbox/payload checks, or other [`AssertionError`] variants failed.
    Assertion(AssertionError),
    /// Scenario step failed error: {0}
    ///
    /// Cause: scenario code reported an explicit failure (user message in [`StepError`]).
    StepFailed(StepError),
    /// Scenario resource not found error: {0}
    ///
    /// Cause: a named handle or binding was missing (wrong name or provision order).
    ResourceNotFound(ResourceNotFoundError),
    /// Scenario timeout error: {timeout:?}
    ///
    /// Cause: the scenario body or a step exceeded the configured wall-clock limit.
    Timeout { timeout: std::time::Duration },
    /// Scenario error: {0}
    ///
    /// Cause: boxed error returned from scenario or extension code.
    Other(Box<dyn Error + Send + Sync>),
}

// See comment on `impl Error for ShakedownError` — manual `source()` for boxed `Other` and nested
// assertion/step/resource errors.
impl Error for ScenarioError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ScenarioError::Assertion(assertion) => Some(assertion),
            ScenarioError::StepFailed(step) => Some(step),
            ScenarioError::ResourceNotFound(resource) => Some(resource),
            ScenarioError::Timeout { .. } => None,
            ScenarioError::Other(e) => Some(e.as_ref()),
        }
    }
}

/// Error types related to infrastructure setup, readiness, and teardown.
///
/// These errors indicate failures in the underlying infrastructure
/// that are not caused by the scenario logic itself.
///
/// [`Display`] is derived via [`displaydoc`]. For [`crate::fault_injection::Error`], map at the
/// call site with `.map_err(InfrastructureError::FaultInjection)?` (no `From` impl — see project
/// rust-style).
#[derive(Debug, Display)]
#[ignore_extra_doc_attributes]
pub enum InfrastructureError {
    /// Infrastructure error: Failed to provision {resource}: {source}
    ///
    /// Cause: Docker, driver, or network failure while starting the resource, or invalid config.
    Provision {
        resource: String,
        source: Box<dyn Error + Send + Sync>,
    },
    /// Infrastructure error: Resource '{resource}' not ready: {source}
    ///
    /// Cause: TCP up but deep readiness failed (migrations, auth, broker metadata, etc.).
    Readiness {
        resource: String,
        source: Box<dyn Error + Send + Sync>,
    },
    /// Infrastructure error: Failed to teardown {resource}: {source}
    ///
    /// Cause: container stop/remove failed, connection leak, or timeout during cleanup.
    Teardown {
        resource: String,
        source: Box<dyn Error + Send + Sync>,
    },
    /// Infrastructure error: Resource '{0}' already provisioned
    ///
    /// Cause: duplicate registration or double provision for the same logical resource name.
    AlreadyProvisioned(String),
    /// Infrastructure error: Resource '{0}' was not provisioned
    ///
    /// Cause: scenario code expects a handle (e.g. Kafka bindings) but that resource never ran provision in this run.
    NotProvisioned(String),
    /// Infrastructure error: Environment backend error: {0}
    ///
    /// Cause: I/O or integration failure in the environment backend (process, Docker socket, etc.).
    Backend(Box<dyn Error + Send + Sync>),
    /// Infrastructure error: Fault injection (Toxiproxy control API): {0}
    ///
    /// Cause: Toxiproxy HTTP API error, missing proxy, or harness invariant (see [`crate::fault_injection::Error`]).
    #[cfg(feature = "fault-injection")]
    FaultInjection(crate::fault_injection::Error),
}

// See comment on `impl Error for ShakedownError` — manual `source()` for `Box<dyn Error>` sources.
impl Error for InfrastructureError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            InfrastructureError::Provision { source, .. } => Some(source.as_ref()),
            InfrastructureError::Readiness { source, .. } => Some(source.as_ref()),
            InfrastructureError::Teardown { source, .. } => Some(source.as_ref()),
            InfrastructureError::AlreadyProvisioned(_) => None,
            InfrastructureError::NotProvisioned(_) => None,
            InfrastructureError::Backend(e) => Some(e.as_ref()),
            #[cfg(feature = "fault-injection")]
            InfrastructureError::FaultInjection(err) => Some(err),
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
///
/// [`Display`] is derived via [`displaydoc`]. Backtraces on [`InternalError::WithBacktrace`] are not
/// printed in [`Display`] (attach a debugger or log `Backtrace` when reporting).
#[derive(Debug, Display)]
#[ignore_extra_doc_attributes]
pub enum InternalError {
    /// Internal error: Invariant violation: {0}
    ///
    /// Cause: a harness invariant was broken (should not happen in normal runs — report as a bug).
    InvariantViolation(String),
    /// Internal error: Invalid configuration: {0}
    ///
    /// Cause: lifecycle or resource wiring is inconsistent (e.g. missing prerequisite before run).
    InvalidConfiguration(String),
    /// Internal error: Unexpected state: {0}
    ///
    /// Cause: harness state machine saw an illegal transition or stale handle.
    UnexpectedState(String),
    /// Internal error: Framework bug: {0}
    ///
    /// Cause: explicit framework defect path; please report with a minimal repro.
    Bug(String),
    /// Internal error: {msg}
    ///
    /// Cause: internal failure with captured backtrace (not shown in this message).
    WithBacktrace {
        msg: String,
        #[doc(hidden)]
        backtrace: std::backtrace::Backtrace,
    },
    /// Internal error: Poisoned mutex ({0}): another thread panicked while holding the lock
    ///
    /// Cause: another thread panicked while holding a mutex used by the harness; the lock is poisoned.
    PoisonedMutex(String),
}

// See comment on `impl Error for ShakedownError` — variants are mostly leaf errors (`source` none).
impl Error for InternalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            InternalError::InvariantViolation(_) => None,
            InternalError::InvalidConfiguration(_) => None,
            InternalError::UnexpectedState(_) => None,
            InternalError::Bug(_) => None,
            InternalError::WithBacktrace { .. } => None,
            InternalError::PoisonedMutex(_) => None,
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
        // Given a scenario assertion error carrying an unspecified detail
        let err = ScenarioError::Assertion(AssertionError::Unspecified {
            detail: "assertion failed".to_string(),
        });
        // When we format it for display
        let displayed = format!("{}", err);
        // Then the output includes the scenario assertion prefix and detail
        assert_eq!(displayed, "Scenario assertion error: assertion failed");
    }

    #[test]
    fn scenario_error_step_failed_display() {
        // Given an explicit step failure with a detail message
        let err = ScenarioError::StepFailed(StepError::Unspecified {
            detail: "user aborted".to_string(),
        });
        // When we format it for display
        let displayed = format!("{}", err);
        // Then the scenario step prefix and detail appear
        assert_eq!(displayed, "Scenario step failed error: user aborted");
    }

    #[test]
    fn scenario_error_resource_not_found_display() {
        // Given a missing named resource
        let err = ScenarioError::ResourceNotFound(ResourceNotFoundError::Missing {
            resource_name: "kafka".to_string(),
        });
        // When we format it for display
        let displayed = format!("{}", err);
        // Then the resource name is included
        assert_eq!(displayed, "Scenario resource not found error: kafka");
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
    fn internal_error_poisoned_mutex_display() {
        // Given a poisoned-mutex internal error with a subsystem context label
        let err = InternalError::PoisonedMutex("resource stats rings".to_string());
        // When we format it for display
        let displayed = format!("{}", err);
        // Then the message names poisoned mutex and includes the context
        assert!(displayed.contains("Poisoned mutex"));
        assert!(displayed.contains("resource stats rings"));
    }

    #[test]
    fn shakedown_error_from_scenario() {
        // Given a scenario-level error
        let scenario_err = ScenarioError::Assertion(AssertionError::Unspecified {
            detail: "failed".to_string(),
        });
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
        let err = ScenarioError::Assertion(AssertionError::Unspecified {
            detail: "test".to_string(),
        });
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

    #[cfg(feature = "fault-injection")]
    #[test]
    fn infrastructure_fault_injection_display_and_source() {
        let inner = crate::fault_injection::Error::NotAvailable;
        let infra_err = InfrastructureError::FaultInjection(inner);
        let displayed = format!("{}", infra_err);
        assert!(displayed.contains("Fault injection"), "{displayed}");
        assert!(infra_err.source().is_some());
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
    fn outbox_publication_not_found_display_includes_transport() {
        let err = ShakedownError::OutboxPublicationNotFound {
            transport: "kafka",
            channel: "orders.events".to_string(),
            aggregate_id: "a-1".to_string(),
        };
        let displayed = format!("{}", err);
        assert!(displayed.contains("[kafka]"));
        assert!(displayed.contains("aggregate_id='a-1'"));
        assert!(displayed.contains("channel='orders.events'"));
    }

    #[test]
    fn outbox_publication_timeout_display_includes_transport() {
        let err = ShakedownError::OutboxPublicationTimeout {
            transport: "rabbitmq",
            channel: "orders.q".to_string(),
            aggregate_id: "a-2".to_string(),
        };
        let displayed = format!("{}", err);
        assert!(displayed.contains("[rabbitmq]"));
        assert!(displayed.contains("timed out"));
        assert!(displayed.contains("aggregate_id='a-2'"));
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
