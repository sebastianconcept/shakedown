use std::error::Error;

use super::*;
use crate::OutboxError;

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
fn infrastructure_error_service_pre_start_display() {
    // Given a service pre_start hook failure wrapping an I/O error
    let err = InfrastructureError::ServicePreStart {
        resource: "service:web".to_string(),
        source: Box::new(std::io::Error::other("hook failed")),
    };
    // When we format it for display
    let displayed = format!("{}", err);
    // Then the message names pre_start and the service label
    assert!(displayed.contains("pre_start"), "{displayed}");
    assert!(displayed.contains("service:web"), "{displayed}");
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
    let err: ShakedownError = OutboxError::PublicationNotFound {
        transport: "kafka",
        channel: "orders.events".to_string(),
        aggregate_id: "a-1".to_string(),
    }
    .into();
    let displayed = format!("{}", err);
    assert!(displayed.contains("[kafka]"));
    assert!(displayed.contains("aggregate_id='a-1'"));
    assert!(displayed.contains("channel='orders.events'"));
}

#[test]
fn outbox_publication_timeout_display_includes_transport() {
    let err: ShakedownError = OutboxError::PublicationTimeout {
        transport: "rabbitmq",
        channel: "orders.q".to_string(),
        aggregate_id: "a-2".to_string(),
    }
    .into();
    let displayed = format!("{}", err);
    assert!(displayed.contains("[rabbitmq]"));
    assert!(displayed.contains("timed out"));
    assert!(displayed.contains("aggregate_id='a-2'"));
}

#[test]
fn payload_serialisation_error_chains_serde_source() {
    let inner = serde_json::from_str::<i32>("not-json").unwrap_err();
    let err = ShakedownError::PayloadSerialisation(inner);
    assert!(err.source().is_some());
}

#[test]
fn outbox_json_payload_error_chains_serde_source() {
    let inner = serde_json::from_str::<serde_json::Value>("not-json").unwrap_err();
    let outbox_err = OutboxError::from(inner);
    assert!(outbox_err.source().is_some());
    let harness_err: ShakedownError = outbox_err.into();
    assert!(harness_err.source().is_some());
}

#[cfg(feature = "load")]
#[test]
fn performance_assertion_failed_display_includes_fields() {
    use crate::error::PerformanceAssertionDetails;

    let err = ShakedownError::PerformanceAssertionFailed(Box::new(PerformanceAssertionDetails {
        load_name: "checkout".to_string(),
        metric: "p99".to_string(),
        measured: "12.00ms".to_string(),
        threshold: "<= 10.00ms".to_string(),
        phase: crate::load::Phase::pause(std::time::Duration::ZERO),
        comparison_table: None,
    }));
    let displayed = format!("{err}");
    assert!(displayed.contains("checkout"));
    assert!(displayed.contains("p99"));
    assert!(displayed.contains("12.00ms"));
    assert!(displayed.contains("10.00ms"));
    assert!(displayed.contains("Pause"));
}

#[test]
fn helper_timeout_bus_predicate_helpers_match_produce_and_await_operations() {
    let kafka = ShakedownError::HelperTimeout {
        resource: "t".to_string(),
        operation: "kafka.produce_and_await",
        timeout: std::time::Duration::from_secs(1),
        elapsed: std::time::Duration::from_secs(2),
    };
    assert!(kafka.is_kafka_produce_and_await_timeout());
    assert!(!kafka.is_rabbit_publish_and_await_timeout());
    assert!(!kafka.is_nats_publish_and_await_timeout());

    let rabbit = ShakedownError::HelperTimeout {
        resource: "q".to_string(),
        operation: "rabbit.publish_and_await",
        timeout: std::time::Duration::from_secs(1),
        elapsed: std::time::Duration::from_secs(2),
    };
    assert!(rabbit.is_rabbit_publish_and_await_timeout());
    assert!(!rabbit.is_kafka_produce_and_await_timeout());

    let nats = ShakedownError::HelperTimeout {
        resource: "s".to_string(),
        operation: "nats.publish_and_await",
        timeout: std::time::Duration::from_secs(1),
        elapsed: std::time::Duration::from_secs(2),
    };
    assert!(nats.is_nats_publish_and_await_timeout());
    assert!(!nats.is_kafka_produce_and_await_timeout());
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
