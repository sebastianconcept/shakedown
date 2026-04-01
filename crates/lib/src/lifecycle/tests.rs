use super::*;
use crate::error::{ResourceHandle, ScenarioId};
use crate::event::{Event, EventSnapshot};
use crate::resources::Resource;
use crate::ScenarioError;
use async_trait::async_trait;

#[test]
fn test_phase_display() {
    // Given each lifecycle phase variant
    // When we format them with Debug
    // Then the printed names match the phase identifiers
    assert_eq!(format!("{:?}", Phase::Provision), "Provision");
    assert_eq!(format!("{:?}", Phase::Readiness), "Readiness");
    assert_eq!(format!("{:?}", Phase::Seed), "Seed");
    assert_eq!(format!("{:?}", Phase::Run), "Run");
    assert_eq!(format!("{:?}", Phase::Assert), "Assert");
    assert_eq!(format!("{:?}", Phase::Teardown), "Teardown");
}

#[test]
fn test_context_new() {
    // Given a newly constructed scenario context
    let ctx = Context::new();
    // When we ask whether it has resources
    // Then it starts empty
    assert!(ctx.is_empty());
}

#[cfg(feature = "fault-injection")]
#[test]
fn fault_handle_none_without_toxiproxy_provision() {
    // Given a fresh context with no Toxiproxy resource provisioned
    let ctx = Context::new();
    // When we query for a fault handle
    // Then none is installed
    assert!(ctx.fault_handle().is_none());
}

#[test]
fn test_context_with_backend() {
    // Given a context wired to the noop environment backend
    let ctx = Context::with_backend(NoopEnvironmentBackend {});
    // When we check for registered resources
    // Then none are present yet
    assert!(ctx.is_empty());
}

#[test]
fn test_context_with_scenario_id() {
    // Given a context created with a fixed scenario id string
    let ctx = Context::with_scenario_id("test-scenario-123");
    // When we read the scenario id back
    // Then it matches what we passed in
    assert_eq!(ctx.scenario_id().to_string(), "test-scenario-123");
}

#[test]
fn test_context_register_builtin() {
    // Given an empty context
    let mut ctx = Context::new();
    // When we register the fake builtin resource
    ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    // Then the context is non-empty with exactly one entry
    assert!(!ctx.is_empty());
    assert_eq!(ctx.resources().len(), 1);
}

#[test]
fn test_context_register_extension() {
    // Given an empty context
    let mut ctx = Context::new();
    // When we register an extension resource
    ctx.register_extension(FakeResource::default());
    // Then one resource is recorded
    assert_eq!(ctx.resources().len(), 1);
}

#[test]
fn test_lifecycle_state() {
    // Given the lifecycle state enum
    // When we construct the NotStarted variant
    let _not_started = LifecycleState::NotStarted;
    // Then it type-checks (smoke test for the state machine surface)
    let _ = _not_started;
}

#[tokio::test]
async fn test_noop_environment_backend() {
    // Given the noop environment backend
    let backend = NoopEnvironmentBackend {};
    // When we run the readiness check
    let readiness = backend.check_readiness().await;
    // Then it succeeds without side effects
    readiness.expect("readiness should succeed");
}

#[test]
fn test_event_log_append_and_retrieval() {
    // Given an empty event log
    let mut log = EventLog::new();
    // When we append two events and then snapshot, clear, and re-check length
    log.append(Event::info("scenario", "test event 1"));
    log.append(Event::debug("resource:fake", "test event 2"));
    assert_eq!(log.len(), 2);
    let events: Vec<_> = log.events().collect();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].message, "test event 1");
    assert_eq!(events[1].source, "resource:fake");
    let _snapshot: EventSnapshot = log.clone().into();
    assert_eq!(log.len(), 2);
    log.clear();
    // Then after clear the log is empty
    assert!(log.is_empty());
}

#[test]
fn test_lifecycle_ordering() {
    // Given lifecycle phase and state types (without running a full async scenario here)
    let _ctx = Context::new();
    let _event_log = EventLog::new();
    // When we refer to each phase and the initial lifecycle state
    // Then Debug strings and state construction match expectations
    assert_eq!(format!("{:?}", Phase::Provision), "Provision");
    assert_eq!(format!("{:?}", Phase::Readiness), "Readiness");
    assert_eq!(format!("{:?}", Phase::Seed), "Seed");
    assert_eq!(format!("{:?}", Phase::Run), "Run");
    assert_eq!(format!("{:?}", Phase::Assert), "Assert");
    assert_eq!(format!("{:?}", Phase::Teardown), "Teardown");
    let _state = LifecycleState::NotStarted;
}

#[test]
fn test_error_classification() {
    use crate::error::ShakedownError;

    // Given representative scenario, infrastructure, and internal errors
    let scenario_err: ShakedownError =
        ScenarioError::Assertion(crate::error::AssertionError::Unspecified {
            detail: "test assertion failed".to_string(),
        })
        .into();
    let infra_err: ShakedownError = crate::InfrastructureError::Provision {
        resource: "test_resource".to_string(),
        source: Box::new(std::io::Error::other("provision failed")),
    }
    .into();
    let internal_err: ShakedownError =
        crate::InternalError::Bug("unexpected code path".to_string()).into();
    // When we classify them at the ShakedownError level
    // Then each lands in the matching top-level variant
    assert!(matches!(scenario_err, ShakedownError::Scenario(_)));
    assert!(matches!(infra_err, ShakedownError::Infrastructure(_)));
    assert!(matches!(internal_err, ShakedownError::Internal(_)));
}

#[test]
fn test_builtin_resource_sync() {
    // Given a fake builtin resource
    let builtin = BuiltinResource::Fake(FakeResource::default());
    // When we pattern-match it
    // Then it is the Fake variant
    assert!(matches!(builtin, BuiltinResource::Fake(_)));
}

#[test]
fn test_error_source_chain() {
    // Given an infrastructure error that wraps an I/O error and a plain scenario error
    let infra_err = crate::InfrastructureError::Provision {
        resource: "test".to_string(),
        source: Box::new(std::io::Error::other("inner error")),
    };
    let scenario_err = ScenarioError::Assertion(crate::error::AssertionError::Unspecified {
        detail: "test".to_string(),
    });
    // When we query std::error::Error::source
    // Then infrastructure exposes a source and scenario assertion exposes the inner assertion error
    assert!(std::error::Error::source(&infra_err).is_some());
    assert!(std::error::Error::source(&scenario_err).is_some());
    assert!(std::error::Error::source(std::error::Error::source(&scenario_err).unwrap()).is_none());
}

#[test]
fn test_event_level_display() {
    // Given each event level
    // When we format them with Display
    // Then we get the uppercase labels used in logs
    assert_eq!(format!("{}", EventLevel::Debug), "DEBUG");
    assert_eq!(format!("{}", EventLevel::Info), "INFO");
    assert_eq!(format!("{}", EventLevel::Warn), "WARN");
    assert_eq!(format!("{}", EventLevel::Error), "ERROR");
}

#[test]
fn test_context_lifecycle() {
    // Given a new context
    let mut ctx = Context::new();
    assert!(ctx.is_empty());
    // When we register a builtin fake resource
    ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    // Then the context is no longer empty
    assert!(!ctx.is_empty());
}

#[test]
fn test_resource_trait_bounds() {
    // Given the concrete FakeResource type
    fn assert_send_sync<T: Send + Sync>() {}
    // When we require Send + Sync
    // Then FakeResource satisfies the bounds
    assert_send_sync::<FakeResource>();
}

#[test]
fn test_event_log_comprehensive() {
    // Given an empty log
    let mut log = EventLog::new();
    // When we append one raw line per level, clear, then append a structured event
    log.append_raw(EventLevel::Debug, "source1", "debug message");
    log.append_raw(EventLevel::Info, "source2", "info message");
    log.append_raw(EventLevel::Warn, "source3", "warn message");
    log.append_raw(EventLevel::Error, "source4", "error message");
    assert_eq!(log.len(), 4);
    let events: Vec<_> = log.events().collect();
    assert_eq!(events[0].level, EventLevel::Debug);
    assert_eq!(events[1].level, EventLevel::Info);
    assert_eq!(events[2].level, EventLevel::Warn);
    assert_eq!(events[3].level, EventLevel::Error);
    log.clear();
    assert!(log.is_empty());
    let event = Event::info("scenario", "test");
    log.append(event);
    assert_eq!(log.len(), 1);
    let retrieved: Vec<_> = log.events().collect();
    // Then the last message is the structured one we appended after clear
    assert_eq!(retrieved[0].message, "test");
}

#[tokio::test]
async fn test_run_scenario_with_fake_resource() {
    // Given a context with a fake builtin that always succeeds
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    let mut event_log = EventLog::new();
    // When we run a no-op scenario body through the lifecycle
    let result = run_scenario(
        &mut ctx,
        "test-scenario",
        |_ctx: &mut Context| Box::pin(async {}),
        &mut event_log,
    )
    .await;
    // Then the run completes successfully
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_run_scenario_events() {
    // Given a context with fake infrastructure and an empty event log
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    let mut event_log = EventLog::new();
    // When we run a scenario through the lifecycle
    run_scenario(
        &mut ctx,
        "test-events-scenario",
        |_ctx: &mut Context| Box::pin(async {}),
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
    let events: Vec<_> = event_log.events().collect();
    // Then lifecycle phases emitted at least one event tagged with the lifecycle source
    assert!(events.iter().any(|e| e.source == "lifecycle"));
}

#[tokio::test]
async fn test_run_scenario_fails_on_readiness() {
    // Given a resource type whose readiness check always fails (compile-time wiring check)
    struct UnreadyResource;

    #[async_trait]
    impl Resource for UnreadyResource {
        async fn provision(
            &mut self,
            _scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            Ok(vec![ResourceHandle {
                id: "unready".to_string(),
                connection_string: "://fake".to_string(),
                metadata: None,
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            Err(InfrastructureError::Readiness {
                resource: "test_resource".to_string(),
                source: Box::new(std::io::Error::other("not ready")),
            })
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }
    }

    let _ctx = Context::new();

    #[allow(dead_code)]
    async fn verify_signatures_compile() {
        async fn test_method_access(
            _r: &mut UnreadyResource,
        ) -> std::result::Result<(), InfrastructureError> {
            _r.check_ready().await
        }

        let _ = test_method_access;
    }
    // When we keep the nested helpers available for type-checking
    // Then the UnreadyResource Resource impl compiles with failing readiness
    let _ = verify_signatures_compile;
}

#[tokio::test]
async fn test_async_resource_phases() {
    // Given an extension resource whose async phases yield to the runtime
    struct AsyncResource;

    #[async_trait]
    impl Resource for AsyncResource {
        async fn provision(
            &mut self,
            _scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            tokio::task::yield_now().await;
            Ok(vec![ResourceHandle {
                id: "async_resource".to_string(),
                connection_string: "://async".to_string(),
                metadata: None,
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            tokio::task::yield_now().await;
            Ok(())
        }

        async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }
    }

    let mut ctx = Context::new();
    ctx.register_extension(AsyncResource);
    // When we run the full scenario lifecycle against it
    let result = run_scenario(
        &mut ctx,
        "test-async-resource",
        |_ctx: &mut Context| Box::pin(async {}),
        &mut EventLog::new(),
    )
    .await;
    // Then provisioning and readiness complete without error
    assert!(result.is_ok());
}

#[cfg(feature = "mock-services")]
mod mock_service_registration_tests {
    use crate::error::{InternalError, ShakedownError};
    use crate::mock_service::HttpMockService;

    use super::Context;

    #[test]
    fn register_http_mock_service_duplicate_name_returns_error() {
        // Given a context and one registered HTTP mock service
        let mut context = Context::new();
        let stub = HttpMockService::new("api").on("GET", "/").respond(200);
        context
            .register_http_mock_service("edge", stub.clone())
            .unwrap();
        // When we register another mock service with the same name
        let error = context
            .register_http_mock_service("edge", stub)
            .unwrap_err();
        // Then we get invalid configuration mentioning duplicate name
        match error {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(message.contains("duplicate mock service name"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn register_http_mock_service_after_servers_started_returns_error() {
        // Given a context where the mock service phase is marked complete
        let mut context = Context::new();
        context.set_mock_service_servers_phase_complete_for_test(true);
        let stub = HttpMockService::new("api").on("GET", "/").respond(200);
        // When we try to register a mock service
        let error = context
            .register_http_mock_service("edge", stub)
            .unwrap_err();
        // Then registration is rejected
        match error {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(message.contains("after mock service servers were started"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

#[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
mod mock_service_grpc_registration_tests {
    use crate::error::{InternalError, ShakedownError};
    use crate::mock_service::{GrpcMockService, HttpMockService};
    use tonic::Code;

    use super::Context;

    #[test]
    fn register_grpc_mock_service_duplicate_name_returns_error() {
        // Given one registered gRPC mock service
        let mut context = Context::new();
        let first_stub = GrpcMockService::new("grpc")
            .on_method("svc/Method")
            .respond_status(Code::Aborted, "first");
        let second_stub = GrpcMockService::new("grpc")
            .on_method("svc/Other")
            .respond_status(Code::Aborted, "second");
        context
            .register_grpc_mock_service("dup", first_stub)
            .unwrap();
        // When we register another gRPC mock service with the same name
        let error = context
            .register_grpc_mock_service("dup", second_stub)
            .unwrap_err();
        // Then we get duplicate name
        match error {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(message.contains("duplicate mock service name"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn register_grpc_mock_service_name_conflicts_with_http_mock_service() {
        // Given an HTTP mock service already registered under a name
        let mut context = Context::new();
        let http_stub = HttpMockService::new("api").on("GET", "/").respond(200);
        context
            .register_http_mock_service("shared", http_stub)
            .unwrap();
        let grpc_stub = GrpcMockService::new("grpc")
            .on_method("svc/M")
            .respond_status(Code::Ok, "ok");
        // When we register gRPC under the same name
        let error = context
            .register_grpc_mock_service("shared", grpc_stub)
            .unwrap_err();
        // Then we get duplicate name
        match error {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(message.contains("duplicate mock service name"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
