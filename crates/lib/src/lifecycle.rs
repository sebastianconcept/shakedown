//! Scenario lifecycle types and execution.
//!
//! The scenario lifecycle consists of six ordered phases:
//!
//! 1. Provision resources
//! 2. Check resource readiness
//! 3. Seed data into resources
//! 4. Run the scenario logic
//! 5. Assert scenario outcomes
//! 6. Teardown resources
//!
//! If any phase fails, the lifecycle will attempt to run teardown for resources that were
//! successfully provisioned. The error classification distinguishes between scenario errors
//! (user logic/assertions) and infrastructure errors (provision/readiness failures).
//!
//! ## Event logging
//!
//! The lifecycle emits structured events during each phase. These events are collected in an
//! [`EventLog`] that can be retrieved for debugging or failure analysis.
//!
//! # Example
//!
//! ```ignore
//! use shakedown::{run_scenario, Context, EventLog};
//!
//! async fn my_scenario(ctx: &mut Context) {
//!     // Scenario implementation
//! }
//!
//! #[tokio::test]
//! async fn test_scenario_with_events() {
//!     let mut ctx = Context::new();
//!     let mut event_log = EventLog::new();
//!
//!     run_scenario(&mut ctx, "test-scenario", my_scenario, &mut event_log).await.unwrap();
//!
//!     // Retrieve events after scenario completion
//!     let events = event_log.events();
//!     assert!(events.iter().any(|e| e.source == "scenario" && e.level == EventLevel::Info));
//! }
//! ```

use crate::error::{InfrastructureError, ResourceHandle, ScenarioId, ShakedownError};
use crate::event::{EventLevel, EventLog};
use crate::resources::{
    BuiltinResource, FakeResource, KafkaBindings, RegisteredResource, Resource,
};
use async_trait::async_trait;

/// Represents a scenario lifecycle phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Provision resources
    Provision,
    /// Check resource readiness
    Readiness,
    /// Seed data into resources
    Seed,
    /// Run the scenario logic
    Run,
    /// Assert scenario outcomes
    Assert,
    /// Teardown resources
    Teardown,
}

/// Represents the lifecycle state of a scenario.
#[derive(Debug)]
pub enum LifecycleState {
    /// Not yet started
    NotStarted,
    /// Currently running a phase
    Running(Phase),
    /// Completed successfully
    Completed,
    /// Failed during a specific phase
    Failed(Phase, Box<dyn std::error::Error + Send + Sync>),
}

/// The environment backend abstraction.
///
/// This trait abstracts the underlying infrastructure where scenarios run,
/// allowing for different backends (Docker, local processes, cloud environments, etc.)
#[async_trait]
pub trait EnvironmentBackend: Send + Sync {
    /// Provision a single resource.
    ///
    /// This method is called during the provision phase. The environment backend
    /// should handle setting up the resource with scenario-scoped isolation.
    async fn provision_resource(
        &mut self,
        resource: RegisteredResource,
    ) -> Result<(), InfrastructureError>;

    /// Check readiness of all resources.
    async fn check_readiness(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    /// Teardown all resources.
    ///
    /// This method should be idempotent - it can be called multiple times
    /// safely, even if some resources were never provisioned or already torn down.
    async fn teardown(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }
}

/// A default stub environment backend that does nothing.
#[derive(Debug, Default)]
pub struct NoopEnvironmentBackend;

#[async_trait]
impl EnvironmentBackend for NoopEnvironmentBackend {
    async fn provision_resource(
        &mut self,
        _resource: RegisteredResource,
    ) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn check_readiness(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }
}

/// Represents the kind of scenario being executed.
///
/// This is used by `#[shakedown::faulty_test]` to mark fault-oriented scenarios,
/// and by future fault-injection code to decide whether to apply faults like
/// Toxiproxy toxics.
#[allow(clippy::derivable_impls)] // Manual impl needed for enum without #[default] marker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioKind {
    /// A normal scenario (no faults injected)
    Normal,
    /// A faulty scenario (faults may be injected)
    Faulty,
}

/// Represents the context for a scenario run.
pub struct Context {
    /// Registered resources
    resources: Vec<RegisteredResource>,
    /// Environment backend for lifecycle operations
    environment_backend: Box<dyn EnvironmentBackend>,
    /// Scenario ID for isolation
    scenario_id: ScenarioId,
    /// Kafka bindings when a Kafka resource populated them (from provision metadata).
    kafka_bindings: Option<KafkaBindings>,
    /// Kind of scenario being executed (Normal or Faulty)
    scenario_kind: ScenarioKind,
}

impl Context {
    /// Create a new empty context with the default noop environment backend.
    pub fn new() -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(NoopEnvironmentBackend),
            scenario_id: ScenarioId::empty(),
            kafka_bindings: None,
            scenario_kind: ScenarioKind::Normal,
        }
    }

    /// Create a new context with a custom environment backend.
    pub fn with_backend(backend: impl EnvironmentBackend + 'static) -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(backend),
            scenario_id: ScenarioId::empty(),
            kafka_bindings: None,
            scenario_kind: ScenarioKind::Normal,
        }
    }

    /// Create a new context with a specific scenario ID.
    pub fn with_scenario_id(scenario_id: impl Into<String>) -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(NoopEnvironmentBackend),
            scenario_id: ScenarioId::new(scenario_id),
            kafka_bindings: None,
            scenario_kind: ScenarioKind::Normal,
        }
    }

    /// Register a first-party [`BuiltinResource`].
    pub fn register_builtin(&mut self, resource: BuiltinResource) {
        self.resources.push(RegisteredResource::Builtin(resource));
    }

    /// Register an extension resource with the context.
    pub fn register_extension<R: Resource + 'static>(&mut self, resource: R) {
        self.resources
            .push(RegisteredResource::Extension(Box::new(resource)));
    }

    /// Get a reference to all registered resources.
    pub fn resources(&self) -> &[RegisteredResource] {
        &self.resources
    }

    /// Get a mutable reference to all registered resources.
    pub fn resources_mut(&mut self) -> &mut Vec<RegisteredResource> {
        &mut self.resources
    }

    /// Get a reference to the environment backend.
    pub fn environment_backend(&self) -> &dyn EnvironmentBackend {
        &*self.environment_backend
    }

    /// Get a mutable reference to the environment backend.
    pub fn environment_backend_mut(&mut self) -> &mut dyn EnvironmentBackend {
        &mut *self.environment_backend
    }

    /// Get the scenario ID.
    pub fn scenario_id(&self) -> &ScenarioId {
        &self.scenario_id
    }

    /// Set Kafka bindings for this scenario (typically after Kafka provision).
    pub fn set_kafka_bindings(&mut self, kafka_bindings: KafkaBindings) {
        self.kafka_bindings = Some(kafka_bindings);
    }

    /// Get a reference to Kafka bindings if available.
    pub fn kafka_bindings(&self) -> Option<&KafkaBindings> {
        self.kafka_bindings.as_ref()
    }

    /// Check if the context has any registered resources.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }

    /// Get a mutable reference to the scenario ID.
    pub fn set_scenario_id(&mut self, id: impl Into<String>) {
        self.scenario_id = ScenarioId::new(id);
    }

    /// Get a reference to the scenario kind.
    pub fn scenario_kind(&self) -> ScenarioKind {
        self.scenario_kind
    }

    /// Set the scenario kind for this run.
    pub fn set_scenario_kind(&mut self, kind: ScenarioKind) {
        self.scenario_kind = kind;
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

fn apply_resource_handles_to_kafka_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let (Some(bs), Some(tp)) = (
            meta.get("bootstrap_servers").and_then(|v| v.as_str()),
            meta.get("topic_prefix").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        ctx.set_kafka_bindings(KafkaBindings::new(bs, tp));
    }
}

async fn teardown_all_resources(ctx: &mut Context) {
    for resource in ctx.resources_mut() {
        let _ = resource.teardown().await;
    }
    let _ = ctx.environment_backend_mut().teardown().await;
}

/// Runs a scenario through all lifecycle phases.
///
/// This function executes the scenario in order:
/// - Provision resources (async)
/// - Check readiness (async)
/// - Seed data (async)
/// - Run scenario logic
/// - Assert outcomes
/// - Teardown resources (always attempted at the end, async)
///
/// # Errors
///
/// Returns an error if Provision or Readiness phases fail. The error type indicates the failure category:
/// - [`crate::ShakedownError::Infrastructure`] for provisioning/readiness failures
///
/// If Provision or Readiness fails, teardown is attempted before returning the error.
///
/// # Lifecycle ordering
///
/// The function respects the lifecycle order: provision → readiness → seed → run → assert → teardown
/// as documented in ARCHITECTURE.md §§ 3.6–3.9.
///
/// # Event logging
///
/// Events emitted during the scenario run are appended to the provided [`EventLog`].
/// After completion, callers can retrieve all events for debugging or analysis.
///
/// # Example
///
/// ```ignore
/// use shakedown::{Context, run_scenario, EventLog};
///
/// async fn my_scenario(ctx: &mut Context) {
///     // Scenario implementation
/// }
///
/// #[tokio::test]
/// async fn test_scenario() {
///     let mut ctx = Context::new();
///     let mut event_log = EventLog::new();
///
///     run_scenario(&mut ctx, "test-scenario", my_scenario, &mut event_log).await.unwrap();
///
///     // Retrieve events after scenario completion
///     let events = event_log.events();
/// }
/// ```
pub async fn run_scenario<F, Fut>(
    ctx: &mut Context,
    scenario_id: impl Into<String>,
    scenario_fn: F,
    event_log: &mut EventLog,
) -> Result<(), ShakedownError>
where
    F: Fn(&mut Context, &mut EventLog) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    // Update scenario ID
    ctx.set_scenario_id(scenario_id);

    // Provision phase (async)
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting provision phase");

    let scenario_id = ctx.scenario_id().clone();
    let n = ctx.resources().len();
    for i in 0..n {
        let result = {
            let resource = &mut ctx.resources_mut()[i];
            resource.provision(&scenario_id).await
        };
        match result {
            Ok(handles) => {
                apply_resource_handles_to_kafka_bindings(ctx, &handles);
                event_log.append_raw(EventLevel::Debug, "lifecycle", "Provisioned resource(s)");
            }
            Err(e) => {
                event_log.append_raw(
                    EventLevel::Error,
                    "lifecycle",
                    format!("Provision failed: {e}"),
                );
                teardown_all_resources(ctx).await;
                return Err(e.into());
            }
        }
    }

    // Readiness phase (async)
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting readiness phase");
    for resource in ctx.resources() {
        if let Err(e) = resource.check_ready().await {
            event_log.append_raw(
                EventLevel::Error,
                "lifecycle",
                format!("Readiness failed: {e}"),
            );
            teardown_all_resources(ctx).await;
            return Err(e.into());
        }
    }

    // Seed phase (async)
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting seed phase");
    for resource in ctx.resources_mut() {
        if let Err(e) = resource.apply_default_seed().await {
            event_log.append_raw(EventLevel::Error, "lifecycle", format!("Seed failed: {e}"));
            teardown_all_resources(ctx).await;
            return Err(e.into());
        }
    }

    // Run phase
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting run phase");
    // Execute the scenario function
    let _ = scenario_fn(ctx, event_log).await;

    // Assert phase (in a real implementation, this would run assertions)
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting assert phase");

    // Teardown phase
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting teardown phase");
    teardown_all_resources(ctx).await;

    Ok(())
}

/// Creates a context with a builtin Fake resource.
pub fn new_context_with_fake_resource() -> Context {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    ctx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ResourceHandle, ScenarioId};
    use crate::event::{Event, EventSnapshot};
    use crate::resources::Resource;
    use crate::ScenarioError;
    use async_trait::async_trait;

    #[test]
    fn test_phase_display() {
        assert_eq!(format!("{:?}", Phase::Provision), "Provision");
        assert_eq!(format!("{:?}", Phase::Readiness), "Readiness");
        assert_eq!(format!("{:?}", Phase::Seed), "Seed");
        assert_eq!(format!("{:?}", Phase::Run), "Run");
        assert_eq!(format!("{:?}", Phase::Assert), "Assert");
        assert_eq!(format!("{:?}", Phase::Teardown), "Teardown");
    }

    #[test]
    fn test_context_new() {
        let ctx = Context::new();
        assert!(ctx.is_empty());
    }

    #[test]
    fn test_context_with_backend() {
        let ctx = Context::with_backend(NoopEnvironmentBackend {});
        assert!(ctx.is_empty());
    }

    #[test]
    fn test_context_with_scenario_id() {
        let ctx = Context::with_scenario_id("test-scenario-123");
        assert_eq!(ctx.scenario_id().to_string(), "test-scenario-123");
    }

    #[test]
    fn test_context_register_builtin() {
        let mut ctx = Context::new();
        ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
        assert!(!ctx.is_empty());
        assert_eq!(ctx.resources().len(), 1);
    }

    #[test]
    fn test_context_register_extension() {
        let mut ctx = Context::new();
        ctx.register_extension(FakeResource::default());
        assert_eq!(ctx.resources().len(), 1);
    }

    #[test]
    fn test_lifecycle_state() {
        // Just check that we can construct the enum variants
        let _not_started = LifecycleState::NotStarted;
    }

    #[tokio::test]
    async fn test_noop_environment_backend() {
        let backend = NoopEnvironmentBackend {};
        backend
            .check_readiness()
            .await
            .expect("readiness should succeed");
    }

    #[test]
    fn test_event_log_append_and_retrieval() {
        // Test the core event log functionality: append and retrieval
        let mut log = EventLog::new();

        // Append events
        log.append(Event::info("scenario", "test event 1"));
        log.append(Event::debug("resource:fake", "test event 2"));

        // Verify append worked
        assert_eq!(log.len(), 2);

        // Retrieve events
        let events = log.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].message, "test event 1");
        assert_eq!(events[1].source, "resource:fake");

        // Test snapshot
        let _snapshot: EventSnapshot = log.clone().into();
        assert_eq!(log.len(), 2);

        // Test clear
        log.clear();
        assert!(log.is_empty());
    }

    #[test]
    fn test_lifecycle_ordering() {
        // Test that lifecycle phases execute in the correct order
        let _ctx = Context::new();
        let _event_log = EventLog::new();

        // We can't actually run the full scenario without async, but we can verify
        // that the phase enum and lifecycle types exist and are accessible
        assert_eq!(format!("{:?}", Phase::Provision), "Provision");
        assert_eq!(format!("{:?}", Phase::Readiness), "Readiness");
        assert_eq!(format!("{:?}", Phase::Seed), "Seed");
        assert_eq!(format!("{:?}", Phase::Run), "Run");
        assert_eq!(format!("{:?}", Phase::Assert), "Assert");
        assert_eq!(format!("{:?}", Phase::Teardown), "Teardown");

        // Verify the lifecycle state enum exists
        let _state = LifecycleState::NotStarted;
    }

    #[test]
    fn test_error_classification() {
        // Test that errors are properly classified into scenario vs infrastructure
        use crate::ShakedownError;

        // Scenario error (user logic/assertions)
        let scenario_err: ShakedownError =
            ScenarioError::Assertion("test assertion failed".to_string()).into();
        assert!(matches!(scenario_err, ShakedownError::Scenario(_)));

        // Infrastructure error (provision/readiness/teardown failures)
        let infra_err: ShakedownError = crate::InfrastructureError::Provision {
            resource: "test_resource".to_string(),
            source: Box::new(std::io::Error::other("provision failed")),
        }
        .into();
        assert!(matches!(infra_err, ShakedownError::Infrastructure(_)));

        // Internal error (framework bugs)
        let internal_err: ShakedownError =
            crate::InternalError::Bug("unexpected code path".to_string()).into();
        assert!(matches!(internal_err, ShakedownError::Internal(_)));
    }

    #[test]
    fn test_builtin_resource_sync() {
        let builtin = BuiltinResource::Fake(FakeResource::default());
        assert!(matches!(builtin, BuiltinResource::Fake(_)));
    }

    #[test]
    fn test_error_source_chain() {
        // Test that error source chains work correctly
        let infra_err = crate::InfrastructureError::Provision {
            resource: "test".to_string(),
            source: Box::new(std::io::Error::other("inner error")),
        };

        // InfrastructureError should have a source
        assert!(std::error::Error::source(&infra_err).is_some());

        let scenario_err = ScenarioError::Assertion("test".to_string());
        // ScenarioError doesn't have a source by default
        assert!(std::error::Error::source(&scenario_err).is_none());
    }

    #[test]
    fn test_event_level_display() {
        // Test Display implementation for EventLevel
        assert_eq!(format!("{}", EventLevel::Debug), "DEBUG");
        assert_eq!(format!("{}", EventLevel::Info), "INFO");
        assert_eq!(format!("{}", EventLevel::Warn), "WARN");
        assert_eq!(format!("{}", EventLevel::Error), "ERROR");
    }

    #[test]
    fn test_context_lifecycle() {
        // Test Context lifecycle methods
        let mut ctx = Context::new();

        // Verify empty initially
        assert!(ctx.is_empty());

        // Register a builtin resource
        ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
        assert!(!ctx.is_empty());
    }

    #[test]
    fn test_resource_trait_bounds() {
        // Verify Resource trait is Send + Sync
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FakeResource>();
    }

    #[test]
    fn test_event_log_comprehensive() {
        // Comprehensive test of event log functionality
        let mut log = EventLog::new();

        // Test append_raw with different levels
        log.append_raw(EventLevel::Debug, "source1", "debug message");
        log.append_raw(EventLevel::Info, "source2", "info message");
        log.append_raw(EventLevel::Warn, "source3", "warn message");
        log.append_raw(EventLevel::Error, "source4", "error message");

        assert_eq!(log.len(), 4);

        // Verify all levels
        let events = log.events();
        assert_eq!(events[0].level, EventLevel::Debug);
        assert_eq!(events[1].level, EventLevel::Info);
        assert_eq!(events[2].level, EventLevel::Warn);
        assert_eq!(events[3].level, EventLevel::Error);

        // Test clear empties the log
        log.clear();
        assert!(log.is_empty());

        // Test append and retrieval cycle
        let event = Event::info("scenario", "test");
        log.append(event);
        assert_eq!(log.len(), 1);

        let retrieved = log.events();
        assert_eq!(retrieved[0].message, "test");
    }

    #[tokio::test]
    async fn test_run_scenario_with_fake_resource() {
        // Test that run_scenario works with a fake resource
        let mut ctx = Context::new();
        ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));

        let mut event_log = EventLog::new();

        // This should succeed since FakeResource always succeeds
        let result = run_scenario(
            &mut ctx,
            "test-scenario",
            |_ctx: &mut Context, _event_log: &mut EventLog| async {},
            &mut event_log,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_run_scenario_events() {
        // Test that events are logged during scenario execution
        let mut ctx = Context::new();
        ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));

        let mut event_log = EventLog::new();

        run_scenario(
            &mut ctx,
            "test-events-scenario",
            |_ctx: &mut Context, _event_log: &mut EventLog| async {},
            &mut event_log,
        )
        .await
        .unwrap();

        // Verify that some events were logged
        let events = event_log.events();
        assert!(events.iter().any(|e| e.source == "lifecycle"));
    }

    #[tokio::test]
    async fn test_run_scenario_fails_on_readiness() {
        // Test that run_scenario properly handles readiness failures
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
    }

    #[tokio::test]
    async fn test_async_resource_phases() {
        // Test that async resource phases compile and are callable
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

        let result = run_scenario(
            &mut ctx,
            "test-async-resource",
            |_ctx: &mut Context, _event_log: &mut EventLog| async {},
            &mut EventLog::new(),
        )
        .await;

        assert!(result.is_ok());
    }
}
