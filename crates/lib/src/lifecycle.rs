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
//! async fn my_scenario(_ctx: &mut Context) {
//!     // Scenario implementation
//! }
//!
//! #[tokio::test]
//! async fn test_scenario_with_events() {
//!     let mut ctx = Context::new();
//!     let mut event_log = EventLog::new();
//!
//!     run_scenario(&mut ctx, my_scenario, &mut event_log).await.unwrap();
//!
//!     // Retrieve events after scenario completion
//!     let events = event_log.events();
//!     assert!(events.iter().any(|e| e.source == "scenario" && e.level == EventLevel::Info));
//! }
//! ```

use crate::event::{EventLevel, EventLog};
use crate::resources::{FakeResource, Resource};
use crate::{InfrastructureError, Result};

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
pub trait EnvironmentBackend: Send + Sync {
    /// Provision a single resource.
    fn provision_resource(&mut self, resource: Box<dyn Resource>) -> Result<()>;

    /// Check readiness of all resources.
    fn check_readiness(&self) -> Result<()>;

    /// Teardown all resources.
    ///
    /// This method should be idempotent - it can be called multiple times
    /// safely, even if some resources were never provisioned or already torn down.
    fn teardown(&self) -> Result<()>;
}

/// A default stub environment backend that does nothing.
#[derive(Debug, Default)]
pub struct NoopEnvironmentBackend;

impl EnvironmentBackend for NoopEnvironmentBackend {
    fn provision_resource(&mut self, _resource: Box<dyn Resource>) -> Result<()> {
        Ok(())
    }

    fn check_readiness(&self) -> Result<()> {
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        Ok(())
    }
}

/// Represents the context for a scenario run.
pub struct Context {
    /// Registered resources
    resources: Vec<Box<dyn Resource>>,
    /// Environment backend for lifecycle operations
    environment_backend: Box<dyn EnvironmentBackend>,
}

impl Context {
    /// Create a new empty context with the default noop environment backend.
    pub fn new() -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(NoopEnvironmentBackend),
        }
    }

    /// Create a new context with a custom environment backend.
    pub fn with_backend(backend: impl EnvironmentBackend + 'static) -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(backend),
        }
    }

    /// Register a resource with the context.
    pub fn register_resource<R: Resource + 'static>(&mut self, resource: R) {
        self.resources.push(Box::new(resource));
    }

    /// Get a reference to all registered resources.
    pub fn resources(&self) -> &[Box<dyn Resource>] {
        &self.resources
    }

    /// Get a reference to the environment backend.
    pub fn environment_backend(&self) -> &dyn EnvironmentBackend {
        &*self.environment_backend
    }

    /// Get a mutable reference to the environment backend.
    pub fn environment_backend_mut(&mut self) -> &mut dyn EnvironmentBackend {
        &mut *self.environment_backend
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

/// Runs a scenario through all lifecycle phases.
///
/// This function executes the scenario in order:
/// 1. Provision resources
/// 2. Check readiness
/// 3. Seed data
/// 4. Run scenario logic
/// 5. Assert outcomes
/// 6. Teardown resources (always attempted at the end)
///
/// # Errors
///
/// Returns an error if Provision or Readiness phases fail. The error type indicates the failure category:
/// - [`ShakedownError::Infrastructure`] for provisioning/readiness failures
///
/// If Provision or Readiness fails, teardown is attempted before returning the error.
/// The return value from the scenario function and assertion results are not currently
/// captured as errors (this may be added in future milestones).
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
/// async fn my_scenario(_ctx: &mut Context) {
///     // Scenario implementation
/// }
///
/// #[tokio::test]
/// async fn test_scenario() {
///     let mut ctx = Context::new();
///     let mut event_log = EventLog::new();
///
///     run_scenario(&mut ctx, my_scenario, &mut event_log).await.unwrap();
///
///     // Retrieve events after scenario completion
///     let events = event_log.events();
/// }
/// ```
pub async fn run_scenario<F, Fut>(
    ctx: &mut Context,
    scenario_fn: F,
    event_log: &mut EventLog,
) -> Result<()>
where
    F: Fn(&mut Context, &mut EventLog) -> Fut,
    Fut: std::future::Future,
{
    // Provision phase
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting provision phase");
    let num_resources = ctx.resources.len();
    for i in 0..num_resources {
        // Remove each resource, provision it, then put it back
        let resource = std::mem::replace(&mut ctx.resources[i], Box::new(FakeResource {}));
        match ctx.environment_backend_mut().provision_resource(resource) {
            Ok(()) => {}
            Err(e) => {
                // Log the provision failure
                event_log.append_raw(
                    EventLevel::Error,
                    "lifecycle",
                    format!("Provision failed: {}", e),
                );
                // On provision failure, tear down what we can
                let _ = ctx.environment_backend().teardown();
                return Err(InfrastructureError::Provision {
                    resource: "unknown".to_string(),
                    source: Box::new(e),
                }
                .into());
            }
        }
    }

    // Readiness phase
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting readiness phase");
    if let Err(e) = ctx.environment_backend().check_readiness() {
        event_log.append_raw(
            EventLevel::Error,
            "lifecycle",
            format!("Readiness failed: {}", e),
        );
        // On readiness failure, tear down what we can
        let _ = ctx.environment_backend().teardown();
        return Err(InfrastructureError::Readiness {
            resource: "unknown".to_string(),
            source: Box::new(e),
        }
        .into());
    }

    // Seed phase
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting seed phase");
    for resource in &ctx.resources {
        resource.apply_default_seed();
    }

    // Run phase
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting run phase");
    // Note: The scenario function returns (), so we can't catch errors from it directly.
    // In a real implementation, the scenario function might return a Result that we could handle.
    let _ = scenario_fn(ctx, event_log).await;

    // Assert phase (in a real implementation, this would run assertions)
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting assert phase");

    // Teardown phase - always runs, even if previous phases failed
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting teardown phase");
    let _ = ctx.environment_backend().teardown();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use crate::event::EventSnapshot;

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
        assert!(ctx.resources().is_empty());
    }

    #[test]
    fn test_context_with_backend() {
        let ctx = Context::with_backend(NoopEnvironmentBackend {});
        assert!(ctx.resources().is_empty());
    }

    #[test]
    fn test_context_register_resource() {
        let mut ctx = Context::new();
        ctx.register_resource(crate::resources::FakeResource {});
        assert_eq!(ctx.resources().len(), 1);
    }

    #[test]
    fn test_lifecycle_state() {
        // Just check that we can construct the enum variants
        let _not_started = LifecycleState::NotStarted;
    }

    #[test]
    fn test_noop_environment_backend() {
        let backend = NoopEnvironmentBackend {};
        assert!(backend.check_readiness().is_ok());
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
}
