//! Scenario lifecycle types and execution.

use crate::resources::Resource;
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
    /// Provision a single resource and return its binding.
    fn provision_resource(&self, resource: &mut dyn Resource) -> Result<()>;

    /// Check readiness of all resources.
    fn check_readiness(&self) -> Result<()>;

    /// Teardown all resources.
    fn teardown(&self) -> Result<()>;
}

/// A default stub environment backend that does nothing.
#[derive(Debug, Default)]
pub struct NoopEnvironmentBackend;

impl EnvironmentBackend for NoopEnvironmentBackend {
    fn provision_resource(&self, _resource: &mut dyn Resource) -> Result<()> {
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
#[derive(Default)]
pub struct Context {
    /// Registered resources
    resources: Vec<Box<dyn Resource>>,
}

impl Context {
    /// Create a new empty context.
    pub fn new() -> Self {
        Self {
            resources: Vec::new(),
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
}

/// Runs a scenario through all lifecycle phases.
///
/// Returns `Ok(())` if the scenario completed successfully.
/// Returns an error with appropriate classification based on which phase failed.
pub fn run_scenario<F, Fut>(_ctx: &mut Context, _scenario_fn: F) -> Result<()>
where
    F: Fn(&mut Context) -> Fut,
    Fut: std::future::Future,
{
    // Provision phase
    for _resource in _ctx.resources() {
        // In a real implementation, this would provision resources
    }

    // Readiness phase
    for _resource in _ctx.resources() {
        if !_resource.check_ready() {
            return Err(InfrastructureError::Readiness {
                resource: "unknown".to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "resource not ready",
                )),
            }
            .into());
        }
    }

    // Seed phase
    for _resource in _ctx.resources() {
        _resource.apply_default_seed();
    }

    // Run phase
    // In a real implementation, this would execute the scenario logic

    // Assert phase
    // In a real implementation, this would run assertions

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
