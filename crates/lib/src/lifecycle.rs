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

/// Represents the context for a scenario run.
#[derive(Default)]
pub struct TestContext {
    /// Registered resources
    resources: Vec<Box<dyn Resource>>,
}

impl TestContext {
    /// Create a new empty test context.
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
pub fn run_scenario<F, Fut>(_ctx: &mut TestContext, _scenario_fn: F) -> Result<()>
where
    F: Fn(&mut TestContext) -> Fut,
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
    fn test_test_context_new() {
        let ctx = TestContext::new();
        assert!(ctx.resources().is_empty());
    }

    #[test]
    fn test_test_context_register_resource() {
        struct FakeResource;

        impl crate::resources::Resource for FakeResource {
            fn apply_default_seed(&self) {}

            fn check_ready(&self) -> bool {
                true
            }

            fn teardown(self) {}
        }

        let mut ctx = TestContext::new();
        ctx.register_resource(FakeResource {});
        assert_eq!(ctx.resources().len(), 1);
    }

    #[test]
    fn test_lifecycle_state() {
        // Just check that we can construct the enum variants
        let _not_started = LifecycleState::NotStarted;
    }
}
