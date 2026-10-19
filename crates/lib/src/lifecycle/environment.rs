//! Lifecycle state, scenario kind, and the optional environment backend.
//!
//! Engines plug in through [`crate::resources::Resource`]. [`EnvironmentBackend`] is the
//! teardown hook invoked after those resources stop.

use crate::error::InfrastructureError;
use crate::resources::RegisteredResource;
use async_trait::async_trait;

use super::phase::Phase;

/// High-level state machine position for a scenario (reserved for richer orchestration).
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

/// Optional teardown hook beside [`crate::resources::Resource`].
///
/// `run_scenario` provisions, checks readiness, and seeds through each registered
/// [`crate::resources::Resource`]. After those resources tear down, it calls
/// [`EnvironmentBackend::teardown`]. [`EnvironmentBackend::provision_resource`] is not
/// called by the harness. Implement [`crate::resources::Resource`] and register it with
/// [`Context::register_extension`](crate::lifecycle::Context::register_extension) to add an engine.
#[async_trait]
pub trait EnvironmentBackend: Send + Sync {
    /// Provision a single resource.
    ///
    /// Not invoked by [`crate::lifecycle::run_scenario`]. Resource provisioning goes through
    /// [`crate::resources::Resource::provision`].
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
/// Fault-injection rewrites read this kind and apply toxics only when it is
/// [`ScenarioKind::Faulty`].
#[allow(clippy::derivable_impls)] // Manual impl needed for enum without #[default] marker
#[derive(Debug, Clone, Copy, PartialEq, Eq, displaydoc::Display)]
#[ignore_extra_doc_attributes]
pub enum ScenarioKind {
    /// Normal
    Normal,
    /// Faulty
    Faulty,
}
