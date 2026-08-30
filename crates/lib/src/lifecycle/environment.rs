//! Environment backend and scenario-kind types for one lifecycle run.
//!
//! This module is useful when you add an environment backend or a new scenario kind flag.
//! This module holds the backend trait and the kind flag the harness stores on [`super::Context`].
//! This module does not run provision, readiness, or teardown itself.

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, displaydoc::Display)]
#[ignore_extra_doc_attributes]
pub enum ScenarioKind {
    /// Normal
    Normal,
    /// Faulty
    Faulty,
}

/// Map key for the single-resource / unnamed binding path ([`Context::set_kafka_bindings`], etc.).
pub const DEFAULT_RESOURCE_BINDING_NAME: &str = "default";
