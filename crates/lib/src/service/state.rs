//! Staged and runtime state for a service under test (`services` feature).
//!
//! Registration stores [`ServiceSpec`]; lifecycle (US-009) attaches [`ServiceHandle`] after the
//! container is healthy.

use testcontainers::{ContainerAsync, GenericImage};

use super::handle::ServiceHandle;
use super::types::ServiceSpec;

/// Harness state for one registered service: validated spec plus optional running handle.
pub(crate) struct ServiceState {
    #[allow(dead_code)] // Read by injection (US-007) and container start (US-009).
    pub(crate) spec: ServiceSpec,
    pub(crate) handle: Option<ServiceHandle>,
    /// Running Docker container, set between start and teardown (US-009).
    pub(crate) container: Option<ContainerAsync<GenericImage>>,
}

impl ServiceState {
    pub(crate) fn new(spec: ServiceSpec) -> Self {
        Self {
            spec,
            handle: None,
            container: None,
        }
    }
}
