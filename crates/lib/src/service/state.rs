//! Staged and runtime state for a service under test (`services` feature).
//!
//! Registration stores [`ServiceSpec`]; lifecycle attaches [`ServiceHandle`] after the
//! container is healthy.

use testcontainers::{ContainerAsync, GenericImage};

use super::handle::ServiceHandle;
use super::types::ServiceSpec;

/// Harness state for one registered service: validated spec plus optional running handle.
pub(crate) struct ServiceState {
    #[allow(dead_code)] // Read by injection and container start.
    pub(crate) spec: ServiceSpec,
    pub(crate) handle: Option<ServiceHandle>,
    /// Running Docker container, set between start and teardown.
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
