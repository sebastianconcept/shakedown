//! Configured versus running reference for a service under test (`services` feature).
//!
//! Registration stores [`ServiceSpec`] (service configured). Lifecycle starts the container and
//! attaches [`ServiceHandle`] once the service is running and healthy.

use testcontainers::{ContainerAsync, GenericImage};

use super::handle::ServiceHandle;
use super::types::ServiceSpec;

/// Reference for one registered service: validated spec plus optional running handle/container.
pub(crate) struct ServiceReference {
    #[allow(dead_code)] // Read by injection and container start.
    pub(crate) spec: ServiceSpec,
    pub(crate) handle: Option<ServiceHandle>,
    /// Running Docker container, set between start and teardown.
    pub(crate) container: Option<ContainerAsync<GenericImage>>,
}

impl ServiceReference {
    pub(crate) fn new(spec: ServiceSpec) -> Self {
        Self {
            spec,
            handle: None,
            container: None,
        }
    }
}
