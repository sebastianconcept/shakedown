//! Service-container registration accessors on [`super::Context`].
//!
//! This module is useful when a scenario registers a service spec or reads a running service handle.
//! This module does not start service containers.

#[cfg(feature = "services")]
use crate::error::InternalError;
#[cfg(feature = "services")]
use crate::error::ShakedownError;
#[cfg(feature = "services")]
use crate::service::ServiceHandle;

use super::Context;

impl Context {
    /// Register a built [`crate::service::ServiceSpec`] for the harness to start after infrastructure
    /// and mock services (service container lifecycle).
    ///
    /// Duplicate service names return [`crate::error::InternalError::InvalidConfiguration`]. Every
    /// `depends_on` target must already be registered, and the combined dependency graph must be acyclic
    /// (multi-service dependency ordering);
    /// otherwise the new registration is rolled back and the same error variant is returned. [`Self::service_url`]
    /// and [`Self::service_handle`] stay empty until lifecycle attaches a [`ServiceHandle`] after readiness.
    #[cfg(feature = "services")]
    pub fn register_service(
        &mut self,
        service_spec: crate::service::ServiceSpec,
    ) -> Result<(), ShakedownError> {
        let name = service_spec.name().to_string();
        if self.services.contains_key(&name) {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(format!("duplicate service name '{name}'")),
            ));
        }
        self.services.insert(
            name.clone(),
            crate::service::ServiceReference::new(service_spec),
        );
        if let Err(error) = crate::service::dependency::service_startup_order(&self.services) {
            self.services.remove(&name);
            return Err(error);
        }
        Ok(())
    }

    /// HTTP base URL for a registered service after the container is healthy (`services` feature).
    ///
    /// Returns [`None`] if the name is unknown or the service has not been started yet for this run.
    #[cfg(feature = "services")]
    pub fn service_url(&self, name: &str) -> Option<String> {
        self.services
            .get(name)
            .and_then(|state| state.handle.as_ref())
            .map(|handle| handle.base_url().to_string())
    }

    /// Read-only handle for a registered service after the container is healthy (`services` feature).
    #[cfg(feature = "services")]
    pub fn service_handle(&self, name: &str) -> Option<&ServiceHandle> {
        self.services
            .get(name)
            .and_then(|state| state.handle.as_ref())
    }

    /// Used by lifecycle when a service container passes readiness.
    #[cfg(feature = "services")]
    pub(crate) fn set_service_handle(
        &mut self,
        name: &str,
        handle: ServiceHandle,
    ) -> Result<(), ShakedownError> {
        let state = self.services.get_mut(name).ok_or_else(|| {
            ShakedownError::Internal(InternalError::InvalidConfiguration(format!(
                "set_service_handle: no registered service named '{name}'"
            )))
        })?;
        state.handle = Some(handle);
        Ok(())
    }

    /// Number of registered [`crate::service::ServiceSpec`] entries (used by env injection for partitioning).
    #[cfg(feature = "services")]
    #[allow(dead_code)] // `inject_service_env`; service container lifecycle
    pub(crate) fn registered_service_count(&self) -> usize {
        self.services.len()
    }

    /// Invoke `visit` for each running HTTP/gRPC mock service base URL (service container env injection).
    #[cfg(all(feature = "mock-services", feature = "services"))]
    #[allow(dead_code)] // `inject_service_env`; service container lifecycle
    pub(crate) fn for_each_mock_service_url(&self, mut visit: impl FnMut(&str, &str)) {
        for (name, handle) in &self.mock_service_handles {
            visit(name.as_str(), handle.client_url.as_str());
        }
    }
}
