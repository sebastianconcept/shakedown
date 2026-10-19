//! Read-only view of a running service-under-test (`services` feature).
//!
//! Populated by the harness lifecycle. Test authors use only the public accessors.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

use super::endpoint::ServiceEndpoint;

/// Handle to a running service container: published endpoints, Docker id, and health flag.
///
/// Interior mutability is limited to [`AtomicBool`] for the health signal so lifecycle code can
/// update readiness without exposing mutation to scenario code ([`ServiceHandle`] has no public
/// `&mut self` methods).
pub struct ServiceHandle {
    pub(crate) name: String,
    endpoint: ServiceEndpoint,
    named_endpoints: HashMap<String, ServiceEndpoint>,
    container_id: Option<String>,
    healthy: AtomicBool,
}

impl fmt::Debug for ServiceHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceHandle")
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .field("named_endpoints", &self.named_endpoints)
            .field("container_id", &self.container_id)
            .field("healthy", &self.is_healthy())
            .finish()
    }
}

impl ServiceHandle {
    /// Constructed by the harness when a service container is running.
    pub(crate) fn new(
        name: String,
        endpoint: ServiceEndpoint,
        named_endpoints: HashMap<String, ServiceEndpoint>,
        container_id: Option<String>,
        initially_healthy: bool,
    ) -> Self {
        Self {
            name,
            endpoint,
            named_endpoints,
            container_id,
            healthy: AtomicBool::new(initially_healthy),
        }
    }

    /// HTTP or gRPC origin for the primary exposed port (`http://127.0.0.1:{port}`).
    ///
    /// Absent when readiness was a TCP connect or an in-container command. Use [`Self::endpoint`]
    /// for the host and port in that case.
    pub fn base_url(&self) -> Option<&str> {
        self.endpoint.http_url()
    }

    /// Published address of the primary port.
    pub fn endpoint(&self) -> &ServiceEndpoint {
        &self.endpoint
    }

    /// HTTP or gRPC URL for a logically named port from [`crate::service::ServiceUnderTest::port_named`].
    ///
    /// Absent when `port_name` was not published, or when readiness was TCP or an in-container command.
    pub fn port_url(&self, port_name: &str) -> Option<&str> {
        self.named_endpoints
            .get(port_name)
            .and_then(ServiceEndpoint::http_url)
    }

    /// Published address of a logically named port, when that name was exposed.
    pub fn port_endpoint(&self, port_name: &str) -> Option<&ServiceEndpoint> {
        self.named_endpoints.get(port_name)
    }

    /// Docker container id, when known (for diagnostics and correlation).
    pub fn container_id(&self) -> Option<&str> {
        self.container_id.as_deref()
    }

    /// Whether the service passed the harness readiness probe.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Used by lifecycle when readiness is determined; also exercised from unit tests.
    #[allow(dead_code)] // Not called from production lifecycle yet (health is set at construction).
    pub(crate) fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::Relaxed);
    }
}

// String, HashMap, Option<String>, AtomicBool, ServiceEndpoint: Send + Sync.
#[cfg(test)]
mod tests {
    use super::super::health::HealthCheck;
    use super::*;

    fn http_endpoint(port: u16) -> ServiceEndpoint {
        ServiceEndpoint::for_readiness(&HealthCheck::http("/"), port)
    }

    fn sample_handle() -> ServiceHandle {
        let health = HealthCheck::http("/health");
        let mut named = HashMap::new();
        named.insert(
            "metrics".to_string(),
            ServiceEndpoint::for_readiness(&health, 9090),
        );
        ServiceHandle::new(
            "orders".to_string(),
            ServiceEndpoint::for_readiness(&health, 8080),
            named,
            Some("abc123container".to_string()),
            true,
        )
    }

    #[test]
    fn base_url_returns_primary_http_origin() {
        // Given a handle whose readiness was HTTP
        let handle = sample_handle();
        // Then base_url is the loopback HTTP origin
        assert_eq!(handle.base_url(), Some("http://127.0.0.1:8080"));
    }

    #[test]
    fn port_url_returns_named_http_origin() {
        // Given a named port on an HTTP-ready handle
        let handle = sample_handle();
        assert_eq!(handle.port_url("metrics"), Some("http://127.0.0.1:9090"));
    }

    #[test]
    fn port_url_missing_returns_none() {
        // Given a handle without a "grpc" named port
        let handle = sample_handle();
        assert!(handle.port_url("grpc").is_none());
        assert!(handle.port_endpoint("grpc").is_none());
    }

    #[test]
    fn tcp_readiness_omits_http_urls() {
        // Given TCP readiness on the primary port and a named port
        let health = HealthCheck::tcp();
        let mut named = HashMap::new();
        named.insert(
            "admin".to_string(),
            ServiceEndpoint::for_readiness(&health, 5433),
        );
        let handle = ServiceHandle::new(
            "db".to_string(),
            ServiceEndpoint::for_readiness(&health, 5432),
            named,
            None,
            true,
        );
        // Then neither accessor presents an HTTP URL
        assert!(handle.base_url().is_none());
        assert!(handle.port_url("admin").is_none());
        assert!(matches!(
            handle.endpoint(),
            ServiceEndpoint::HostPort(address) if address.to_string() == "127.0.0.1:5432"
        ));
        assert!(matches!(
            handle.port_endpoint("admin"),
            Some(ServiceEndpoint::HostPort(address)) if address.to_string() == "127.0.0.1:5433"
        ));
    }

    #[test]
    fn command_readiness_omits_http_urls() {
        // Given command readiness
        let handle = ServiceHandle::new(
            "stone".to_string(),
            ServiceEndpoint::for_readiness(&HealthCheck::command(["/bin/true"]), 50377),
            HashMap::new(),
            None,
            true,
        );
        assert!(handle.base_url().is_none());
        assert!(matches!(
            handle.endpoint(),
            ServiceEndpoint::HostPort(address) if address.to_string() == "127.0.0.1:50377"
        ));
    }

    #[test]
    #[cfg(feature = "grpc")]
    fn grpc_readiness_keeps_http_origin() {
        // Given gRPC health readiness and a named port
        let health = HealthCheck::grpc();
        let mut named = HashMap::new();
        named.insert(
            "grpc".to_string(),
            ServiceEndpoint::for_readiness(&health, 50051),
        );
        let handle = ServiceHandle::new(
            "payments".to_string(),
            ServiceEndpoint::for_readiness(&health, 50051),
            named,
            None,
            true,
        );
        assert_eq!(handle.base_url(), Some("http://127.0.0.1:50051"));
        assert_eq!(handle.port_url("grpc"), Some("http://127.0.0.1:50051"));
        assert!(matches!(handle.endpoint(), ServiceEndpoint::Grpc(_)));
    }

    #[test]
    fn container_id_some() {
        // Given a handle constructed with a container id
        let handle = sample_handle();
        assert_eq!(handle.container_id(), Some("abc123container"));
    }

    #[test]
    fn container_id_none() {
        // Given a handle with no Docker id
        let handle = ServiceHandle::new(
            "x".to_string(),
            http_endpoint(1),
            HashMap::new(),
            None,
            false,
        );
        assert!(handle.container_id().is_none());
    }

    #[test]
    fn is_healthy_reflects_initial_and_updates() {
        // Given a handle starting unhealthy
        let handle = ServiceHandle::new(
            "y".to_string(),
            http_endpoint(2),
            HashMap::new(),
            None,
            false,
        );
        assert!(!handle.is_healthy());
        // When we flip healthy via lifecycle hook
        handle.set_healthy(true);
        assert!(handle.is_healthy());
        handle.set_healthy(false);
        assert!(!handle.is_healthy());
    }

    #[test]
    fn service_handle_is_send_sync() {
        // Given ServiceHandle as a type parameter
        fn assert_send_sync<T: Send + Sync>() {}
        // Then it is Send + Sync
        assert_send_sync::<ServiceHandle>();
    }
}
