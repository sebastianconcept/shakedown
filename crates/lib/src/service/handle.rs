//! Read-only view of a running service-under-test (`services` feature).
//!
//! Populated by the harness lifecycle (US-009). Test authors use only the public accessors.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

/// Handle to a running service container: base URL, named port URLs, Docker id, and health flag.
///
/// Interior mutability is limited to [`AtomicBool`] for the health signal so lifecycle code can
/// update readiness without exposing mutation to scenario code ([`ServiceHandle`] has no public
/// `&mut self` methods).
pub struct ServiceHandle {
    pub(crate) name: String,
    base_url: String,
    named_urls: HashMap<String, String>,
    container_id: Option<String>,
    healthy: AtomicBool,
}

impl fmt::Debug for ServiceHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceHandle")
            .field("name", &self.name)
            .field("base_url", &self.base_url)
            .field("named_urls", &self.named_urls)
            .field("container_id", &self.container_id)
            .field("healthy", &self.is_healthy())
            .finish()
    }
}

impl ServiceHandle {
    /// Constructed by the harness when a service container is running (US-009).
    pub(crate) fn new(
        name: String,
        base_url: String,
        named_urls: HashMap<String, String>,
        container_id: Option<String>,
        initially_healthy: bool,
    ) -> Self {
        Self {
            name,
            base_url,
            named_urls,
            container_id,
            healthy: AtomicBool::new(initially_healthy),
        }
    }

    /// HTTP(S) base URL for the primary exposed port (scheme + host + port).
    pub fn base_url(&self) -> &str {
        self.base_url.as_str()
    }

    /// Full URL for a logically named port from [`crate::service::ServiceUnderTest::port_named`], if present.
    pub fn port_url(&self, port_name: &str) -> Option<&str> {
        self.named_urls.get(port_name).map(|url| url.as_str())
    }

    /// Docker container id, when known (for diagnostics and correlation).
    pub fn container_id(&self) -> Option<&str> {
        self.container_id.as_deref()
    }

    /// Whether the service passed the harness readiness probe.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Used by lifecycle when readiness is determined (US-009); also exercised from unit tests.
    #[allow(dead_code)] // Not called from production lifecycle yet (health is set at construction).
    pub(crate) fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::Relaxed);
    }
}

// String, HashMap, Option<String>, AtomicBool: Send + Sync.
#[cfg(test)]
mod tests {
    use super::*;

    fn sample_handle() -> ServiceHandle {
        let mut named = HashMap::new();
        named.insert("grpc".to_string(), "http://127.0.0.1:50051".to_string());
        ServiceHandle::new(
            "orders".to_string(),
            "http://127.0.0.1:8080".to_string(),
            named,
            Some("abc123container".to_string()),
            true,
        )
    }

    #[test]
    fn base_url_returns_primary_url() {
        // Given a handle with a primary base URL
        let handle = sample_handle();
        // Then base_url returns it
        assert_eq!(handle.base_url(), "http://127.0.0.1:8080");
    }

    #[test]
    fn port_url_returns_named_mapping() {
        // Given named port "grpc" on the handle
        let handle = sample_handle();
        assert_eq!(handle.port_url("grpc"), Some("http://127.0.0.1:50051"));
    }

    #[test]
    fn port_url_missing_returns_none() {
        // Given a handle without a "metrics" named port
        let handle = sample_handle();
        assert!(handle.port_url("metrics").is_none());
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
            "http://127.0.0.1:1".to_string(),
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
            "http://127.0.0.1:2".to_string(),
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
