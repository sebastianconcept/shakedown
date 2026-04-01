//! Service-under-test lifecycle (`services` feature).
//!
//! Opt-in module for running real services in Docker during scenarios: builder ([`ServiceUnderTest`]),
//! injection ([`crate::service::injection`]), dependency order, readiness probes, and lifecycle hooks
//! in [`crate::lifecycle`].

pub mod address;
mod builder;
pub(crate) mod dependency;
mod handle;
mod health;
pub(crate) mod injection;
pub(crate) mod lifecycle;
mod service_reference;
mod types;

pub(crate) use service_reference::ServiceReference;

pub use address::{
    ensure_host_docker_internal_resolution, linux_host_gateway_mapping_required,
    rewrite_injected_addresses_for_service_container, HOST_DOCKER_INTERNAL,
};
pub use builder::ServiceUnderTest;
pub use handle::ServiceHandle;
pub use health::{HealthCheck, DEFAULT_HEALTH_CHECK_INTERVAL, DEFAULT_HEALTH_CHECK_TIMEOUT};
pub use types::{EnvMapping, PullPolicy, ServiceSpec};

#[cfg(all(test, feature = "services"))]
mod registration_tests {
    use crate::error::{InternalError, ShakedownError};
    use crate::lifecycle::Context;
    use crate::service::{ServiceHandle, ServiceUnderTest};

    fn sample_spec(service_name: &str) -> crate::service::ServiceSpec {
        ServiceUnderTest::docker("traefik/whoami")
            .name(service_name)
            .port(80)
            .build()
            .unwrap_or_else(|error| panic!("sample spec must build: {error:?}"))
    }

    #[test]
    fn register_service_stages_spec_without_handle() {
        // Given a fresh context and a valid service spec
        let mut context = Context::new();
        let specification = sample_spec("orders");
        assert_eq!(specification.name(), "orders");
        // When we register the service before lifecycle has run
        context.register_service(specification).unwrap();
        // Then accessors are empty until the harness sets a handle
        assert!(context.service_url("orders").is_none());
        assert!(context.service_handle("orders").is_none());
    }

    #[test]
    fn duplicate_service_name_returns_invalid_configuration() {
        // Given a context that already registered service `dup`
        let mut context = Context::new();
        context.register_service(sample_spec("dup")).unwrap();
        // When we register the same name again
        let error = context.register_service(sample_spec("dup")).unwrap_err();
        // Then we get InvalidConfiguration mentioning duplicate name
        match error {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(message.contains("duplicate service name"));
                assert!(message.contains("dup"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn register_service_requires_dependency_registered_first() {
        // Given a spec that depends on `api` before `api` is registered
        let mut context = Context::new();
        let worker_first = ServiceUnderTest::docker("traefik/whoami")
            .name("worker")
            .port(80)
            .depends_on("api")
            .build()
            .unwrap_or_else(|error| panic!("fixture spec must build: {error:?}"));
        // When we try to register the dependent first
        let error = context.register_service(worker_first).unwrap_err();
        // Then registration fails and no service is registered
        match error {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(message.contains("api"), "{message}");
                assert!(message.contains("not registered"), "{message}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert!(context.services.is_empty());
    }

    #[test]
    fn register_service_accepts_dependency_after_upstream() {
        // Given `api` registered first
        let mut context = Context::new();
        context.register_service(sample_spec("api")).unwrap();
        // When we register `worker` depending on `api`
        let worker = ServiceUnderTest::docker("traefik/whoami")
            .name("worker")
            .port(80)
            .depends_on("api")
            .build()
            .unwrap_or_else(|error| panic!("fixture spec must build: {error:?}"));
        context.register_service(worker).unwrap();
        // Then both services are registered (configured only; not yet running)
        assert_eq!(context.registered_service_count(), 2);
    }

    #[test]
    fn service_url_and_service_handle_after_set_service_handle() {
        // Given a registered service and a synthetic handle (as lifecycle would set)
        let mut context = Context::new();
        context.register_service(sample_spec("api")).unwrap();
        let handle = ServiceHandle::new(
            "api".to_string(),
            "http://127.0.0.1:8080".to_string(),
            std::collections::HashMap::new(),
            None,
            true,
        );
        // When the harness attaches the handle
        context.set_service_handle("api", handle).unwrap();
        // Then URL and handle accessors reflect the base URL
        assert_eq!(
            context.service_url("api").as_deref(),
            Some("http://127.0.0.1:8080")
        );
        assert_eq!(
            context.service_handle("api").unwrap().base_url(),
            "http://127.0.0.1:8080"
        );
    }

    #[test]
    fn unknown_service_accessors_return_none() {
        // Given a context with no services
        let context = Context::new();
        // When we query a name that was never registered
        // Then accessors return None
        assert!(context.service_url("missing").is_none());
        assert!(context.service_handle("missing").is_none());
    }

    #[test]
    fn set_service_handle_unknown_name_errors() {
        // Given a context with no registration for `ghost`
        let mut context = Context::new();
        let handle = ServiceHandle::new(
            "ghost".to_string(),
            "http://127.0.0.1:1".to_string(),
            std::collections::HashMap::new(),
            None,
            false,
        );
        // When we try to attach a handle for an unknown name
        let error = context.set_service_handle("ghost", handle).unwrap_err();
        // Then we get InvalidConfiguration
        match error {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(message.contains("no registered service"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
