//! Service-under-test lifecycle management (`services` feature).
//!
//! Testing a service in isolation with mocked dependencies misses serialization bugs,
//! real error-handling paths, and emergent behaviour at service boundaries. With Shakedown's ServiceUnderTest,
//! you are running the real service binary in Docker during a scenario and it exercises
//! the same code path that production does without requiring the full service mesh.
//!
//! Build a [`ServiceUnderTest`] with the Docker image, environment variables, and dependency order,
//! then register it on [`crate::lifecycle::Context`] before calling [`crate::lifecycle::run_scenario`].
//! During the run, Shakedown stops the container during teardown. After HTTP or gRPC readiness,
//! the scenario reads the origin via `ctx.service_url("my-service")`. After TCP or in-container
//! command readiness, the scenario reads the host and port from [`ServiceHandle::endpoint`].
//!
//! If the scenario also declares infrastructure resources, [`crate::lifecycle::run_scenario`] runs
//! **[`crate::lifecycle::Phase::Seed`]** first: default seeding on those resources and any
//! database migrations you configure on the resource (for example `Postgres(migrations = \"./migrations\")`
//! on [`crate::test`], or [`crate::PostgresResource::with_migrations_path`](crate::PostgresResource::with_migrations_path)
//! in a prelude). Mock services start next; for each registered service the harness runs
//! [`ServiceUnderTest::pre_start`] hooks (read-only [`crate::lifecycle::Context`]), then starts that
//! service's container, waits for [`HealthCheck`], and injects resource URLs via [`EnvMapping`].
//!
//! # Book reference
//!
//! Part II, *Chapter 9: Service Basics* — introduces `ServiceUnderTest`, Docker image
//! configuration, health checks, and environment injection into service containers.

pub mod address;
mod builder;
pub(crate) mod dependency;
mod endpoint;
mod handle;
mod health;
pub(crate) mod injection;
pub(crate) mod lifecycle;
mod pre_start;
mod service_reference;
mod types;

pub(crate) use service_reference::ServiceReference;

pub use address::{
    ensure_host_docker_internal_resolution, linux_host_gateway_mapping_required,
    rewrite_injected_addresses_for_service_container, HOST_DOCKER_INTERNAL,
};
pub use builder::ServiceUnderTest;
pub use endpoint::{MappedHostPort, ServiceEndpoint, ServiceHostPort, ServiceHttpOrigin};
pub use handle::ServiceHandle;
pub use health::{HealthCheck, DEFAULT_HEALTH_CHECK_INTERVAL, DEFAULT_HEALTH_CHECK_TIMEOUT};
pub use types::{EnvMapping, PullPolicy, ServiceSpec};

#[cfg(all(test, feature = "services"))]
mod registration_tests {
    use crate::error::{InternalError, ShakedownError};
    use crate::lifecycle::Context;
    use crate::service::{HealthCheck, ServiceEndpoint, ServiceHandle, ServiceUnderTest};

    fn http_endpoint(port: u16) -> ServiceEndpoint {
        ServiceEndpoint::for_readiness(&HealthCheck::http("/"), port)
    }

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
            http_endpoint(8080),
            std::collections::HashMap::new(),
            None,
            true,
        );
        // When the harness attaches the handle
        context.set_service_handle("api", handle).unwrap();
        // Then URL and handle accessors reflect the HTTP origin
        assert_eq!(
            context.service_url("api").as_deref(),
            Some("http://127.0.0.1:8080")
        );
        assert_eq!(
            context.service_handle("api").unwrap().base_url(),
            Some("http://127.0.0.1:8080")
        );
    }

    #[test]
    fn service_url_absent_when_readiness_is_tcp() {
        // Given a registered service whose handle was published from TCP readiness
        let mut context = Context::new();
        context.register_service(sample_spec("api")).unwrap();
        let mut named = std::collections::HashMap::new();
        named.insert(
            "admin".to_string(),
            ServiceEndpoint::for_readiness(&HealthCheck::tcp(), 5433),
        );
        let handle = ServiceHandle::new(
            "api".to_string(),
            ServiceEndpoint::for_readiness(&HealthCheck::tcp(), 5432),
            named,
            None,
            true,
        );
        context.set_service_handle("api", handle).unwrap();
        // Then the scenario URL is absent and the handle keeps the host and port
        assert!(context.service_url("api").is_none());
        let endpoint = context.service_handle("api").unwrap().endpoint();
        assert!(matches!(
            endpoint,
            ServiceEndpoint::HostPort(address) if address.to_string() == "127.0.0.1:5432"
        ));
        assert!(context
            .service_handle("api")
            .unwrap()
            .port_url("admin")
            .is_none());
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
            http_endpoint(1),
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
