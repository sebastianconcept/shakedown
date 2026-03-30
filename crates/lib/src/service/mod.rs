//! Service-under-test lifecycle (`services` feature).
//!
//! Opt-in module for running real services in Docker during scenarios. Concrete types and
//! lifecycle wiring land in follow-up user stories; this module is the compile-time seam.

pub mod address;
mod builder;
mod dependency;
mod handle;
mod health;
pub(crate) mod injection;
pub(crate) mod lifecycle;
mod state;
mod types;

pub(crate) use state::ServiceState;

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
            .expect("valid spec")
    }

    #[test]
    fn register_service_stages_spec_without_handle() {
        let mut context = Context::new();
        let specification = sample_spec("orders");
        assert_eq!(specification.name(), "orders");
        context.register_service(specification).unwrap();
        assert!(context.service_url("orders").is_none());
        assert!(context.service_handle("orders").is_none());
    }

    #[test]
    fn duplicate_service_name_returns_invalid_configuration() {
        let mut context = Context::new();
        context.register_service(sample_spec("dup")).unwrap();
        let error = context.register_service(sample_spec("dup")).unwrap_err();
        match error {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(message.contains("duplicate service name"));
                assert!(message.contains("dup"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn service_url_and_service_handle_after_set_service_handle() {
        let mut context = Context::new();
        context.register_service(sample_spec("api")).unwrap();
        let handle = ServiceHandle::new(
            "api".to_string(),
            "http://127.0.0.1:8080".to_string(),
            std::collections::HashMap::new(),
            None,
            true,
        );
        context.set_service_handle("api", handle).unwrap();
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
        let context = Context::new();
        assert!(context.service_url("missing").is_none());
        assert!(context.service_handle("missing").is_none());
    }

    #[test]
    fn set_service_handle_unknown_name_errors() {
        let mut context = Context::new();
        let handle = ServiceHandle::new(
            "ghost".to_string(),
            "http://127.0.0.1:1".to_string(),
            std::collections::HashMap::new(),
            None,
            false,
        );
        let error = context.set_service_handle("ghost", handle).unwrap_err();
        match error {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(message.contains("no registered service"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
