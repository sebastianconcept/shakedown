//! [`ServiceUnderTest`] builder → [`ServiceSpec`] (`services` feature).
//!
//! This is **pure configuration validation** — no Docker I/O (container start happens in the
//! service lifecycle).
//!
//! Use this to **declare** a service container your scenario depends on, then **register** the
//! resulting [`ServiceSpec`] on [`crate::lifecycle::Context`] so the lifecycle can start it, wait for
//! readiness, and expose a [`crate::service::ServiceHandle`] (for example via
//! [`crate::lifecycle::Context::service_url`]).
//!
//! Minimal example (build a spec):
//!
//! ```rust
//! use shakedown::ServiceUnderTest;
//!
//! let spec = ServiceUnderTest::docker("traefik/whoami:v1.10")
//!     .name("orders")
//!     .port(80)
//!     .build()
//!     .unwrap();
//!
//! assert_eq!(spec.name(), "orders");
//! ```
//!
//! Registering on a context (containers start later, during lifecycle):
//!
//! ```rust,no_run
//! # #[cfg(feature = "services")]
//! # {
//! use shakedown::{Context, ServiceUnderTest};
//!
//! let mut ctx = Context::new();
//!
//! let spec = ServiceUnderTest::docker("traefik/whoami:v1.10")
//!     .name("orders")
//!     .port(80)
//!     .build()
//!     .unwrap();
//!
//! ctx.register_service(spec).unwrap();
//! // Later (after lifecycle readiness), `ctx.service_url("orders")` becomes available.
//! # }
//! ```

use std::collections::{HashMap, HashSet};

use crate::error::{InternalError, Result, ShakedownError};
use crate::service::health::HealthCheck;
use crate::service::types::{EnvMapping, PortMapping, PullPolicy, ServiceSpec};

/// Builder for declaring a Docker-backed service under test.
#[derive(Debug, Clone)]
pub struct ServiceUnderTest {
    image: String,
    name: Option<String>,
    ports: Vec<PortMapping>,
    env_vars: HashMap<String, String>,
    env_mappings: Vec<EnvMapping>,
    health_check: Option<HealthCheck>,
    depends_on: Vec<String>,
    auto_inject: bool,
    pull_policy: PullPolicy,
}

impl ServiceUnderTest {
    /// Start a builder for an image reference (e.g. `"traefik/whoami:v1.10"`).
    pub fn docker(image: impl Into<String>) -> Self {
        Self {
            image: image.into(),
            name: None,
            ports: Vec::new(),
            env_vars: HashMap::new(),
            env_mappings: Vec::new(),
            health_check: None,
            depends_on: Vec::new(),
            auto_inject: true,
            pull_policy: PullPolicy::default(),
        }
    }

    /// Logical service name (used for URLs, labels, and dependency edges).
    pub fn name(mut self, service_name: impl Into<String>) -> Self {
        self.name = Some(service_name.into());
        self
    }

    /// Extra or override environment variable for the container (wins over convention injection for the same key).
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env_vars.insert(key.into(), value.into());
        self
    }

    /// Expose `container_port` on an anonymous host mapping (single-port services).
    pub fn port(mut self, container_port: u16) -> Self {
        self.ports.push(PortMapping {
            name: None,
            container_port,
        });
        self
    }

    /// Expose `container_port` under a logical `port_name` (multi-port services, URL helpers).
    pub fn port_named(mut self, port_name: impl Into<String>, container_port: u16) -> Self {
        self.ports.push(PortMapping {
            name: Some(port_name.into()),
            container_port,
        });
        self
    }

    /// Readiness probe for the harness (defaults to [`HealthCheck::tcp`] if unset).
    pub fn health_check(mut self, check: HealthCheck) -> Self {
        self.health_check = Some(check);
        self
    }

    /// Another service that must be healthy before this one starts.
    pub fn depends_on(mut self, service_name: impl Into<String>) -> Self {
        self.depends_on.push(service_name.into());
        self
    }

    /// Override default env var names for harness bindings (see [`EnvMapping`]).
    pub fn env_mapping(mut self, mapping: EnvMapping) -> Self {
        self.env_mappings.push(mapping);
        self
    }

    /// When `true`, inject Kafka/DB/mock service URLs by convention; when `false`, only explicit `.env` / `.env_mapping` apply.
    pub fn auto_inject(mut self, enabled: bool) -> Self {
        self.auto_inject = enabled;
        self
    }

    /// Docker image pull policy before `GenericImage::start` (service container lifecycle).
    pub fn pull_policy(mut self, policy: PullPolicy) -> Self {
        self.pull_policy = policy;
        self
    }

    /// Validate and freeze configuration for registration on [`crate::lifecycle::Context`].
    pub fn build(self) -> Result<ServiceSpec> {
        let Some(name) = self.name else {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(
                    "service under test: `.name(...)` is required before `.build()`".to_string(),
                ),
            ));
        };

        if name.trim().is_empty() {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(
                    "service under test: `.name(...)` must not be empty".to_string(),
                ),
            ));
        }

        if self.ports.is_empty() {
            return Err(ShakedownError::Internal(InternalError::InvalidConfiguration(
                "service under test: at least one `.port(...)` or `.port_named(...)` is required before `.build()`"
                    .to_string(),
            )));
        }

        let mut seen_dependencies = HashSet::<&str>::new();
        for dependency in &self.depends_on {
            if dependency == &name {
                return Err(ShakedownError::Internal(
                    InternalError::InvalidConfiguration(format!(
                        "service under test '{name}': cannot `.depends_on` the same service (self-dependency)"
                    )),
                ));
            }
            if !seen_dependencies.insert(dependency.as_str()) {
                return Err(ShakedownError::Internal(
                    InternalError::InvalidConfiguration(format!(
                        "service under test '{name}': duplicate `.depends_on(\"{dependency}\")`"
                    )),
                ));
            }
        }

        let health_check = self.health_check.unwrap_or_else(HealthCheck::tcp);

        Ok(ServiceSpec {
            name,
            image: self.image,
            ports: self.ports,
            env_vars: self.env_vars,
            env_mappings: self.env_mappings,
            health_check,
            depends_on: self.depends_on,
            auto_inject: self.auto_inject,
            pull_policy: self.pull_policy,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::types::EnvMapping;

    #[test]
    fn docker_starts_builder_with_image() {
        // Given a docker image reference
        // When we start a builder from it
        let builder = ServiceUnderTest::docker("alpine:3.20");
        // Then the image string is stored
        assert_eq!(builder.image, "alpine:3.20");
    }

    #[test]
    fn build_happy_path_minimal() {
        // Given name + port + image
        let spec = ServiceUnderTest::docker("traefik/whoami:v1.10")
            .name("orders")
            .port(80)
            .build()
            .unwrap_or_else(|error| panic!("minimal spec must build: {error:?}"));

        // Then defaults match: TCP health, auto_inject, IfNotPresent pull
        assert_eq!(spec.name, "orders");
        assert_eq!(spec.image, "traefik/whoami:v1.10");
        assert_eq!(
            spec.ports,
            vec![PortMapping {
                name: None,
                container_port: 80,
            }]
        );
        assert!(spec.env_vars.is_empty());
        assert!(spec.env_mappings.is_empty());
        assert!(spec.auto_inject);
        assert_eq!(spec.pull_policy, PullPolicy::IfNotPresent);
        assert!(matches!(spec.health_check, HealthCheck::Tcp { .. }));
        assert!(spec.depends_on.is_empty());
    }

    #[test]
    fn build_happy_path_full_fluent_chain() {
        // Given a fully chained builder
        let spec = ServiceUnderTest::docker("my/app:1")
            .name("api")
            .env("RUST_LOG", "info")
            .port_named("http", 8080)
            .health_check(HealthCheck::http("/health"))
            .depends_on("worker")
            .env_mapping(EnvMapping::kafka("KAFKA"))
            .auto_inject(false)
            .pull_policy(PullPolicy::Always)
            .build()
            .unwrap_or_else(|error| panic!("fluent spec must build: {error:?}"));

        // Then every option is reflected on the spec
        assert_eq!(spec.name, "api");
        assert_eq!(
            spec.env_vars.get("RUST_LOG").map(String::as_str),
            Some("info")
        );
        assert_eq!(spec.ports.len(), 1);
        assert_eq!(spec.ports[0].name.as_deref(), Some("http"));
        assert_eq!(spec.ports[0].container_port, 8080);
        assert!(matches!(spec.health_check, HealthCheck::Http { .. }));
        assert_eq!(spec.depends_on, vec!["worker".to_string()]);
        assert_eq!(spec.env_mappings.len(), 1);
        assert!(!spec.auto_inject);
        assert_eq!(spec.pull_policy, PullPolicy::Always);
    }

    #[test]
    fn build_fails_without_name() {
        // Given a builder with port but no name
        let err = ServiceUnderTest::docker("x:y")
            .port(80)
            .build()
            .expect_err("expected error");

        match err {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(
                    message.contains("name"),
                    "message should mention name: {message}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn build_fails_without_port() {
        // Given a builder with name but no port
        let err = ServiceUnderTest::docker("x:y")
            .name("svc")
            .build()
            .expect_err("expected error");

        match err {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(
                    message.contains("port") || message.contains("Port"),
                    "message should mention port: {message}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn build_fails_on_empty_name() {
        // Given a whitespace-only name
        let err = ServiceUnderTest::docker("x:y")
            .name("   ")
            .port(1)
            .build()
            .expect_err("expected error");

        match err {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(
                    message.contains("empty") || message.contains("name"),
                    "message: {message}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn build_fails_on_duplicate_depends_on() {
        // Given duplicate depends_on entries for the same upstream
        let err = ServiceUnderTest::docker("x:y")
            .name("b")
            .port(80)
            .depends_on("a")
            .depends_on("a")
            .build()
            .expect_err("expected error");

        match err {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(
                    message.contains("duplicate") && message.contains("depends_on"),
                    "message: {message}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn build_fails_on_self_dependency() {
        // Given depends_on pointing at the same service name
        let err = ServiceUnderTest::docker("x:y")
            .name("api")
            .port(80)
            .depends_on("api")
            .build()
            .expect_err("expected error");

        match err {
            ShakedownError::Internal(InternalError::InvalidConfiguration(message)) => {
                assert!(
                    message.contains("self-dependency") || message.contains("same service"),
                    "message: {message}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn env_last_write_wins_same_key() {
        // Given two .env calls for the same key
        let spec = ServiceUnderTest::docker("x:y")
            .name("s")
            .port(1)
            .env("K", "first")
            .env("K", "second")
            .build()
            .unwrap_or_else(|error| panic!("spec must build: {error:?}"));
        // Then the last value wins
        assert_eq!(spec.env_vars.get("K").map(String::as_str), Some("second"));
    }
}
