//! # Shakedown — Orchestration library for shakedown testing
//!
//! **Shakedown** is a Rust framework for shakedown testing — proving systems are safe to
//! promote to production by running realistic scenarios that exercise the full stack.
//!
//! ## Overview
//!
//! This library provides:
//!
//! - **Scenario orchestration** — lifecycle phases: provision → readiness → seed → run → assert → teardown
//! - **Resource providers** — traits for integrating databases, message queues, and other infrastructure
//! - **Error classification** — structured errors distinguishing scenario vs infrastructure failures
//! - **Event logging hooks** — structured event collection for failure timelines
//!
//! ## Async Resource Trait (D-6)
//!
//! The [`Resource`] trait is now **async and scenario-aware**:
//!
//! ```text
//! #[async_trait]
//! pub trait Resource: Send + Sync {
//!     async fn provision(&mut self, scenario_id: &ScenarioId) -> Result<Vec<ResourceHandle>, InfrastructureError>;
//!     async fn check_ready(&self) -> Result<(), InfrastructureError>;
//!     async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError>;
//!     async fn teardown(&mut self) -> Result<(), InfrastructureError>;
//! }
//! ```
//!
//! This enables real infrastructure (Kafka, Postgres, etc.) to be integrated while maintaining
//! a clean extension point for custom resources.
//!
//! ## Workspace layout
//!
//! ```text
//! shakedown/
//! ├── crates/
//! │   ├── lib/          # this crate: orchestration engine
//! │   ├── macros/       # procedural macros (shakedown-macros)
//! │   └── cli/          # CLI binary (shakedown-cli)
//! ```
//!
//! ## Extension points
//!
//! - **[`Resource`] trait** — implement for new infrastructure integrations (Kafka, Postgres, etc.)
//! - **[`EnvironmentBackend`] trait** — implement for alternative execution environments
//! - **`Context::register_builtin()` / `Context::register_extension()`** — declarative registration
//!
//! ## Milestone status
//!
//! - **Shipped:** `mvp-outbox` — first-party outbox helpers on relational + Kafka bindings (`ctx.outbox()`, row/payload/Kafka assertions).
//! - **Prior:** `mvp-postgres-mariadb` — PostgreSQL, MariaDB, Oracle MySQL, and SQLite [`Resource`] types with suite-shared Docker containers and per-scenario isolation.
//! - **Next:** `mvp-fault-injection` — Toxiproxy-backed fault scenarios on the [`#[shakedown::faulty_test]`](crate::faulty_test) path (see adopted docs `ideation/scoping-brief-mvp-fault-injection.md`).
//!
//! ## See also
//!
//! Product docs (PRD, SPECS, TECH-STACK) live in the adopted documentation root; see the workspace
//! [`docs/README.md`](../../docs/README.md).
//!
//! ## Example
//!
//! ```text
//! use shakedown::Context;
//!
//! #[async_trait]
//! impl Resource for MyResource {
//!     async fn provision(&mut self, scenario_id: &ScenarioId) -> Result<Vec<ResourceHandle>, InfrastructureError> {
//!         // Provision my resource with scenario isolation
//!         Ok(vec![ResourceHandle { ... }])
//!     }
//!
//!     async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError> {
//!         // Apply migrations, seed data, etc.
//!         Ok(())
//!     }
//!
//!     async fn check_ready(&self) -> Result<(), InfrastructureError> {
//!         // Verify deep readiness beyond TCP
//!         Ok(())
//!     }
//!
//!     async fn teardown(&mut self) -> Result<(), InfrastructureError> {
//!         // Clean up
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::test]
//! async fn test_my_scenario() {
//!     let mut ctx = Context::new();
//!     ctx.register_extension(MyResource);
//!
//!     run_scenario(&mut ctx, "my-scenario", |c| Box::pin(my_scenario_logic(c)), &mut event_log)
//!         .await
//!         .unwrap();
//! }
//! ```

mod docker_labels;

/// Service bridge types and traits (`bridges` feature, default).
#[cfg(feature = "bridges")]
pub mod bridge;
/// Docker Engine diagnostics (bollard) when a Docker-backed resource or `services` is enabled.
#[cfg(any(
    feature = "kafka",
    feature = "rabbitmq",
    feature = "postgres",
    feature = "mariadb",
    feature = "mysql",
    feature = "redis",
    feature = "valkey",
    feature = "mongodb",
    feature = "nats",
    feature = "neo4j",
    feature = "services",
))]
pub mod diagnostics;
/// Structured errors: scenario vs infrastructure vs internal framework failures.
pub mod error;
/// Structured events and in-memory logs for timelines and diagnostics.
pub mod event;
/// Toxiproxy fault-injection control API (`fault-injection` feature).
#[cfg(feature = "fault-injection")]
pub mod fault_injection;
/// Typed helpers on [`lifecycle::Context`] for Kafka, RabbitMQ, and relational resources (feature-gated submodules).
pub mod helpers;
/// Scenario [`lifecycle::Context`], lifecycle phases, and [`lifecycle::run_scenario`].
pub mod lifecycle;
/// Outbox table setup and assertions (`outbox` + at least one relational feature).
#[cfg(all(
    feature = "outbox",
    any(
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "sqlite"
    )
))]
pub mod outbox;
/// Lifecycle phase labels (shared with infrastructure event records).
pub mod phase;
/// Publication observation spec and [`publication::PublicationVerifier`] (`outbox` feature).
///
/// Does not require `kafka` or `rabbitmq`; transport implementations are added by those features.
#[cfg(feature = "outbox")]
pub mod publication;
/// [`resources::Resource`] implementations, registration, and Kafka bindings.
pub mod resources;
/// Service-under-test Docker containers and bindings (`services` feature).
#[cfg(feature = "services")]
pub mod service;
/// Failure timeline rendering (terminal) for [`EventLog`].
pub mod timeline;

#[doc(inline)]
pub use crate::error::{
    AssertionError, InfrastructureError, InternalError, ResourceHandle, ResourceNotFoundError,
    Result, ScenarioError, ScenarioId, ShakedownError, StepError,
};
pub use crate::event::{
    DockerEventKind, Event, EventLevel, EventLog, EventSnapshot, InfraEvent, InfraSeverity,
    LogEntry, ScenarioLogEntry,
};
/// Public types and functions from the lifecycle module.
pub use crate::lifecycle::{
    run_scenario, Context, EnvironmentBackend, LifecycleState, NoopEnvironmentBackend, ScenarioKind,
};
pub use crate::phase::Phase;

#[cfg(feature = "bridges")]
pub use crate::bridge::{
    BridgeContext, BridgeError, BridgeHandle, BridgeRequest, BridgeResources, BridgeResponse,
    BridgeResponseBuilder, HttpBridge, RecordedCall, RecordedExchange, RouteMatch, StubBridge,
    StubRoutePending,
};

#[cfg(all(feature = "bridges", feature = "grpc-bridges"))]
pub use crate::bridge::{
    GrpcBridge, GrpcBridgeRequest, GrpcBridgeResponse, GrpcStubBridge, GrpcStubMethodPending,
};

#[cfg(feature = "outbox")]
pub use crate::publication::{
    message_matches_aggregate_id, PublicationObserveSpec, PublicationVerifier,
    DEFAULT_PUBLICATION_OBSERVE_TIMEOUT,
};

#[cfg(feature = "services")]
pub use crate::service::{
    ensure_host_docker_internal_resolution, linux_host_gateway_mapping_required,
    rewrite_injected_addresses_for_service_container, EnvMapping, HealthCheck, PullPolicy,
    ServiceHandle, ServiceSpec, ServiceUnderTest, DEFAULT_HEALTH_CHECK_INTERVAL,
    DEFAULT_HEALTH_CHECK_TIMEOUT, HOST_DOCKER_INTERNAL,
};

#[cfg(all(feature = "outbox", feature = "kafka"))]
pub use crate::publication::KafkaPublicationVerifier;

#[cfg(all(feature = "outbox", feature = "rabbitmq"))]
pub use crate::publication::{RabbitMqPublicationVerifier, RabbitMqPublicationVerifierOptions};
pub use crate::timeline::{
    build_failure_timeline_document, emit_failure_timeline_to_stderr, render_failure_timeline,
    resolve_target_timelines_dir, sanitize_scenario_id_for_filename, write_failure_timeline_json,
    write_failure_timeline_json_to_path, FailureTimelineDocument, FailureTimelineOptions,
    ResourceSnapshotJson, ResourceSnapshotsJson, RootCauseHint, ScenarioMetadataJson,
    TimelineEventJson,
};

#[cfg(any(
    feature = "kafka",
    feature = "rabbitmq",
    feature = "postgres",
    feature = "mariadb",
    feature = "mysql",
    feature = "redis",
    feature = "valkey",
    feature = "mongodb",
    feature = "nats",
    feature = "neo4j",
    feature = "services",
))]
pub use crate::diagnostics::{suite_diagnostics_handle, try_connect, DiagnosticsHandle};

#[cfg(all(
    feature = "diagnostics",
    not(feature = "no-diagnostics"),
    any(
        feature = "kafka",
        feature = "rabbitmq",
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "redis",
        feature = "valkey",
        feature = "mongodb",
        feature = "nats",
        feature = "neo4j",
        feature = "services",
    ),
))]
pub use crate::diagnostics::ResourceAssertBuilder;

/// First-party resource implementations.
#[doc(inline)]
pub use crate::resources::{
    sanitize_nats_subject_prefix, sanitize_redis_key_prefix, sanitize_resource_namespace,
    BuiltinResource, FakeResource, KafkaBindings, RegisteredResource, Resource,
};

#[cfg(feature = "postgres")]
pub use crate::resources::{PostgresBindings, PostgresResource};

#[cfg(feature = "mariadb")]
pub use crate::resources::{MariaDBBindings, MariaDBResource};

#[cfg(feature = "mysql")]
pub use crate::resources::{MysqlBindings, MysqlResource};

#[cfg(feature = "sqlite")]
pub use crate::resources::{sqlite_named_memory_uri, SqliteBindings, SqliteResource};

pub use crate::helpers::ConsumedMessage;
#[cfg(feature = "kafka")]
pub use crate::helpers::KafkaHelpers;
#[cfg(feature = "mariadb")]
pub use crate::helpers::MariaDbHelpers;
#[cfg(feature = "mongodb")]
pub use crate::helpers::MongoHelpers;
#[cfg(feature = "mysql")]
pub use crate::helpers::MysqlHelpers;
#[cfg(feature = "nats")]
pub use crate::helpers::NatsHelpers;
#[cfg(feature = "neo4j")]
pub use crate::helpers::Neo4jHelpers;
#[cfg(feature = "postgres")]
pub use crate::helpers::PostgresHelpers;
#[cfg(any(feature = "redis", feature = "valkey"))]
pub use crate::helpers::RedisHelpers;
#[cfg(feature = "sqlite")]
pub use crate::helpers::SqliteHelpers;
#[cfg(feature = "rabbitmq")]
pub use crate::helpers::{ExchangeKind, RabbitHelpers};
#[cfg(feature = "kafka")]
pub use crate::resources::KafkaResource;

#[cfg(feature = "fault-injection")]
pub use crate::fault_injection::FAULT_INJECTION_EVENT_SOURCE;
#[cfg(feature = "fault-injection")]
pub use crate::fault_injection::{Error as FaultInjectionError, FaultHandle};
#[cfg(feature = "fault-injection")]
pub use crate::fault_injection::{StreamDirection, Toxic, ToxicKind};
#[cfg(feature = "fault-injection")]
pub use crate::resources::ToxiproxyResource;

#[cfg(feature = "rabbitmq")]
pub use crate::resources::{RabbitBindings, RabbitResource};

#[cfg(any(feature = "redis", feature = "valkey"))]
pub use crate::resources::RedisBindings;

#[cfg(feature = "redis")]
pub use crate::resources::RedisResource;

#[cfg(feature = "valkey")]
pub use crate::resources::ValkeyResource;

#[cfg(feature = "mongodb")]
pub use crate::resources::{MongoBindings, MongoResource};

#[cfg(feature = "neo4j")]
pub use crate::resources::{Neo4jBindings, Neo4jResource};

#[cfg(feature = "nats")]
pub use crate::resources::{NatsBindings, NatsResource};

#[cfg(all(
    feature = "outbox",
    any(
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "sqlite"
    )
))]
pub use crate::outbox::{Outbox, OutboxEntry, OutboxRowState};

/// Procedural attributes for Tokio tests that run the full shakedown lifecycle.
///
/// Re-exported so callers depend only on `shakedown`. The macro crate does not depend on this
/// library (avoids cycles); generated code refers to `::shakedown::…` resolved at the call site.
pub use shakedown_macros::{faulty_test, test};

#[cfg(test)]
mod tests {
    use crate::error::{
        AssertionError, InfrastructureError, InternalError, ResourceHandle, ScenarioError,
    };
    use crate::event::{EventLevel, EventLog};
    use crate::lifecycle::{Context, NoopEnvironmentBackend};
    use crate::resources::{
        BuiltinResource, FakeResource, KafkaBindings, RegisteredResource, Resource,
    };

    #[test]
    fn test_context_and_types() {
        // Given a new scenario context and noop environment backend
        let _ctx = Context::new();
        let _backend = NoopEnvironmentBackend {};
        // When we exercise the public surface (event log, Kafka bindings, scenario kind, resources)
        let mut event_log = EventLog::new();
        event_log.append_raw(EventLevel::Info, "test", "test message");
        let bindings = KafkaBindings::new("localhost:9092", "prefix_");
        use crate::lifecycle::ScenarioKind;
        let scenario_kind = ScenarioKind::Normal;
        let builtin = RegisteredResource::Builtin(BuiltinResource::Fake(FakeResource::default()));
        let extension = RegisteredResource::Extension(Box::new(FakeResource::default()));
        // Then core types behave as expected for a smoke test
        assert_eq!(event_log.len(), 1);
        assert_eq!(bindings.bootstrap_servers, "localhost:9092");
        assert_eq!(scenario_kind.to_string(), "Normal");
        assert!(matches!(builtin, RegisteredResource::Builtin(_)));
        assert!(matches!(extension, RegisteredResource::Extension(_)));
        assert_eq!(builtin, builtin);
        assert_ne!(builtin, extension);
    }

    #[test]
    fn test_error_types_are_send_sync() {
        fn check_send_sync<T: Send + Sync>(_: T) {}

        // Given one value of each Shakedown error category
        let _scenario_err = ScenarioError::Assertion(AssertionError::Unspecified {
            detail: "test".to_string(),
        });
        let _infra_err = InfrastructureError::Provision {
            resource: "test".to_string(),
            source: Box::new(std::io::Error::other("test")),
        };
        let _internal_err = InternalError::Bug("test".to_string());
        // When we require Send + Sync for each
        // Then the compiler accepts all three
        check_send_sync(_scenario_err);
        check_send_sync(_infra_err);
        check_send_sync(_internal_err);
    }

    #[test]
    fn test_resource_trait_is_send_sync() {
        // Given a trivial Resource implementation used only for trait-object checks
        use crate::resources::Resource;
        use async_trait::async_trait;

        struct TestResource;

        #[async_trait]
        impl Resource for TestResource {
            async fn provision(
                &mut self,
                _scenario_id: &crate::error::ScenarioId,
            ) -> std::result::Result<Vec<ResourceHandle>, InfrastructureError> {
                Ok(vec![ResourceHandle {
                    id: "test".to_string(),
                    connection_string: "://test".to_string(),
                    metadata: None,
                }])
            }

            async fn check_ready(&self) -> std::result::Result<(), InfrastructureError> {
                Ok(())
            }

            async fn apply_default_seed(&mut self) -> std::result::Result<(), InfrastructureError> {
                Ok(())
            }

            async fn teardown(&mut self) -> std::result::Result<(), InfrastructureError> {
                Ok(())
            }
        }

        // Given a minimal Resource implementation
        // When we store it behind Box<dyn Resource>
        let _boxed: Box<dyn Resource> = Box::new(TestResource);
        // Then it compiles and can be dropped
        drop(_boxed);
    }

    #[test]
    fn test_lifecycle_awaits_async_methods() {
        // Given a Resource whose async hooks yield so lifecycle truly awaits them
        use async_trait::async_trait;

        struct AsyncTestResource;

        #[async_trait]
        impl Resource for AsyncTestResource {
            async fn provision(
                &mut self,
                _scenario_id: &crate::error::ScenarioId,
            ) -> std::result::Result<Vec<ResourceHandle>, InfrastructureError> {
                tokio::task::yield_now().await;
                Ok(vec![ResourceHandle {
                    id: "async_test".to_string(),
                    connection_string: "://async_test".to_string(),
                    metadata: None,
                }])
            }

            async fn check_ready(&self) -> std::result::Result<(), InfrastructureError> {
                tokio::task::yield_now().await;
                Ok(())
            }

            async fn apply_default_seed(&mut self) -> std::result::Result<(), InfrastructureError> {
                Ok(())
            }

            async fn teardown(&mut self) -> std::result::Result<(), InfrastructureError> {
                Ok(())
            }
        }

        // Given a Resource whose async methods yield to the runtime
        // When we require it to implement Resource
        fn assert_resource<T: Resource>() {}
        // Then the compiler accepts AsyncTestResource
        assert_resource::<AsyncTestResource>();
    }

    #[test]
    fn test_context_scenario_kind() {
        // Given a fresh context defaulting to Normal
        let mut ctx = Context::new();
        assert_eq!(ctx.scenario_kind(), crate::lifecycle::ScenarioKind::Normal);
        // When we mark the scenario as Faulty
        ctx.set_scenario_kind(crate::lifecycle::ScenarioKind::Faulty);
        // Then the context reports Faulty
        assert_eq!(ctx.scenario_kind(), crate::lifecycle::ScenarioKind::Faulty);
    }
}
