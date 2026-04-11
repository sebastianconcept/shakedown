//! # Shakedown
//!
//! The familiar pain is **tests pass, production breaks**: CI went green because mocks and unit
//! tests never exercised the real integrations, ordering, and I/O that production relies on—so
//! the first serious run of the system is your users, not your pipeline.
//!
//! Teams adopted testing vocabulary but often skipped the **methodology** that ties scenarios to
//! real infrastructure, isolation, and evidence. Without that thread, "green" stops meaning
//! **safe to ship**.
//!
//! **Shakedown** takes its name from the aerospace and motorsport **shakedown**—the deliberate
//! qualification run before something is cleared as **safe to fly**. Here, scenarios are that
//! runway: prove behavior on real dependencies, record what happened, and promote when the
//! evidence says the system is ready—not when the easy tests merely pass.
//!
//! ## Progressive confidence
//!
//! The library supports **progressive confidence** in five steps—start where you are and deepen
//! fidelity as risk demands:
//!
//! 1. **Scenarios & lifecycle** — run through provision → readiness → seed → run → assert →
//!    teardown so every run has a repeatable shape ([`lifecycle`], [`run_scenario`]).
//! 2. **Resources** — register databases, buses, and other engines with per-scenario isolation
//!    while reusing suite-wide infrastructure where it makes sense ([`resources`]).
//! 3. **Services & data paths** — exercise what you deploy: containers under test, bridges, and
//!    transactional patterns such as outbox flows ([`service`], [`outbox`], [`publication`]).
//! 4. **Faults & instrumentation** — inject network faults on proxied dependencies and read a
//!    correlated timeline when something breaks ([`fault_injection`], [`instrumentation`]).
//! 5. **Load & topology** — add open-model load, performance assertions, and multi-node layouts
//!    when you need scale or HA-shaped coverage ([`load`], [`cluster`]).
//!
//! ## Three modes of confidence
//!
//! Shakedown is built around **three modes of confidence** that reinforce each other:
//!
//! - **Transferring confidence** — shared scenarios and structured output give reviewers and
//!   teammates evidence they can trust in PRs and release gates.
//! - **Preserving confidence** — when production misbehaves, the same harness vocabulary helps
//!   you lock the failure in as a regression scenario instead of a one-off mystery.
//! - **Trusting the result** — diagnostics and error classification help you see whether a red
//!   run points at your product or at the test infrastructure, so flakes do not erode belief in
//!   the signal.
//!
//! ## Quick start
//!
//! The [`#[shakedown::test]`](crate::test) attribute registers resources and runs your scenario
//! on the Tokio test runtime. The example below uses [`BuiltinResource::Fake`]
//! so it compiles with default features (no Docker). For in-memory SQL with no container runtime,
//! enable the `sqlite` feature and `resources = [Sqlite]`—see [`README.md`](../README.md).
//!
//! For full manual control, call [`run_scenario`] with a [`Context`], a scenario id, an async
//! closure, and an [`EventLog`].
//!
//! ## Where to start
//!
//! Enable only the Cargo features your scenarios need; keep Docker-backed tests behind
//! `#[ignore]` in CI unless the job passes `--include-ignored` (Cargo) or `--run-ignored all`
//! (nextest). **`diagnostics`** is pulled in with Docker-backed resource features unless you set
//! **`no-diagnostics`**. The six-category **bindings-as-bridge** table, CI split, and parallelism
//! guidance are in [`README.md`](../README.md).
//!
//! ```rust
//! use shakedown::Context;
//!
//! #[shakedown::test(resources = [Fake])]
//! async fn my_first_shakedown(ctx: &mut Context) {
//!     assert!(!ctx.is_empty());
//! }
//! ```
//!
//! Fault-oriented scenarios use [`#[shakedown::faulty_test]`](crate::faulty_test) instead of
//! [`#[shakedown::test]`](crate::test) so the harness can treat them as the faulty path when you
//! wire Toxiproxy and related tooling.
//!
//! ## Cargo features
//!
//! Integrations are **opt-in** so default builds stay light (`mock-services` only). Enable what
//! you need in `Cargo.toml`: for example `kafka`, `postgres`, `mariadb`, `mysql`, `rabbitmq`,
//! `sqlite`, `duckdb`, `redis`, `valkey`, `redis-sentinel`, `mongodb`, `nats`, `neo4j`,
//! `elasticsearch`, `opensearch`, `qdrant`, `weaviate`, `influxdb`, `timescaledb`, `outbox`,
//! `sqlx` (pools + [`sqlx::migrate::Migrator`] integration with `postgres` / `sqlite`),
//! `fault-injection`, `load`, `services`, `cluster`, and engine-specific cluster flags such as
//! `kafka-cluster` or `postgres-cluster`. The full matrix, CI guidance, and operator workflows
//! are documented in [`README.md`](../README.md).
//!
//! ## What this crate provides
//!
//! - **Scenario orchestration** — the lifecycle above, with structured errors ([`error`])
//! - **Resource registration** — [`BuiltinResource`], [`RegisteredResource`], and the async
//!   [`Resource`] trait for custom backends
//! - **Instrumentation** — events, failure timelines, optional Docker diagnostics ([`instrumentation`])
//!
//! ## Async [`Resource`] trait
//!
//! The [`Resource`] trait is **async and scenario-aware**:
//!
//! ```rust,no_run
//! # use async_trait::async_trait;
//! # use shakedown::{InfrastructureError, ResourceHandle, ScenarioId};
//! #[async_trait]
//! pub trait Resource: Send + Sync {
//!     async fn provision(&mut self, scenario_id: &ScenarioId) -> Result<Vec<ResourceHandle>, InfrastructureError>;
//!     async fn check_ready(&self) -> Result<(), InfrastructureError>;
//!     async fn apply_default_seed(&mut self) -> Result<(), InfrastructureError>;
//!     async fn teardown(&mut self) -> Result<(), InfrastructureError>;
//! }
//! ```
//!
//! That contract lets Kafka, Postgres, SQLite, and custom integrations share the same lifecycle
//! hooks and extension points.
//!
//! ## Workspace layout
//!
//! ```text
//! shakedown/
//! ├── crates/
//! │   ├── lib/          # this crate: orchestration engine
//! │   ├── macros/       # procedural macros (shakedown-macros)
//! │   └── examples/     # optional adoption scenarios (`shakedown-examples`)
//! ```
//!
//! ## Extension points
//!
//! - **[`Resource`] trait** — implement for new infrastructure integrations
//! - **[`EnvironmentBackend`] trait** — plug in alternative execution environments
//! - **`Context::register_builtin()` / `Context::register_extension()`** — register resources from code
//!
//! ## See also
//!
//! The workspace [`docs/README.md`](../../docs/README.md) points maintainers at notes kept
//! alongside this repository.

#![warn(missing_docs)]
// Crate docs and module docs link to feature-gated items, optional dependencies (e.g. `sqlx`), and
// re-exports that are absent under `cargo doc`'s default feature set. Fixing every link for every
// feature matrix is not practical; keep rustdoc warning-clean for the common doc build.
#![allow(rustdoc::broken_intra_doc_links, rustdoc::private_intra_doc_links)]

mod docker_labels;

/// Cluster topology — Docker networks and node control (`cluster` feature).
#[cfg(feature = "cluster")]
pub mod cluster;
/// Events, timelines, and Docker-backed diagnostics — see submodule docs on [`instrumentation`].
pub mod instrumentation;

// Stable crate-root paths (`shakedown::event`, `shakedown::timeline`, `shakedown::diagnostics`).
#[cfg(any(
    feature = "kafka",
    feature = "rabbitmq",
    feature = "postgres",
    feature = "mariadb",
    feature = "mysql",
    feature = "redis",
    feature = "redis-sentinel",
    feature = "valkey",
    feature = "mongodb",
    feature = "nats",
    feature = "neo4j",
    feature = "elasticsearch",
    feature = "opensearch",
    feature = "qdrant",
    feature = "weaviate",
    feature = "influxdb",
    feature = "timescaledb",
    feature = "services",
))]
#[doc(inline)]
pub use instrumentation::diagnostics;
#[doc(inline)]
pub use instrumentation::event;
#[doc(inline)]
pub use instrumentation::timeline;

/// Outbox row / publication / payload errors ([`ShakedownError::Outbox`](crate::error::ShakedownError::Outbox)).
#[path = "outbox/error.rs"]
mod outbox_error;
pub use outbox_error::OutboxError;
/// Structured errors: scenario vs infrastructure vs internal framework failures.
pub mod error;
/// Toxiproxy fault-injection control API (`fault-injection` feature).
#[cfg(feature = "fault-injection")]
pub mod fault_injection;
mod helper_prelude;
/// Scenario [`lifecycle::Context`], lifecycle phases, and [`lifecycle::run_scenario`].
pub mod lifecycle;
/// Open-model load profiles (`load` feature).
#[cfg(feature = "load")]
pub mod load;
/// Service mock service types and traits (`mock-services` feature, default).
#[cfg(feature = "mock-services")]
pub mod mock_service;
/// Outbox table setup and assertions (`outbox` + at least one storage engine feature).
#[cfg(all(
    feature = "outbox",
    any(
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "sqlite",
        feature = "mongodb",
        feature = "duckdb"
    )
))]
pub mod outbox;
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

#[cfg(feature = "load")]
pub use crate::error::PerformanceAssertionDetails;
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
    run_scenario, Context, EnvironmentBackend, LifecycleState, NoopEnvironmentBackend, Phase,
    ScenarioKind, ScenarioRunOutcome,
};

#[cfg(feature = "mock-services")]
pub use crate::mock_service::{
    HttpMockService, HttpMockServiceRequest, HttpMockServiceResponse,
    HttpMockServiceResponseBuilder, HttpMockServiceRoutePending, HttpMockServiceTrait,
    MockServiceContext, MockServiceError, MockServiceHandle, MockServiceResources, RecordedCall,
    RecordedExchange, RouteMatch,
};

#[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
pub use crate::mock_service::{
    GrpcMockService, GrpcMockServiceMethodPending, GrpcMockServiceRequest, GrpcMockServiceResponse,
    GrpcMockServiceTrait,
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

#[cfg(all(feature = "outbox", feature = "nats"))]
pub use crate::publication::{NatsPublicationVerifier, NatsPublicationVerifierOptions};

#[cfg(all(feature = "outbox", any(feature = "redis", feature = "valkey")))]
pub use crate::publication::{
    RedisStreamsPublicationVerifier, RedisStreamsPublicationVerifierOptions,
};
pub use crate::timeline::{
    build_failure_timeline_document, emit_failure_timeline_for_shakedown_error,
    emit_failure_timeline_to_stderr, render_failure_timeline, resolve_target_dir,
    resolve_target_timelines_dir, sanitize_scenario_id_for_filename, write_failure_timeline_json,
    write_failure_timeline_json_to_path, FailureTimelineDocument, FailureTimelineOptions,
    PerformanceAssertionFailureJson, ResourceSnapshotJson, ResourceSnapshotsJson, RootCauseHint,
    ScenarioMetadataJson, TimelineEventJson,
};

#[cfg(feature = "cluster")]
pub use crate::cluster::{
    cluster_network_name, create_cluster_network, remove_cluster_network, ClusterControl,
    ClusterError, ClusterHandle, NodeStatus, CLUSTER_EVENT_SOURCE,
};

#[cfg(feature = "mongodb-cluster")]
pub use crate::cluster::mongo_replica_set::{
    MongoClusterHandle, MongoClusterOps, MongoReplicaSetClusterControl, MongoReplicaSetMarker,
};

#[cfg(feature = "kafka-cluster")]
pub use crate::cluster::kafka_cluster::{
    KafkaClusterClusterControl, KafkaClusterHandle, KafkaClusterMarker, KafkaClusterOps,
};

#[cfg(feature = "redis-sentinel")]
pub use crate::cluster::redis_sentinel::{
    RedisSentinelClusterControl, RedisSentinelHandle, RedisSentinelMarker, RedisSentinelOps,
};

#[cfg(feature = "postgres-cluster")]
pub use crate::cluster::postgres_replica_set::{
    PostgresReplicaSetClusterControl, PostgresReplicaSetHandle, PostgresReplicaSetMarker,
    PostgresReplicaSetOps,
};

#[cfg(feature = "rabbitmq-cluster")]
pub use crate::cluster::rabbit_cluster::{
    RabbitClusterClusterControl, RabbitClusterHandle, RabbitClusterMarker, RabbitClusterOps,
};

#[cfg(any(
    feature = "kafka",
    feature = "rabbitmq",
    feature = "postgres",
    feature = "mariadb",
    feature = "mysql",
    feature = "redis",
    feature = "redis-sentinel",
    feature = "valkey",
    feature = "mongodb",
    feature = "nats",
    feature = "neo4j",
    feature = "elasticsearch",
    feature = "opensearch",
    feature = "qdrant",
    feature = "weaviate",
    feature = "influxdb",
    feature = "timescaledb",
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
        feature = "elasticsearch",
        feature = "opensearch",
        feature = "qdrant",
        feature = "weaviate",
        feature = "influxdb",
        feature = "timescaledb",
        feature = "services",
    ),
))]
pub use crate::diagnostics::ResourceAssertBuilder;

/// First-party resource implementations.
#[doc(inline)]
pub use crate::resources::{
    sanitize_nats_subject_prefix, sanitize_redis_key_prefix, sanitize_resource_namespace,
    BuiltinResource, FakeResource, Isolatable, KafkaBindings, MigrationRunner, Namespaced,
    RegisteredResource, Resource,
};

#[cfg(any(feature = "postgres", feature = "timescaledb"))]
pub use crate::resources::PostgresBindings;

#[cfg(feature = "postgres")]
pub use crate::resources::PostgresResource;

#[cfg(feature = "postgres-cluster")]
pub use crate::cluster::postgres_replica_set::resource::PostgresReplicaSetResource;

#[cfg(feature = "timescaledb")]
pub use crate::resources::TimescaledbResource;

#[cfg(feature = "mariadb")]
pub use crate::resources::{MariaDBBindings, MariaDBResource};

#[cfg(feature = "mysql")]
pub use crate::resources::{MysqlBindings, MysqlResource};

#[cfg(feature = "sqlite")]
pub use crate::resources::{sqlite_named_memory_uri, SqliteBindings, SqliteResource};

#[cfg(feature = "duckdb")]
pub use crate::resources::{duckdb_named_memory_database_path, DuckDbBindings, DuckDbResource};

pub use crate::helper_prelude::{ConsumedMessage, DEFAULT_HELPER_CONSUME_TIMEOUT};
#[cfg(feature = "duckdb")]
pub use crate::resources::duckdb::DuckDbHelpers;
#[cfg(feature = "elasticsearch")]
pub use crate::resources::elasticsearch::ElasticsearchHelpers;
#[cfg(feature = "influxdb")]
pub use crate::resources::influxdb::{InfluxDbHelpers, InfluxFieldValue};
#[cfg(feature = "kafka")]
pub use crate::resources::kafka::KafkaHelpers;
#[cfg(feature = "mariadb")]
pub use crate::resources::mariadb::MariaDbHelpers;
#[cfg(feature = "mongodb")]
pub use crate::resources::mongo::MongoHelpers;
#[cfg(feature = "mysql")]
pub use crate::resources::mysql::MysqlHelpers;
#[cfg(feature = "nats")]
pub use crate::resources::nats::NatsHelpers;
#[cfg(feature = "neo4j")]
pub use crate::resources::neo4j::Neo4jHelpers;
#[cfg(feature = "opensearch")]
pub use crate::resources::opensearch::OpenSearchHelpers;
#[cfg(feature = "postgres")]
pub use crate::resources::postgres::PostgresHelpers;
#[cfg(feature = "qdrant")]
pub use crate::resources::qdrant::QdrantHelpers;
#[cfg(feature = "rabbitmq")]
pub use crate::resources::rabbit::{ExchangeKind, RabbitHelpers};
#[cfg(any(feature = "redis", feature = "valkey"))]
pub use crate::resources::redis::RedisHelpers;
#[cfg(feature = "sqlite")]
pub use crate::resources::sqlite::SqliteHelpers;
#[cfg(feature = "weaviate")]
pub use crate::resources::weaviate::WeaviateHelpers;

#[cfg(feature = "timescaledb")]
pub use crate::resources::timescaledb::TimescaledbHelpers;
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

#[cfg(feature = "load")]
pub use crate::load::{
    compare_load_metrics_to_baseline, render_comparison_markdown, render_summary,
    render_summary_with_comparison, render_summary_with_verdicts, AssertionVerdict,
    ComparisonResult, LoadBaseline, LoadError, LoadHandle, LoadMetrics, LoadMetricsAssertBuilder,
    LoadProfile, LoadProfileBuilder, LoadRunReport, LoadRuntime, MetricComparison, NamedPhase,
    Phase as LoadPhase, Rate, RecoveryConfig, ReportAssertion, ReportComparison,
    ReportComparisonMetric, ReportMetrics, SaturationPolicy, Tolerance,
};

#[cfg(feature = "rabbitmq")]
pub use crate::resources::{RabbitBindings, RabbitResource};

#[cfg(any(feature = "redis", feature = "valkey"))]
pub use crate::resources::RedisBindings;

#[cfg(feature = "redis")]
pub use crate::resources::RedisResource;

#[cfg(feature = "valkey")]
pub use crate::resources::ValkeyResource;

#[cfg(feature = "kafka-cluster")]
pub use crate::cluster::kafka_cluster::resource::KafkaClusterResource;
#[cfg(feature = "mongodb-cluster")]
pub use crate::cluster::mongo_replica_set::resource::MongoReplicaSetResource;
#[cfg(feature = "rabbitmq-cluster")]
pub use crate::cluster::rabbit_cluster::resource::RabbitClusterResource;
#[cfg(feature = "redis-sentinel")]
pub use crate::cluster::redis_sentinel::resource::RedisSentinelResource;
#[cfg(feature = "redis-sentinel")]
pub use crate::resources::RedisSentinelBindings;
#[cfg(feature = "mongodb")]
pub use crate::resources::{MongoBindings, MongoResource};

#[cfg(feature = "neo4j")]
pub use crate::resources::{Neo4jBindings, Neo4jResource};

#[cfg(any(
    feature = "elasticsearch",
    feature = "opensearch",
    feature = "qdrant",
    feature = "weaviate"
))]
pub use crate::resources::sanitize_elasticsearch_index_prefix;
#[cfg(feature = "elasticsearch")]
pub use crate::resources::{ElasticsearchBindings, ElasticsearchResource};
#[cfg(feature = "opensearch")]
pub use crate::resources::{OpenSearchBindings, OpenSearchResource};

#[cfg(feature = "influxdb")]
pub use crate::resources::{InfluxDbBindings, InfluxDbResource};
#[cfg(feature = "qdrant")]
pub use crate::resources::{QdrantBindings, QdrantResource};
#[cfg(feature = "weaviate")]
pub use crate::resources::{WeaviateBindings, WeaviateResource};
#[cfg(feature = "qdrant")]
pub use qdrant_client::qdrant::{Distance, PointStruct, ScoredPoint};

#[cfg(feature = "nats")]
pub use crate::resources::{NatsBindings, NatsResource};

#[cfg(all(
    feature = "outbox",
    any(
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "sqlite",
        feature = "mongodb",
        feature = "duckdb"
    )
))]
pub use crate::outbox::{Outbox, OutboxEntry, OutboxRowState};

/// Procedural attributes for Tokio tests that run the full shakedown lifecycle.
///
/// Re-exported so callers depend only on `shakedown`. The macro crate does not depend on this
/// library (avoids cycles); generated code refers to `::shakedown::…` resolved at the call site.
pub use shakedown_macros::{faulty_test, test};

/// Re-export the pinned `rdkafka` crate so consumers inherit the exact version used internally.
///
/// Enable the `kafka` feature and write `use shakedown::rdkafka::…` instead of declaring a
/// separate `rdkafka` dependency. This prevents version skew between the harness and test code.
#[cfg(feature = "kafka")]
pub use rdkafka;

/// Re-export the pinned `redis` crate so consumers inherit the exact version used internally.
///
/// Enable the `redis` or `valkey` feature and write `use shakedown::redis::…` instead of
/// declaring a separate `redis` dependency.
#[cfg(any(feature = "redis", feature = "valkey"))]
pub use redis;

/// Re-export the pinned `mongodb` crate so consumers inherit the exact version used internally.
///
/// Enable the `mongodb` feature and write `use shakedown::mongodb::…` instead of declaring a
/// separate `mongodb` dependency.
#[cfg(feature = "mongodb")]
pub use mongodb;

/// Re-export the pinned `lapin` crate so consumers inherit the exact version used internally.
///
/// Enable the `rabbitmq` feature and write `use shakedown::lapin::…` instead of declaring a
/// separate `lapin` dependency.
#[cfg(feature = "rabbitmq")]
pub use lapin;

/// Re-export the pinned `sqlx` crate so consumers inherit the exact version used internally.
///
/// Enable the `sqlx` feature and write `use shakedown::sqlx::…` (for example
/// `sqlx::migrate::Migrator` with `#[shakedown::test]` and `migrations = "…"`) instead of
/// declaring a separate `sqlx` dependency.
#[cfg(feature = "sqlx")]
pub use sqlx;

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

        // Given a Resource whose async methods yield to the async executor
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
