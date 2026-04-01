//! Scenario lifecycle types and execution.
//!
//! The scenario lifecycle consists of six ordered phases:
//!
//! 1. Provision resources
//! 2. Check resource readiness
//! 3. Seed data into resources
//! 4. Run the scenario logic
//! 5. Assert scenario outcomes
//! 6. Teardown resources
//!
//! If any phase fails, the lifecycle will attempt to run teardown for resources that were
//! successfully provisioned. The error classification distinguishes between scenario errors
//! (user logic/assertions) and infrastructure errors (provision/readiness failures).
//!
//! ## Event logging
//!
//! The lifecycle emits structured events during each phase. These events are collected in an
//! [`EventLog`] that can be retrieved for debugging or failure analysis.
//!
//! # Example
//!
//! ```ignore
//! use shakedown::{run_scenario, Context, EventLog};
//!
//! async fn my_scenario(ctx: &mut Context) {
//!     // Scenario implementation
//! }
//!
//! #[tokio::test]
//! async fn test_scenario_with_events() {
//!     let mut ctx = Context::new();
//!     let mut event_log = EventLog::new();
//!
//!     run_scenario(&mut ctx, "test-scenario", |c| Box::pin(my_scenario(c)), &mut event_log)
//!         .await
//!         .unwrap();
//!
//!     // Retrieve application events after scenario completion (see also [`EventLog::entries`])
//!     assert!(event_log
//!         .events()
//!         .any(|e| e.source == "scenario" && e.level == EventLevel::Info));
//! }
//! ```

#[cfg(any(feature = "mock-services", feature = "services"))]
use crate::error::InternalError;
use crate::error::{InfrastructureError, ResourceHandle, ScenarioId, ShakedownError};
#[cfg(feature = "mock-services")]
use crate::event::Event;
use crate::event::{EventLevel, EventLog};
#[cfg(feature = "duckdb")]
use crate::helpers::DuckDbHelpers;
#[cfg(feature = "elasticsearch")]
use crate::helpers::ElasticsearchHelpers;
#[cfg(feature = "influxdb")]
use crate::helpers::InfluxDbHelpers;
#[cfg(feature = "kafka")]
use crate::helpers::KafkaHelpers;
#[cfg(feature = "mariadb")]
use crate::helpers::MariaDbHelpers;
#[cfg(feature = "mongodb")]
use crate::helpers::MongoHelpers;
#[cfg(feature = "mysql")]
use crate::helpers::MysqlHelpers;
#[cfg(feature = "nats")]
use crate::helpers::NatsHelpers;
#[cfg(feature = "neo4j")]
use crate::helpers::Neo4jHelpers;
#[cfg(feature = "opensearch")]
use crate::helpers::OpenSearchHelpers;
#[cfg(feature = "postgres")]
use crate::helpers::PostgresHelpers;
#[cfg(feature = "qdrant")]
use crate::helpers::QdrantHelpers;
#[cfg(feature = "rabbitmq")]
use crate::helpers::RabbitHelpers;
#[cfg(any(feature = "redis", feature = "valkey"))]
use crate::helpers::RedisHelpers;
#[cfg(feature = "sqlite")]
use crate::helpers::SqliteHelpers;
#[cfg(feature = "timescaledb")]
use crate::helpers::TimescaledbHelpers;
#[cfg(feature = "weaviate")]
use crate::helpers::WeaviateHelpers;
#[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
use crate::mock_service::grpc::server::{spawn_grpc_mock_service_server, GrpcMockServiceRuntime};
#[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
use crate::mock_service::grpc::GrpcMockServiceTrait;
#[cfg(feature = "mock-services")]
use crate::mock_service::handle::MockServiceHandle;
#[cfg(feature = "mock-services")]
use crate::mock_service::http::server::{spawn_http_mock_service_server, HttpMockServiceServer};
#[cfg(feature = "mock-services")]
use crate::mock_service::http::HttpMockServiceTrait;
#[cfg(feature = "mock-services")]
use crate::mock_service::types::MockServiceResources;
#[cfg(all(
    feature = "outbox",
    any(
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "sqlite"
    )
))]
use crate::outbox::Outbox;
#[cfg(feature = "duckdb")]
use crate::resources::DuckDbBindings;
#[cfg(feature = "elasticsearch")]
use crate::resources::ElasticsearchBindings;
#[cfg(feature = "influxdb")]
use crate::resources::InfluxDbBindings;
#[cfg(feature = "mariadb")]
use crate::resources::MariaDBBindings;
#[cfg(feature = "mongodb")]
use crate::resources::MongoBindings;
#[cfg(feature = "mysql")]
use crate::resources::MysqlBindings;
#[cfg(feature = "nats")]
use crate::resources::NatsBindings;
#[cfg(feature = "neo4j")]
use crate::resources::Neo4jBindings;
#[cfg(feature = "opensearch")]
use crate::resources::OpenSearchBindings;
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
use crate::resources::PostgresBindings;
#[cfg(feature = "qdrant")]
use crate::resources::QdrantBindings;
#[cfg(feature = "rabbitmq")]
use crate::resources::RabbitBindings;
#[cfg(any(feature = "redis", feature = "valkey"))]
use crate::resources::RedisBindings;
#[cfg(feature = "sqlite")]
use crate::resources::SqliteBindings;
#[cfg(feature = "weaviate")]
use crate::resources::WeaviateBindings;
use crate::resources::{
    BuiltinResource, FakeResource, KafkaBindings, RegisteredResource, Resource,
};
#[cfg(feature = "services")]
use crate::service::ServiceHandle;
use async_trait::async_trait;
#[cfg(any(feature = "mock-services", feature = "services"))]
use std::collections::HashMap;
#[cfg(feature = "mock-services")]
use std::sync::Arc;

pub use crate::phase::Phase;

/// High-level state machine position for a scenario (reserved for richer orchestration).
#[derive(Debug)]
pub enum LifecycleState {
    /// Not yet started
    NotStarted,
    /// Currently running a phase
    Running(Phase),
    /// Completed successfully
    Completed,
    /// Failed during a specific phase
    Failed(Phase, Box<dyn std::error::Error + Send + Sync>),
}

/// The environment backend abstraction.
///
/// This trait abstracts the underlying infrastructure where scenarios run,
/// allowing for different backends (Docker, local processes, cloud environments, etc.)
#[async_trait]
pub trait EnvironmentBackend: Send + Sync {
    /// Provision a single resource.
    ///
    /// This method is called during the provision phase. The environment backend
    /// should handle setting up the resource with scenario-scoped isolation.
    async fn provision_resource(
        &mut self,
        resource: RegisteredResource,
    ) -> Result<(), InfrastructureError>;

    /// Check readiness of all resources.
    async fn check_readiness(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    /// Teardown all resources.
    ///
    /// This method should be idempotent - it can be called multiple times
    /// safely, even if some resources were never provisioned or already torn down.
    async fn teardown(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }
}

/// A default stub environment backend that does nothing.
#[derive(Debug, Default)]
pub struct NoopEnvironmentBackend;

#[async_trait]
impl EnvironmentBackend for NoopEnvironmentBackend {
    async fn provision_resource(
        &mut self,
        _resource: RegisteredResource,
    ) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn check_readiness(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }

    async fn teardown(&self) -> Result<(), InfrastructureError> {
        Ok(())
    }
}

/// Represents the kind of scenario being executed.
///
/// This is used by `#[shakedown::faulty_test]` to mark fault-oriented scenarios,
/// and by future fault-injection code to decide whether to apply faults like
/// Toxiproxy toxics.
#[allow(clippy::derivable_impls)] // Manual impl needed for enum without #[default] marker
#[derive(Debug, Clone, Copy, PartialEq, Eq, displaydoc::Display)]
#[ignore_extra_doc_attributes]
pub enum ScenarioKind {
    /// Normal
    Normal,
    /// Faulty
    Faulty,
}

/// Mutable scenario state for one run: resources, backend, ids, and optional messaging/DB bindings.
///
/// Why: the harness and scenario body share one place for registered infrastructure and
/// per-run metadata so [`run_scenario`] can drive provision → teardown without global singletons.
///
/// How: constructed with [`Context::new`] or by `#[shakedown::test]`; the scenario function receives
/// `&mut Context` to register extras or read [`Context::scenario_id`], [`Context::kafka_bindings`],
/// [`Context::rabbit_bindings`] (with the `rabbitmq` feature), [`Context::redis_bindings`] /
/// [`Context::redis`] (with the `redis` / `valkey` features), [`Context::mongo_bindings`] /
/// [`Context::mongo`] (with the `mongodb` feature), [`Context::nats_bindings`] / [`Context::nats`]
/// (with the `nats` feature), [`Context::neo4j_bindings`] / [`Context::neo4j`] (with the `neo4j` feature),
/// [`Context::elasticsearch_bindings`] / [`Context::elasticsearch`] (with the `elasticsearch` feature),
/// [`Context::timescaledb_bindings`] / [`Context::timescaledb`] (with the `timescaledb` feature),
/// and (with the `postgres` / `mariadb` / `mysql` / `sqlite` features)
/// relational bindings accessors.
pub struct Context {
    /// Registered resources
    resources: Vec<RegisteredResource>,
    /// Environment backend for lifecycle operations
    environment_backend: Box<dyn EnvironmentBackend>,
    /// Scenario ID for isolation
    scenario_id: ScenarioId,
    /// Kafka bindings when a Kafka resource populated them (from provision metadata).
    kafka_bindings: Option<KafkaBindings>,
    /// RabbitMQ bindings when a Rabbit resource populated them (`rabbitmq` feature).
    #[cfg(feature = "rabbitmq")]
    rabbit_bindings: Option<RabbitBindings>,
    /// Postgres bindings when a Postgres resource populated them (`postgres` feature).
    #[cfg(feature = "postgres")]
    postgres_bindings: Option<PostgresBindings>,
    /// MariaDB bindings when a MariaDB resource populated them (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    mariadb_bindings: Option<MariaDBBindings>,
    /// Oracle MySQL bindings when a MySQL resource populated them (`mysql` feature).
    #[cfg(feature = "mysql")]
    mysql_bindings: Option<MysqlBindings>,
    /// SQLite bindings when a SQLite resource populated them (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    sqlite_bindings: Option<SqliteBindings>,
    /// DuckDB bindings when a DuckDB resource populated them (`duckdb` feature).
    #[cfg(feature = "duckdb")]
    duckdb_bindings: Option<DuckDbBindings>,
    /// Redis / Valkey bindings when a matching resource populated them (`redis` / `valkey` features).
    #[cfg(any(feature = "redis", feature = "valkey"))]
    redis_bindings: Option<RedisBindings>,
    /// MongoDB bindings when a Mongo resource populated them (`mongodb` feature).
    #[cfg(feature = "mongodb")]
    mongo_bindings: Option<MongoBindings>,
    /// NATS bindings when a NATS resource populated them (`nats` feature).
    #[cfg(feature = "nats")]
    nats_bindings: Option<NatsBindings>,
    /// Neo4j bindings when a Neo4j resource populated them (`neo4j` feature).
    #[cfg(feature = "neo4j")]
    neo4j_bindings: Option<Neo4jBindings>,
    /// Elasticsearch bindings when an Elasticsearch resource populated them (`elasticsearch` feature).
    #[cfg(feature = "elasticsearch")]
    elasticsearch_bindings: Option<ElasticsearchBindings>,
    /// OpenSearch bindings when an OpenSearch resource populated them (`opensearch` feature).
    #[cfg(feature = "opensearch")]
    opensearch_bindings: Option<OpenSearchBindings>,
    /// Qdrant bindings when a Qdrant resource populated them (`qdrant` feature).
    #[cfg(feature = "qdrant")]
    qdrant_bindings: Option<QdrantBindings>,
    /// Weaviate bindings when a Weaviate resource populated them (`weaviate` feature).
    #[cfg(feature = "weaviate")]
    weaviate_bindings: Option<WeaviateBindings>,
    /// InfluxDB bindings when an InfluxDB resource populated them (`influxdb` feature).
    #[cfg(feature = "influxdb")]
    influxdb_bindings: Option<InfluxDbBindings>,
    /// TimescaleDB bindings when a TimescaleDB resource populated them (`timescaledb` feature).
    #[cfg(feature = "timescaledb")]
    timescaledb_bindings: Option<PostgresBindings>,
    /// Kind of scenario being executed (Normal or Faulty)
    scenario_kind: ScenarioKind,
    /// Optional label from `#[shakedown::faulty_test(fault_profile = "...")]` for presets / diagnostics.
    fault_profile: Option<String>,
    /// Toxiproxy control handle when the `fault-injection` feature is on and Toxiproxy provisioned.
    #[cfg(feature = "fault-injection")]
    fault_handle: Option<crate::fault_injection::FaultHandle>,
    /// Toxiproxy HTTP API base URL once seen in provision handles (Kafka fault path; `fault-injection` + `kafka`).
    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    toxiproxy_control_api_base_url: Option<String>,
    /// When Kafka provisioned before Toxiproxy: topic prefix + direct bootstrap until the proxy is created.
    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    kafka_fault_proxy_pending: Option<(String, String)>,
    /// Skip applying Kafka bindings from handles until the proxied bootstrap is known.
    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    kafka_bindings_apply_deferred: bool,
    /// Per-container Docker stats rings (diagnostics); set by [`run_scenario`] when monitoring runs.
    #[cfg(all(
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
        feature = "diagnostics",
        not(feature = "no-diagnostics"),
    ))]
    stats_rings: Option<crate::diagnostics::SharedResourceStatsRings>,
    /// HTTP bridges registered before [`run_scenario`] starts servers (after seed).
    #[cfg(feature = "mock-services")]
    pending_http_mock_services: Vec<(String, Arc<dyn HttpMockServiceTrait>)>,
    /// Running HTTP bridge handles (populated when servers start).
    #[cfg(feature = "mock-services")]
    mock_service_handles: HashMap<String, Arc<MockServiceHandle>>,
    /// Shutdown handles for HTTP bridge accept loops (paired with names for timeline messages).
    #[cfg(feature = "mock-services")]
    http_mock_service_runtimes: Vec<(String, HttpMockServiceServer)>,
    /// gRPC bridges registered before [`run_scenario`] starts tonic servers (after seed).
    #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
    pending_grpc_mock_services: Vec<(String, Arc<dyn GrpcMockServiceTrait>)>,
    /// Running gRPC mock service servers (tonic transport).
    #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
    grpc_mock_service_runtimes: Vec<(String, GrpcMockServiceRuntime)>,
    /// True once HTTP and gRPC mock service servers have been started for this [`run_scenario`] invocation.
    #[cfg(feature = "mock-services")]
    bridge_servers_phase_complete: bool,
    /// Staged [`crate::service::ServiceSpec`] entries; [`ServiceHandle`] filled when containers are healthy (service container lifecycle).
    #[cfg(feature = "services")]
    pub(crate) services: HashMap<String, crate::service::ServiceState>,
}

impl Context {
    /// Create a new empty context with the default noop environment backend.
    pub fn new() -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(NoopEnvironmentBackend),
            scenario_id: ScenarioId::empty(),
            kafka_bindings: None,
            #[cfg(feature = "rabbitmq")]
            rabbit_bindings: None,
            #[cfg(feature = "postgres")]
            postgres_bindings: None,
            #[cfg(feature = "mariadb")]
            mariadb_bindings: None,
            #[cfg(feature = "mysql")]
            mysql_bindings: None,
            #[cfg(feature = "sqlite")]
            sqlite_bindings: None,
            #[cfg(feature = "duckdb")]
            duckdb_bindings: None,
            #[cfg(any(feature = "redis", feature = "valkey"))]
            redis_bindings: None,
            #[cfg(feature = "mongodb")]
            mongo_bindings: None,
            #[cfg(feature = "nats")]
            nats_bindings: None,
            #[cfg(feature = "neo4j")]
            neo4j_bindings: None,
            #[cfg(feature = "elasticsearch")]
            elasticsearch_bindings: None,
            #[cfg(feature = "opensearch")]
            opensearch_bindings: None,
            #[cfg(feature = "qdrant")]
            qdrant_bindings: None,
            #[cfg(feature = "weaviate")]
            weaviate_bindings: None,
            #[cfg(feature = "influxdb")]
            influxdb_bindings: None,
            #[cfg(feature = "timescaledb")]
            timescaledb_bindings: None,
            scenario_kind: ScenarioKind::Normal,
            fault_profile: None,
            #[cfg(feature = "fault-injection")]
            fault_handle: None,
            #[cfg(all(feature = "fault-injection", feature = "kafka"))]
            toxiproxy_control_api_base_url: None,
            #[cfg(all(feature = "fault-injection", feature = "kafka"))]
            kafka_fault_proxy_pending: None,
            #[cfg(all(feature = "fault-injection", feature = "kafka"))]
            kafka_bindings_apply_deferred: false,
            #[cfg(all(
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
                feature = "diagnostics",
                not(feature = "no-diagnostics"),
            ))]
            stats_rings: None,
            #[cfg(feature = "mock-services")]
            pending_http_mock_services: Vec::new(),
            #[cfg(feature = "mock-services")]
            mock_service_handles: HashMap::new(),
            #[cfg(feature = "mock-services")]
            http_mock_service_runtimes: Vec::new(),
            #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
            pending_grpc_mock_services: Vec::new(),
            #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
            grpc_mock_service_runtimes: Vec::new(),
            #[cfg(feature = "mock-services")]
            bridge_servers_phase_complete: false,
            #[cfg(feature = "services")]
            services: HashMap::new(),
        }
    }

    /// Create a new context with a custom environment backend.
    pub fn with_backend(backend: impl EnvironmentBackend + 'static) -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(backend),
            scenario_id: ScenarioId::empty(),
            kafka_bindings: None,
            #[cfg(feature = "rabbitmq")]
            rabbit_bindings: None,
            #[cfg(feature = "postgres")]
            postgres_bindings: None,
            #[cfg(feature = "mariadb")]
            mariadb_bindings: None,
            #[cfg(feature = "mysql")]
            mysql_bindings: None,
            #[cfg(feature = "sqlite")]
            sqlite_bindings: None,
            #[cfg(feature = "duckdb")]
            duckdb_bindings: None,
            #[cfg(any(feature = "redis", feature = "valkey"))]
            redis_bindings: None,
            #[cfg(feature = "mongodb")]
            mongo_bindings: None,
            #[cfg(feature = "nats")]
            nats_bindings: None,
            #[cfg(feature = "neo4j")]
            neo4j_bindings: None,
            #[cfg(feature = "elasticsearch")]
            elasticsearch_bindings: None,
            #[cfg(feature = "opensearch")]
            opensearch_bindings: None,
            #[cfg(feature = "qdrant")]
            qdrant_bindings: None,
            #[cfg(feature = "weaviate")]
            weaviate_bindings: None,
            #[cfg(feature = "influxdb")]
            influxdb_bindings: None,
            #[cfg(feature = "timescaledb")]
            timescaledb_bindings: None,
            scenario_kind: ScenarioKind::Normal,
            fault_profile: None,
            #[cfg(feature = "fault-injection")]
            fault_handle: None,
            #[cfg(all(feature = "fault-injection", feature = "kafka"))]
            toxiproxy_control_api_base_url: None,
            #[cfg(all(feature = "fault-injection", feature = "kafka"))]
            kafka_fault_proxy_pending: None,
            #[cfg(all(feature = "fault-injection", feature = "kafka"))]
            kafka_bindings_apply_deferred: false,
            #[cfg(all(
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
                feature = "diagnostics",
                not(feature = "no-diagnostics"),
            ))]
            stats_rings: None,
            #[cfg(feature = "mock-services")]
            pending_http_mock_services: Vec::new(),
            #[cfg(feature = "mock-services")]
            mock_service_handles: HashMap::new(),
            #[cfg(feature = "mock-services")]
            http_mock_service_runtimes: Vec::new(),
            #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
            pending_grpc_mock_services: Vec::new(),
            #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
            grpc_mock_service_runtimes: Vec::new(),
            #[cfg(feature = "mock-services")]
            bridge_servers_phase_complete: false,
            #[cfg(feature = "services")]
            services: HashMap::new(),
        }
    }

    /// Create a new context with a specific scenario ID.
    pub fn with_scenario_id(scenario_id: impl Into<String>) -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(NoopEnvironmentBackend),
            scenario_id: ScenarioId::new(scenario_id),
            kafka_bindings: None,
            #[cfg(feature = "rabbitmq")]
            rabbit_bindings: None,
            #[cfg(feature = "postgres")]
            postgres_bindings: None,
            #[cfg(feature = "mariadb")]
            mariadb_bindings: None,
            #[cfg(feature = "mysql")]
            mysql_bindings: None,
            #[cfg(feature = "sqlite")]
            sqlite_bindings: None,
            #[cfg(feature = "duckdb")]
            duckdb_bindings: None,
            #[cfg(any(feature = "redis", feature = "valkey"))]
            redis_bindings: None,
            #[cfg(feature = "mongodb")]
            mongo_bindings: None,
            #[cfg(feature = "nats")]
            nats_bindings: None,
            #[cfg(feature = "neo4j")]
            neo4j_bindings: None,
            #[cfg(feature = "elasticsearch")]
            elasticsearch_bindings: None,
            #[cfg(feature = "opensearch")]
            opensearch_bindings: None,
            #[cfg(feature = "qdrant")]
            qdrant_bindings: None,
            #[cfg(feature = "weaviate")]
            weaviate_bindings: None,
            #[cfg(feature = "influxdb")]
            influxdb_bindings: None,
            #[cfg(feature = "timescaledb")]
            timescaledb_bindings: None,
            scenario_kind: ScenarioKind::Normal,
            fault_profile: None,
            #[cfg(feature = "fault-injection")]
            fault_handle: None,
            #[cfg(all(feature = "fault-injection", feature = "kafka"))]
            toxiproxy_control_api_base_url: None,
            #[cfg(all(feature = "fault-injection", feature = "kafka"))]
            kafka_fault_proxy_pending: None,
            #[cfg(all(feature = "fault-injection", feature = "kafka"))]
            kafka_bindings_apply_deferred: false,
            #[cfg(all(
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
                feature = "diagnostics",
                not(feature = "no-diagnostics"),
            ))]
            stats_rings: None,
            #[cfg(feature = "mock-services")]
            pending_http_mock_services: Vec::new(),
            #[cfg(feature = "mock-services")]
            mock_service_handles: HashMap::new(),
            #[cfg(feature = "mock-services")]
            http_mock_service_runtimes: Vec::new(),
            #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
            pending_grpc_mock_services: Vec::new(),
            #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
            grpc_mock_service_runtimes: Vec::new(),
            #[cfg(feature = "mock-services")]
            bridge_servers_phase_complete: false,
            #[cfg(feature = "services")]
            services: HashMap::new(),
        }
    }

    /// Register a first-party [`BuiltinResource`].
    pub fn register_builtin(&mut self, resource: BuiltinResource) {
        self.resources.push(RegisteredResource::Builtin(resource));
    }

    /// Register an extension resource with the context.
    pub fn register_extension<R: Resource + 'static>(&mut self, resource: R) {
        self.resources
            .push(RegisteredResource::Extension(Box::new(resource)));
    }

    /// Get a reference to all registered resources.
    pub fn resources(&self) -> &[RegisteredResource] {
        &self.resources
    }

    /// Get a mutable reference to all registered resources.
    pub fn resources_mut(&mut self) -> &mut Vec<RegisteredResource> {
        &mut self.resources
    }

    /// Get a reference to the environment backend.
    pub fn environment_backend(&self) -> &dyn EnvironmentBackend {
        &*self.environment_backend
    }

    /// Get a mutable reference to the environment backend.
    pub fn environment_backend_mut(&mut self) -> &mut dyn EnvironmentBackend {
        &mut *self.environment_backend
    }

    /// Get the scenario ID.
    pub fn scenario_id(&self) -> &ScenarioId {
        &self.scenario_id
    }

    /// Set Kafka bindings for this scenario (typically after Kafka provision).
    pub fn set_kafka_bindings(&mut self, kafka_bindings: KafkaBindings) {
        self.kafka_bindings = Some(kafka_bindings);
    }

    /// Get a reference to Kafka bindings if available.
    pub fn kafka_bindings(&self) -> Option<&KafkaBindings> {
        self.kafka_bindings.as_ref()
    }

    /// Set RabbitMQ bindings for this scenario (typically after Rabbit provision).
    #[cfg(feature = "rabbitmq")]
    pub fn set_rabbit_bindings(&mut self, rabbit_bindings: RabbitBindings) {
        self.rabbit_bindings = Some(rabbit_bindings);
    }

    /// Get RabbitMQ bindings if available (`rabbitmq` feature).
    #[cfg(feature = "rabbitmq")]
    pub fn rabbit_bindings(&self) -> Option<&RabbitBindings> {
        self.rabbit_bindings.as_ref()
    }

    /// Set Postgres bindings for this scenario (typically after Postgres provision).
    #[cfg(feature = "postgres")]
    pub fn set_postgres_bindings(&mut self, postgres_bindings: PostgresBindings) {
        self.postgres_bindings = Some(postgres_bindings);
    }

    /// Get Postgres bindings if available (`postgres` feature).
    #[cfg(feature = "postgres")]
    pub fn postgres_bindings(&self) -> Option<&PostgresBindings> {
        self.postgres_bindings.as_ref()
    }

    /// Set MariaDB bindings for this scenario (typically after MariaDB provision).
    #[cfg(feature = "mariadb")]
    pub fn set_mariadb_bindings(&mut self, mariadb_bindings: MariaDBBindings) {
        self.mariadb_bindings = Some(mariadb_bindings);
    }

    /// Get MariaDB bindings if available (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    pub fn mariadb_bindings(&self) -> Option<&MariaDBBindings> {
        self.mariadb_bindings.as_ref()
    }

    /// Set MySQL (Oracle) bindings for this scenario (typically after MySQL provision).
    #[cfg(feature = "mysql")]
    pub fn set_mysql_bindings(&mut self, mysql_bindings: MysqlBindings) {
        self.mysql_bindings = Some(mysql_bindings);
    }

    /// Get Oracle MySQL bindings if available (`mysql` feature).
    #[cfg(feature = "mysql")]
    pub fn mysql_bindings(&self) -> Option<&MysqlBindings> {
        self.mysql_bindings.as_ref()
    }

    /// Set SQLite bindings for this scenario (typically after SQLite provision).
    #[cfg(feature = "sqlite")]
    pub fn set_sqlite_bindings(&mut self, sqlite_bindings: SqliteBindings) {
        self.sqlite_bindings = Some(sqlite_bindings);
    }

    /// Get SQLite bindings if available (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    pub fn sqlite_bindings(&self) -> Option<&SqliteBindings> {
        self.sqlite_bindings.as_ref()
    }

    /// Set DuckDB bindings for this scenario (typically after DuckDB provision).
    #[cfg(feature = "duckdb")]
    pub fn set_duckdb_bindings(&mut self, duckdb_bindings: DuckDbBindings) {
        self.duckdb_bindings = Some(duckdb_bindings);
    }

    /// DuckDB connection path when a DuckDB resource populated them (`duckdb` feature).
    #[cfg(feature = "duckdb")]
    pub fn duckdb_bindings(&self) -> Option<&DuckDbBindings> {
        self.duckdb_bindings.as_ref()
    }

    /// Set Redis / Valkey bindings for this scenario (typically after Redis or Valkey provision).
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn set_redis_bindings(&mut self, redis_bindings: RedisBindings) {
        self.redis_bindings = Some(redis_bindings);
    }

    /// Redis or Valkey connection details when a matching resource populated them (`redis` / `valkey` features).
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn redis_bindings(&self) -> Option<&RedisBindings> {
        self.redis_bindings.as_ref()
    }

    /// Set MongoDB bindings for this scenario (typically after Mongo provision).
    #[cfg(feature = "mongodb")]
    pub fn set_mongo_bindings(&mut self, mongo_bindings: MongoBindings) {
        self.mongo_bindings = Some(mongo_bindings);
    }

    /// MongoDB connection details when a Mongo resource populated them (`mongodb` feature).
    #[cfg(feature = "mongodb")]
    pub fn mongo_bindings(&self) -> Option<&MongoBindings> {
        self.mongo_bindings.as_ref()
    }

    /// Set NATS bindings for this scenario (typically after NATS provision).
    #[cfg(feature = "nats")]
    pub fn set_nats_bindings(&mut self, nats_bindings: NatsBindings) {
        self.nats_bindings = Some(nats_bindings);
    }

    /// NATS connection details when a NATS resource populated them (`nats` feature).
    #[cfg(feature = "nats")]
    pub fn nats_bindings(&self) -> Option<&NatsBindings> {
        self.nats_bindings.as_ref()
    }

    /// Set Neo4j bindings for this scenario (typically after Neo4j provision).
    #[cfg(feature = "neo4j")]
    pub fn set_neo4j_bindings(&mut self, neo4j_bindings: Neo4jBindings) {
        self.neo4j_bindings = Some(neo4j_bindings);
    }

    /// Neo4j connection details when a Neo4j resource populated them (`neo4j` feature).
    #[cfg(feature = "neo4j")]
    pub fn neo4j_bindings(&self) -> Option<&Neo4jBindings> {
        self.neo4j_bindings.as_ref()
    }

    /// Set Elasticsearch bindings for this scenario (typically after Elasticsearch provision).
    #[cfg(feature = "elasticsearch")]
    pub fn set_elasticsearch_bindings(&mut self, elasticsearch_bindings: ElasticsearchBindings) {
        self.elasticsearch_bindings = Some(elasticsearch_bindings);
    }

    /// Elasticsearch HTTP URL and index prefix when an Elasticsearch resource populated them (`elasticsearch` feature).
    #[cfg(feature = "elasticsearch")]
    pub fn elasticsearch_bindings(&self) -> Option<&ElasticsearchBindings> {
        self.elasticsearch_bindings.as_ref()
    }

    /// Set OpenSearch bindings for this scenario (typically after OpenSearch provision).
    #[cfg(feature = "opensearch")]
    pub fn set_opensearch_bindings(&mut self, opensearch_bindings: OpenSearchBindings) {
        self.opensearch_bindings = Some(opensearch_bindings);
    }

    /// OpenSearch HTTP URL and index prefix when an OpenSearch resource populated them (`opensearch` feature).
    #[cfg(feature = "opensearch")]
    pub fn opensearch_bindings(&self) -> Option<&OpenSearchBindings> {
        self.opensearch_bindings.as_ref()
    }

    /// Set Qdrant bindings for this scenario (typically after Qdrant provision).
    #[cfg(feature = "qdrant")]
    pub fn set_qdrant_bindings(&mut self, qdrant_bindings: QdrantBindings) {
        self.qdrant_bindings = Some(qdrant_bindings);
    }

    /// Qdrant gRPC/REST endpoints and collection prefix when a Qdrant resource populated them (`qdrant` feature).
    #[cfg(feature = "qdrant")]
    pub fn qdrant_bindings(&self) -> Option<&QdrantBindings> {
        self.qdrant_bindings.as_ref()
    }

    /// Set Weaviate bindings for this scenario (typically after Weaviate provision).
    #[cfg(feature = "weaviate")]
    pub fn set_weaviate_bindings(&mut self, weaviate_bindings: WeaviateBindings) {
        self.weaviate_bindings = Some(weaviate_bindings);
    }

    /// Weaviate REST URL and class name prefix when a Weaviate resource populated them (`weaviate` feature).
    #[cfg(feature = "weaviate")]
    pub fn weaviate_bindings(&self) -> Option<&WeaviateBindings> {
        self.weaviate_bindings.as_ref()
    }

    /// Set InfluxDB bindings for this scenario (typically after InfluxDB provision).
    #[cfg(feature = "influxdb")]
    pub fn set_influxdb_bindings(&mut self, influxdb_bindings: InfluxDbBindings) {
        self.influxdb_bindings = Some(influxdb_bindings);
    }

    /// InfluxDB HTTP URL, token, org, and scenario bucket when an InfluxDB resource populated them (`influxdb` feature).
    #[cfg(feature = "influxdb")]
    pub fn influxdb_bindings(&self) -> Option<&InfluxDbBindings> {
        self.influxdb_bindings.as_ref()
    }

    /// Set TimescaleDB bindings for this scenario (typically after TimescaleDB provision).
    #[cfg(feature = "timescaledb")]
    pub fn set_timescaledb_bindings(&mut self, timescaledb_bindings: PostgresBindings) {
        self.timescaledb_bindings = Some(timescaledb_bindings);
    }

    /// TimescaleDB connection details when a TimescaleDB resource populated them (`timescaledb` feature).
    #[cfg(feature = "timescaledb")]
    pub fn timescaledb_bindings(&self) -> Option<&PostgresBindings> {
        self.timescaledb_bindings.as_ref()
    }

    /// Register an HTTP bridge to be started after the seed phase of [`run_scenario`] (requires `bridges`).
    ///
    /// Call before [`run_scenario`]. After servers start, use [`Self::mock_service_url`] and [`Self::bridge`].
    #[cfg(feature = "mock-services")]
    pub fn register_http_bridge(
        &mut self,
        name: &str,
        bridge: impl HttpMockServiceTrait,
    ) -> Result<(), ShakedownError> {
        if self.bridge_servers_phase_complete {
            return Err(ShakedownError::Internal(InternalError::InvalidConfiguration(
                String::from(
                    "register_http_bridge called after mock service servers were started for this scenario run",
                ),
            )));
        }
        #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
        let grpc_name_taken = self
            .pending_grpc_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "grpc-mock-services")))]
        let grpc_name_taken = false;

        if self.mock_service_handles.contains_key(name)
            || self
                .pending_http_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || grpc_name_taken
        {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(format!("duplicate bridge name '{name}'")),
            ));
        }
        self.pending_http_mock_services
            .push((name.to_owned(), Arc::new(bridge)));
        Ok(())
    }

    /// Register a gRPC bridge to be started after the seed phase of [`run_scenario`] (`grpc-bridges` + `bridges`).
    ///
    /// Call before [`run_scenario`]. After servers start, use [`Self::mock_service_url`] and [`Self::bridge`].
    #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
    pub fn register_grpc_bridge(
        &mut self,
        name: &str,
        bridge: impl GrpcMockServiceTrait,
    ) -> Result<(), ShakedownError> {
        if self.bridge_servers_phase_complete {
            return Err(ShakedownError::Internal(InternalError::InvalidConfiguration(
                String::from(
                    "register_grpc_bridge called after mock service servers were started for this scenario run",
                ),
            )));
        }
        if self.mock_service_handles.contains_key(name)
            || self
                .pending_http_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || self
                .pending_grpc_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
        {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(format!("duplicate bridge name '{name}'")),
            ));
        }
        self.pending_grpc_mock_services
            .push((name.to_owned(), Arc::new(bridge)));
        Ok(())
    }

    /// Test-only: control [`Self::register_http_bridge`] / [`Self::register_grpc_bridge`] phase guards.
    #[cfg(all(test, feature = "mock-services"))]
    pub(crate) fn set_bridge_servers_phase_complete_for_test(&mut self, complete: bool) {
        self.bridge_servers_phase_complete = complete;
    }

    /// Client-facing base URL for a registered bridge (HTTP or gRPC).
    ///
    /// Returns [`None`] if no bridge was registered under `name` or mock service servers have not
    /// started yet for this run (e.g. called before [`run_scenario`] has bound listeners).
    #[cfg(feature = "mock-services")]
    pub fn mock_service_url(&self, name: &str) -> Option<String> {
        self.mock_service_handles
            .get(name)
            .map(|handle| handle.client_url.clone())
    }

    /// Handle for assertions on recorded bridge traffic (HTTP or gRPC).
    ///
    /// Returns [`None`] if no bridge was registered under `name` or mock service servers have not
    /// started yet for this run.
    #[cfg(feature = "mock-services")]
    pub fn mock_service(&self, name: &str) -> Option<&MockServiceHandle> {
        self.mock_service_handles.get(name).map(|arc| arc.as_ref())
    }

    /// Build an outbox helper bound to this context (`outbox` + relational features).
    #[cfg(all(
        feature = "outbox",
        any(
            feature = "postgres",
            feature = "mariadb",
            feature = "mysql",
            feature = "sqlite"
        )
    ))]
    pub fn outbox(&self) -> Outbox<'_> {
        Outbox::new(self)
    }

    /// Kafka produce/consume helpers (`kafka` feature).
    #[cfg(feature = "kafka")]
    pub fn kafka(&self) -> KafkaHelpers<'_> {
        KafkaHelpers::new(self)
    }

    /// RabbitMQ publish/topology helpers (`rabbitmq` feature).
    #[cfg(feature = "rabbitmq")]
    pub fn rabbit(&self) -> RabbitHelpers<'_> {
        RabbitHelpers::new(self)
    }

    /// PostgreSQL execute/query helpers (`postgres` feature).
    #[cfg(feature = "postgres")]
    pub fn postgres(&self) -> PostgresHelpers<'_> {
        PostgresHelpers::new(self)
    }

    /// MariaDB execute/query helpers (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    pub fn mariadb(&self) -> MariaDbHelpers<'_> {
        MariaDbHelpers::new(self)
    }

    /// Oracle MySQL execute/query helpers (`mysql` feature).
    #[cfg(feature = "mysql")]
    pub fn mysql(&self) -> MysqlHelpers<'_> {
        MysqlHelpers::new(self)
    }

    /// SQLite execute/query helpers (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    pub fn sqlite(&self) -> SqliteHelpers<'_> {
        SqliteHelpers::new(self)
    }

    /// DuckDB execute/query helpers (`duckdb` feature).
    #[cfg(feature = "duckdb")]
    pub fn duckdb(&self) -> DuckDbHelpers<'_> {
        DuckDbHelpers::new(self)
    }

    /// Redis / Valkey key/value helpers (`redis` and/or `valkey` features).
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn redis(&self) -> RedisHelpers<'_> {
        RedisHelpers::new(self)
    }

    /// MongoDB document helpers (`mongodb` feature).
    #[cfg(feature = "mongodb")]
    pub fn mongo(&self) -> MongoHelpers<'_> {
        MongoHelpers::new(self)
    }

    /// NATS core pub/sub helpers (`nats` feature).
    #[cfg(feature = "nats")]
    pub fn nats(&self) -> NatsHelpers<'_> {
        NatsHelpers::new(self)
    }

    /// Neo4j Cypher helpers (`neo4j` feature).
    #[cfg(feature = "neo4j")]
    pub fn neo4j(&self) -> Neo4jHelpers<'_> {
        Neo4jHelpers::new(self)
    }

    /// Elasticsearch index/search helpers (`elasticsearch` feature).
    #[cfg(feature = "elasticsearch")]
    pub fn elasticsearch(&self) -> ElasticsearchHelpers<'_> {
        ElasticsearchHelpers::new(self)
    }

    /// OpenSearch index/search helpers (`opensearch` feature).
    #[cfg(feature = "opensearch")]
    pub fn opensearch(&self) -> OpenSearchHelpers<'_> {
        OpenSearchHelpers::new(self)
    }

    /// Qdrant vector helpers (`qdrant` feature).
    #[cfg(feature = "qdrant")]
    pub fn qdrant(&self) -> QdrantHelpers<'_> {
        QdrantHelpers::new(self)
    }

    /// Weaviate schema/object helpers (`weaviate` feature).
    #[cfg(feature = "weaviate")]
    pub fn weaviate(&self) -> WeaviateHelpers<'_> {
        WeaviateHelpers::new(self)
    }

    /// InfluxDB 2.x write/query helpers (`influxdb` feature).
    #[cfg(feature = "influxdb")]
    pub fn influxdb(&self) -> InfluxDbHelpers<'_> {
        InfluxDbHelpers::new(self)
    }

    /// TimescaleDB SQL + hypertable helpers (`timescaledb` feature).
    #[cfg(feature = "timescaledb")]
    pub fn timescaledb(&self) -> TimescaledbHelpers<'_> {
        TimescaledbHelpers::new(self)
    }

    /// Check if the context has any registered resources.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }

    /// Get a mutable reference to the scenario ID.
    pub fn set_scenario_id(&mut self, id: impl Into<String>) {
        self.scenario_id = ScenarioId::new(id);
    }

    /// Get a reference to the scenario kind.
    pub fn scenario_kind(&self) -> ScenarioKind {
        self.scenario_kind
    }

    /// Set the scenario kind for this run.
    pub fn set_scenario_kind(&mut self, kind: ScenarioKind) {
        self.scenario_kind = kind;
    }

    /// Optional fault profile label from `#[shakedown::faulty_test(fault_profile = "...")]`, if set.
    pub fn fault_profile(&self) -> Option<&str> {
        self.fault_profile.as_deref()
    }

    /// Set the fault profile label (used by `#[shakedown::faulty_test(fault_profile = "...")]`).
    pub fn set_fault_profile(&mut self, profile: impl Into<String>) {
        self.fault_profile = Some(profile.into());
    }

    /// Toxiproxy fault control when `fault-injection` is enabled and Toxiproxy was provisioned.
    #[cfg(feature = "fault-injection")]
    pub fn fault_handle(&self) -> Option<&crate::fault_injection::FaultHandle> {
        self.fault_handle.as_ref()
    }

    /// Mutable access for harness code and advanced scenario wiring.
    #[cfg(feature = "fault-injection")]
    pub fn fault_handle_mut(&mut self) -> Option<&mut crate::fault_injection::FaultHandle> {
        self.fault_handle.as_mut()
    }

    #[cfg(feature = "fault-injection")]
    pub(crate) fn set_fault_handle(
        &mut self,
        fault_handle: Option<crate::fault_injection::FaultHandle>,
    ) {
        self.fault_handle = fault_handle;
    }

    #[cfg(feature = "fault-injection")]
    pub(crate) fn clear_fault_handle(&mut self) {
        self.fault_handle = None;
        #[cfg(all(feature = "fault-injection", feature = "kafka"))]
        {
            self.toxiproxy_control_api_base_url = None;
            self.kafka_fault_proxy_pending = None;
            self.kafka_bindings_apply_deferred = false;
        }
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn set_toxiproxy_control_api_base_url(&mut self, url: Option<String>) {
        self.toxiproxy_control_api_base_url = url;
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn toxiproxy_control_api_base_url(&self) -> Option<&str> {
        self.toxiproxy_control_api_base_url.as_deref()
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn set_kafka_fault_proxy_pending(&mut self, pending: Option<(String, String)>) {
        self.kafka_fault_proxy_pending = pending;
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn take_kafka_fault_proxy_pending(&mut self) -> Option<(String, String)> {
        self.kafka_fault_proxy_pending.take()
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn set_kafka_bindings_apply_deferred(&mut self, deferred: bool) {
        self.kafka_bindings_apply_deferred = deferred;
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn kafka_bindings_apply_deferred(&self) -> bool {
        self.kafka_bindings_apply_deferred
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    pub(crate) fn clear_kafka_fault_injection_deferred_state(&mut self) {
        self.kafka_fault_proxy_pending = None;
        self.kafka_bindings_apply_deferred = false;
    }

    /// Snapshot handle for per-container resource stats rings (diagnostics).
    #[cfg(all(
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
        feature = "diagnostics",
        not(feature = "no-diagnostics"),
    ))]
    pub(crate) fn resource_stats_rings(
        &self,
    ) -> Option<&crate::diagnostics::SharedResourceStatsRings> {
        self.stats_rings.as_ref()
    }

    /// Install or clear the stats ring handle (used by [`run_scenario`] and tests).
    #[cfg(all(
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
        feature = "diagnostics",
        not(feature = "no-diagnostics"),
    ))]
    pub(crate) fn set_resource_stats_rings(
        &mut self,
        rings: Option<crate::diagnostics::SharedResourceStatsRings>,
    ) {
        self.stats_rings = rings;
    }

    /// Assert CPU and memory bounds using collected Docker stats for `container_name`.
    #[cfg(all(
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
        feature = "diagnostics",
        not(feature = "no-diagnostics"),
    ))]
    pub fn assert_resource<'a>(
        &'a self,
        container_name: &'a str,
    ) -> crate::diagnostics::ResourceAssertBuilder<'a> {
        crate::diagnostics::ResourceAssertBuilder::new(self, container_name)
    }

    /// Register a built [`crate::service::ServiceSpec`] for the harness to start after infrastructure
    /// and bridges (service container lifecycle).
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
            crate::service::ServiceState::new(service_spec),
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

    /// Invoke `visit` for each running HTTP/gRPC bridge base URL (service container env injection).
    #[cfg(all(feature = "mock-services", feature = "services"))]
    #[allow(dead_code)] // `inject_service_env`; service container lifecycle
    pub(crate) fn for_each_mock_service_url(&self, mut visit: impl FnMut(&str, &str)) {
        for (name, handle) in &self.mock_service_handles {
            visit(name.as_str(), handle.client_url.as_str());
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "fault-injection")]
fn toxiproxy_api_url_from_handles(handles: &[ResourceHandle]) -> Option<String> {
    for handle in handles {
        let metadata = handle.metadata.as_ref()?;
        let engine = metadata.get("engine")?.as_str()?;
        if engine != "toxiproxy" {
            continue;
        }
        if let Some(url) = metadata.get("api_url").and_then(|value| value.as_str()) {
            return Some(url.to_string());
        }
        if !handle.connection_string.is_empty() {
            return Some(handle.connection_string.clone());
        }
    }
    None
}

#[cfg(feature = "fault-injection")]
fn install_fault_handle_if_toxiproxy_provisioned(ctx: &mut Context, handles: &[ResourceHandle]) {
    let Some(api_url) = toxiproxy_api_url_from_handles(handles) else {
        return;
    };
    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    ctx.set_toxiproxy_control_api_base_url(Some(api_url.clone()));
    if ctx.fault_handle().is_none() {
        ctx.set_fault_handle(Some(crate::fault_injection::FaultHandle::new(api_url)));
    }
}

/// Stable Toxiproxy proxy name for the suite Kafka broker (SPECS D-MVP-6).
#[cfg(all(feature = "fault-injection", feature = "kafka"))]
const KAFKA_FAULT_PROXY_NAME: &str = "shakedown_kafka";

#[cfg(all(feature = "fault-injection", feature = "kafka"))]
fn is_kafka_provision_metadata(meta: &serde_json::Value) -> bool {
    meta.get("bootstrap_servers")
        .and_then(|v| v.as_str())
        .is_some()
        && meta.get("topic_prefix").and_then(|v| v.as_str()).is_some()
}

#[cfg(all(feature = "fault-injection", feature = "kafka"))]
fn context_registers_kafka_and_toxiproxy(ctx: &Context) -> bool {
    let mut has_kafka = false;
    let mut has_toxiproxy = false;
    for registered in ctx.resources() {
        match registered {
            RegisteredResource::Builtin(BuiltinResource::Kafka(_)) => has_kafka = true,
            RegisteredResource::Builtin(BuiltinResource::Toxiproxy(_)) => has_toxiproxy = true,
            _ => {}
        }
    }
    has_kafka && has_toxiproxy
}

#[cfg(all(feature = "fault-injection", feature = "kafka"))]
fn update_kafka_handle_bootstrap(handle: &mut ResourceHandle, proxied_bootstrap: &str) {
    handle.connection_string = proxied_bootstrap.to_string();
    if let Some(meta) = handle.metadata.as_mut() {
        if let Some(map) = meta.as_object_mut() {
            map.insert(
                "bootstrap_servers".to_string(),
                serde_json::Value::String(proxied_bootstrap.to_string()),
            );
        }
    }
}

/// When [`ScenarioKind::Faulty`] and both Kafka and Toxiproxy are registered, rewrites Kafka
/// bootstrap metadata to the Toxiproxy listen address (pass-through proxy) before bindings apply.
#[cfg(all(feature = "fault-injection", feature = "kafka"))]
async fn apply_fault_injection_proxy_rewrites(
    ctx: &mut Context,
    handles: &mut [ResourceHandle],
) -> Result<(), InfrastructureError> {
    if let Some(url) = toxiproxy_api_url_from_handles(handles) {
        ctx.set_toxiproxy_control_api_base_url(Some(url));
    }

    let api_url = ctx
        .toxiproxy_control_api_base_url()
        .map(std::string::ToString::to_string)
        .or_else(|| toxiproxy_api_url_from_handles(handles));

    if ctx.scenario_kind() != ScenarioKind::Faulty || !context_registers_kafka_and_toxiproxy(ctx) {
        ctx.clear_kafka_fault_injection_deferred_state();
        return Ok(());
    }

    let Some(api_url) = api_url else {
        for handle in handles.iter_mut() {
            let Some(meta) = handle.metadata.as_ref() else {
                continue;
            };
            if !is_kafka_provision_metadata(meta) {
                continue;
            }
            let topic_prefix = meta
                .get("topic_prefix")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let direct_bootstrap = meta
                .get("bootstrap_servers")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            if let (Some(tp), Some(bs)) = (topic_prefix, direct_bootstrap) {
                ctx.set_kafka_fault_proxy_pending(Some((tp, bs)));
                ctx.set_kafka_bindings_apply_deferred(true);
            }
        }
        return Ok(());
    };

    if ctx.fault_handle().is_none() {
        ctx.set_fault_handle(Some(crate::fault_injection::FaultHandle::new(
            api_url.clone(),
        )));
    }

    if let Some((topic_prefix, direct_bootstrap)) = ctx.take_kafka_fault_proxy_pending() {
        ctx.set_kafka_bindings_apply_deferred(false);
        let fault_handle = ctx.fault_handle_mut().ok_or_else(|| {
            InfrastructureError::FaultInjection(crate::fault_injection::Error::NotAvailable)
        })?;
        if fault_handle
            .proxy_listen_address(KAFKA_FAULT_PROXY_NAME)
            .is_none()
        {
            fault_handle
                .create_proxy(
                    KAFKA_FAULT_PROXY_NAME,
                    "127.0.0.1:0",
                    direct_bootstrap.as_str(),
                )
                .await
                .map_err(InfrastructureError::FaultInjection)?;
        }
        let listen = fault_handle
            .proxy_listen_address(KAFKA_FAULT_PROXY_NAME)
            .ok_or_else(
                || crate::fault_injection::Error::ListenAddressMissingAfterProxyCreate {
                    proxy_name: KAFKA_FAULT_PROXY_NAME.to_string(),
                },
            )
            .map_err(InfrastructureError::FaultInjection)?
            .to_string();
        ctx.set_kafka_bindings(KafkaBindings::new(listen.as_str(), topic_prefix.as_str()));
    }

    for handle in handles.iter_mut() {
        let Some(meta) = handle.metadata.as_ref() else {
            continue;
        };
        if !is_kafka_provision_metadata(meta) {
            continue;
        }
        let direct_bootstrap = meta
            .get("bootstrap_servers")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let Some(direct_bootstrap) = direct_bootstrap else {
            continue;
        };
        let fault_handle = ctx.fault_handle_mut().ok_or_else(|| {
            InfrastructureError::FaultInjection(crate::fault_injection::Error::NotAvailable)
        })?;
        if fault_handle
            .proxy_listen_address(KAFKA_FAULT_PROXY_NAME)
            .is_none()
        {
            fault_handle
                .create_proxy(
                    KAFKA_FAULT_PROXY_NAME,
                    "127.0.0.1:0",
                    direct_bootstrap.as_str(),
                )
                .await
                .map_err(InfrastructureError::FaultInjection)?;
        }
        let listen = fault_handle
            .proxy_listen_address(KAFKA_FAULT_PROXY_NAME)
            .ok_or_else(
                || crate::fault_injection::Error::ListenAddressMissingAfterProxyCreate {
                    proxy_name: KAFKA_FAULT_PROXY_NAME.to_string(),
                },
            )
            .map_err(InfrastructureError::FaultInjection)?
            .to_string();
        update_kafka_handle_bootstrap(handle, listen.as_str());
    }

    Ok(())
}

#[cfg(feature = "postgres")]
/// If any handle metadata carries Postgres engine fields, set [`Context::set_postgres_bindings`].
fn apply_resource_handles_to_postgres_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|v| v.as_str()) else {
            continue;
        };
        if engine != "postgres" {
            continue;
        }
        let host = meta.get("host").and_then(|v| v.as_str());
        let port = meta.get("port").and_then(|v| v.as_u64()).map(|p| p as u16);
        let database = meta.get("database").and_then(|v| v.as_str());
        let username = meta.get("username").and_then(|v| v.as_str());
        let password = meta.get("password").and_then(|v| v.as_str());
        let connection_url = meta.get("connection_url").and_then(|v| v.as_str());
        if let (
            Some(host),
            Some(port),
            Some(database),
            Some(username),
            Some(password),
            Some(connection_url),
        ) = (host, port, database, username, password, connection_url)
        {
            ctx.set_postgres_bindings(PostgresBindings {
                host: host.to_string(),
                port,
                database: database.to_string(),
                username: username.to_string(),
                password: password.to_string(),
                connection_url: connection_url.to_string(),
            });
        }
    }
}

#[cfg(feature = "mariadb")]
/// If any handle metadata carries MariaDB engine fields, set [`Context::set_mariadb_bindings`].
fn apply_resource_handles_to_mariadb_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|v| v.as_str()) else {
            continue;
        };
        if engine != "mariadb" {
            continue;
        }
        let host = meta.get("host").and_then(|v| v.as_str());
        let port = meta.get("port").and_then(|v| v.as_u64()).map(|p| p as u16);
        let database = meta.get("database").and_then(|v| v.as_str());
        let username = meta.get("username").and_then(|v| v.as_str());
        let password = meta.get("password").and_then(|v| v.as_str());
        let connection_url = meta.get("connection_url").and_then(|v| v.as_str());
        if let (
            Some(host),
            Some(port),
            Some(database),
            Some(username),
            Some(password),
            Some(connection_url),
        ) = (host, port, database, username, password, connection_url)
        {
            ctx.set_mariadb_bindings(MariaDBBindings {
                host: host.to_string(),
                port,
                database: database.to_string(),
                username: username.to_string(),
                password: password.to_string(),
                connection_url: connection_url.to_string(),
            });
        }
    }
}

#[cfg(feature = "sqlite")]
/// If any handle metadata carries SQLite engine fields, set [`Context::set_sqlite_bindings`].
fn apply_resource_handles_to_sqlite_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|v| v.as_str()) else {
            continue;
        };
        if engine != "sqlite" {
            continue;
        }
        let Some(database_url) = meta.get("database_url").and_then(|v| v.as_str()) else {
            continue;
        };
        ctx.set_sqlite_bindings(SqliteBindings {
            database_url: database_url.to_string(),
        });
    }
}

#[cfg(feature = "duckdb")]
/// If any handle metadata carries DuckDB engine fields, set [`Context::set_duckdb_bindings`].
fn apply_resource_handles_to_duckdb_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|v| v.as_str()) else {
            continue;
        };
        if engine != "duckdb" {
            continue;
        }
        let Some(database_url) = meta.get("database_url").and_then(|v| v.as_str()) else {
            continue;
        };
        ctx.set_duckdb_bindings(DuckDbBindings {
            database_url: database_url.to_string(),
        });
    }
}

#[cfg(feature = "mysql")]
/// If any handle metadata carries MySQL engine fields, set [`Context::set_mysql_bindings`].
fn apply_resource_handles_to_mysql_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|v| v.as_str()) else {
            continue;
        };
        if engine != "mysql" {
            continue;
        }
        let host = meta.get("host").and_then(|v| v.as_str());
        let port = meta.get("port").and_then(|v| v.as_u64()).map(|p| p as u16);
        let database = meta.get("database").and_then(|v| v.as_str());
        let username = meta.get("username").and_then(|v| v.as_str());
        let password = meta.get("password").and_then(|v| v.as_str());
        let connection_url = meta.get("connection_url").and_then(|v| v.as_str());
        if let (
            Some(host),
            Some(port),
            Some(database),
            Some(username),
            Some(password),
            Some(connection_url),
        ) = (host, port, database, username, password, connection_url)
        {
            ctx.set_mysql_bindings(MysqlBindings {
                host: host.to_string(),
                port,
                database: database.to_string(),
                username: username.to_string(),
                password: password.to_string(),
                connection_url: connection_url.to_string(),
            });
        }
    }
}

/// If any handle metadata carries `bootstrap_servers` and `topic_prefix`, set [`Context::set_kafka_bindings`].
fn apply_resource_handles_to_kafka_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    if ctx.kafka_bindings_apply_deferred() {
        return;
    }
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let (Some(bs), Some(tp)) = (
            meta.get("bootstrap_servers").and_then(|v| v.as_str()),
            meta.get("topic_prefix").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        ctx.set_kafka_bindings(KafkaBindings::new(bs, tp));
    }
}

#[cfg(feature = "mongodb")]
/// If any handle metadata carries MongoDB engine fields, set [`Context::set_mongo_bindings`].
fn apply_resource_handles_to_mongo_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|value| value.as_str()) else {
            continue;
        };
        if engine != "mongodb" {
            continue;
        }
        let host = meta.get("host").and_then(|value| value.as_str());
        let port = meta
            .get("port")
            .and_then(|value| value.as_u64())
            .map(|port| port as u16);
        let database = meta.get("database").and_then(|value| value.as_str());
        let username = meta.get("username").and_then(|value| value.as_str());
        let password = meta.get("password").and_then(|value| value.as_str());
        let connection_url = meta.get("connection_url").and_then(|value| value.as_str());
        if let (
            Some(host),
            Some(port),
            Some(database),
            Some(username),
            Some(password),
            Some(connection_url),
        ) = (host, port, database, username, password, connection_url)
        {
            ctx.set_mongo_bindings(MongoBindings {
                host: host.to_string(),
                port,
                database: database.to_string(),
                username: username.to_string(),
                password: password.to_string(),
                connection_url: connection_url.to_string(),
            });
        }
    }
}

#[cfg(feature = "nats")]
/// If any handle metadata carries NATS engine fields, set [`Context::set_nats_bindings`].
fn apply_resource_handles_to_nats_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|value| value.as_str()) else {
            continue;
        };
        if engine != "nats" {
            continue;
        }
        let host = meta.get("host").and_then(|value| value.as_str());
        let port = meta
            .get("port")
            .and_then(|value| value.as_u64())
            .map(|port| port as u16);
        let connection_url = meta.get("connection_url").and_then(|value| value.as_str());
        let subject_prefix = meta.get("subject_prefix").and_then(|value| value.as_str());
        if let (Some(host), Some(port), Some(connection_url), Some(subject_prefix)) =
            (host, port, connection_url, subject_prefix)
        {
            ctx.set_nats_bindings(NatsBindings {
                host: host.to_string(),
                port,
                connection_url: connection_url.to_string(),
                subject_prefix: subject_prefix.to_string(),
            });
        }
    }
}

#[cfg(feature = "neo4j")]
/// If any handle metadata carries Neo4j engine fields, set [`Context::set_neo4j_bindings`].
fn apply_resource_handles_to_neo4j_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|value| value.as_str()) else {
            continue;
        };
        if engine != "neo4j" {
            continue;
        }
        let host = meta.get("host").and_then(|value| value.as_str());
        let port = meta
            .get("port")
            .and_then(|value| value.as_u64())
            .map(|port| port as u16);
        let http_port = meta
            .get("http_port")
            .and_then(|value| value.as_u64())
            .map(|port| port as u16);
        let database = meta.get("database").and_then(|value| value.as_str());
        let username = meta.get("username").and_then(|value| value.as_str());
        let password = meta.get("password").and_then(|value| value.as_str());
        let bolt_url = meta.get("bolt_url").and_then(|value| value.as_str());
        let http_url = meta.get("http_url").and_then(|value| value.as_str());
        if let (
            Some(host),
            Some(port),
            Some(http_port),
            Some(database),
            Some(username),
            Some(password),
            Some(bolt_url),
            Some(http_url),
        ) = (
            host, port, http_port, database, username, password, bolt_url, http_url,
        ) {
            ctx.set_neo4j_bindings(Neo4jBindings {
                bolt_url: bolt_url.to_string(),
                http_url: http_url.to_string(),
                host: host.to_string(),
                port,
                http_port,
                username: username.to_string(),
                password: password.to_string(),
                database: database.to_string(),
            });
        }
    }
}

#[cfg(feature = "elasticsearch")]
/// If any handle metadata carries Elasticsearch engine fields, set [`Context::set_elasticsearch_bindings`].
fn apply_resource_handles_to_elasticsearch_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|value| value.as_str()) else {
            continue;
        };
        if engine != "elasticsearch" {
            continue;
        }
        let host = meta.get("host").and_then(|value| value.as_str());
        let port = meta
            .get("port")
            .and_then(|value| value.as_u64())
            .map(|port| port as u16);
        let url = meta.get("url").and_then(|value| value.as_str());
        let index_prefix = meta.get("index_prefix").and_then(|value| value.as_str());
        let username = meta
            .get("username")
            .and_then(|value| value.as_str())
            .map(str::to_string);
        let password = meta
            .get("password")
            .and_then(|value| value.as_str())
            .map(str::to_string);
        if let (Some(host), Some(port), Some(url), Some(index_prefix)) =
            (host, port, url, index_prefix)
        {
            ctx.set_elasticsearch_bindings(ElasticsearchBindings {
                url: url.to_string(),
                host: host.to_string(),
                port,
                index_prefix: index_prefix.to_string(),
                username,
                password,
            });
        }
    }
}

#[cfg(feature = "opensearch")]
/// If any handle metadata carries OpenSearch engine fields, set [`Context::set_opensearch_bindings`].
fn apply_resource_handles_to_opensearch_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|value| value.as_str()) else {
            continue;
        };
        if engine != "opensearch" {
            continue;
        }
        let host = meta.get("host").and_then(|value| value.as_str());
        let port = meta
            .get("port")
            .and_then(|value| value.as_u64())
            .map(|port| port as u16);
        let url = meta.get("url").and_then(|value| value.as_str());
        let index_prefix = meta.get("index_prefix").and_then(|value| value.as_str());
        let username = meta
            .get("username")
            .and_then(|value| value.as_str())
            .map(str::to_string);
        let password = meta
            .get("password")
            .and_then(|value| value.as_str())
            .map(str::to_string);
        if let (Some(host), Some(port), Some(url), Some(index_prefix)) =
            (host, port, url, index_prefix)
        {
            ctx.set_opensearch_bindings(OpenSearchBindings {
                url: url.to_string(),
                host: host.to_string(),
                port,
                index_prefix: index_prefix.to_string(),
                username,
                password,
            });
        }
    }
}

#[cfg(feature = "weaviate")]
/// If any handle metadata carries Weaviate engine fields, set [`Context::set_weaviate_bindings`].
fn apply_resource_handles_to_weaviate_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|value| value.as_str()) else {
            continue;
        };
        if engine != "weaviate" {
            continue;
        }
        let host = meta.get("host").and_then(|value| value.as_str());
        let port = meta
            .get("port")
            .and_then(|value| value.as_u64())
            .map(|port| port as u16);
        let rest_url = meta.get("rest_url").and_then(|value| value.as_str());
        let class_prefix = meta.get("class_prefix").and_then(|value| value.as_str());
        if let (Some(host), Some(port), Some(rest_url), Some(class_prefix)) =
            (host, port, rest_url, class_prefix)
        {
            ctx.set_weaviate_bindings(WeaviateBindings {
                rest_url: rest_url.to_string(),
                host: host.to_string(),
                port,
                class_prefix: class_prefix.to_string(),
            });
        }
    }
}

#[cfg(feature = "influxdb")]
/// If any handle metadata carries InfluxDB engine fields, set [`Context::set_influxdb_bindings`].
fn apply_resource_handles_to_influxdb_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|value| value.as_str()) else {
            continue;
        };
        if engine != "influxdb" {
            continue;
        }
        let host = meta.get("host").and_then(|value| value.as_str());
        let port = meta
            .get("port")
            .and_then(|value| value.as_u64())
            .map(|port| port as u16);
        let url = meta.get("url").and_then(|value| value.as_str());
        let token = meta.get("token").and_then(|value| value.as_str());
        let organization = meta.get("organization").and_then(|value| value.as_str());
        let bucket = meta.get("bucket").and_then(|value| value.as_str());
        if let (Some(host), Some(port), Some(url), Some(token), Some(organization), Some(bucket)) =
            (host, port, url, token, organization, bucket)
        {
            ctx.set_influxdb_bindings(InfluxDbBindings {
                url: url.to_string(),
                host: host.to_string(),
                port,
                token: token.to_string(),
                organization: organization.to_string(),
                bucket: bucket.to_string(),
            });
        }
    }
}

#[cfg(feature = "timescaledb")]
/// If any handle metadata carries TimescaleDB engine fields, set [`Context::set_timescaledb_bindings`].
fn apply_resource_handles_to_timescaledb_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|value| value.as_str()) else {
            continue;
        };
        if engine != "timescaledb" {
            continue;
        }
        let host = meta.get("host").and_then(|value| value.as_str());
        let port = meta
            .get("port")
            .and_then(|value| value.as_u64())
            .map(|port| port as u16);
        let database = meta.get("database").and_then(|value| value.as_str());
        let username = meta.get("username").and_then(|value| value.as_str());
        let password = meta.get("password").and_then(|value| value.as_str());
        let connection_url = meta.get("connection_url").and_then(|value| value.as_str());
        if let (
            Some(host),
            Some(port),
            Some(database),
            Some(username),
            Some(password),
            Some(connection_url),
        ) = (host, port, database, username, password, connection_url)
        {
            ctx.set_timescaledb_bindings(PostgresBindings {
                host: host.to_string(),
                port,
                database: database.to_string(),
                username: username.to_string(),
                password: password.to_string(),
                connection_url: connection_url.to_string(),
            });
        }
    }
}

#[cfg(feature = "qdrant")]
/// If any handle metadata carries Qdrant engine fields, set [`Context::set_qdrant_bindings`].
fn apply_resource_handles_to_qdrant_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|value| value.as_str()) else {
            continue;
        };
        if engine != "qdrant" {
            continue;
        }
        let host = meta.get("host").and_then(|value| value.as_str());
        let rest_port = meta
            .get("rest_port")
            .and_then(|value| value.as_u64())
            .map(|port| port as u16);
        let grpc_port = meta
            .get("grpc_port")
            .and_then(|value| value.as_u64())
            .map(|port| port as u16);
        let rest_url = meta.get("rest_url").and_then(|value| value.as_str());
        let grpc_url = meta.get("grpc_url").and_then(|value| value.as_str());
        let collection_prefix = meta
            .get("collection_prefix")
            .and_then(|value| value.as_str());
        if let (
            Some(host),
            Some(rest_port),
            Some(grpc_port),
            Some(rest_url),
            Some(grpc_url),
            Some(collection_prefix),
        ) = (
            host,
            rest_port,
            grpc_port,
            rest_url,
            grpc_url,
            collection_prefix,
        ) {
            ctx.set_qdrant_bindings(QdrantBindings {
                grpc_url: grpc_url.to_string(),
                rest_url: rest_url.to_string(),
                host: host.to_string(),
                grpc_port,
                rest_port,
                collection_prefix: collection_prefix.to_string(),
            });
        }
    }
}

#[cfg(any(feature = "redis", feature = "valkey"))]
/// If any handle metadata carries Redis or Valkey engine fields, set [`Context::set_redis_bindings`].
fn apply_resource_handles_to_redis_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|value| value.as_str()) else {
            continue;
        };
        if engine != "redis" && engine != "valkey" {
            continue;
        }
        let host = meta.get("host").and_then(|value| value.as_str());
        let port = meta
            .get("port")
            .and_then(|value| value.as_u64())
            .map(|port| port as u16);
        let connection_url = meta.get("connection_url").and_then(|value| value.as_str());
        let key_prefix = meta.get("key_prefix").and_then(|value| value.as_str());
        if let (Some(host), Some(port), Some(connection_url), Some(key_prefix)) =
            (host, port, connection_url, key_prefix)
        {
            ctx.set_redis_bindings(RedisBindings {
                host: host.to_string(),
                port,
                connection_url: connection_url.to_string(),
                key_prefix: key_prefix.to_string(),
            });
        }
    }
}

#[cfg(feature = "rabbitmq")]
/// If any handle metadata carries RabbitMQ engine fields, set [`Context::set_rabbit_bindings`].
fn apply_resource_handles_to_rabbit_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for h in handles {
        let Some(meta) = &h.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|v| v.as_str()) else {
            continue;
        };
        if engine != "rabbitmq" {
            continue;
        }
        let amqp_uri = meta.get("amqp_uri").and_then(|v| v.as_str());
        let host = meta.get("host").and_then(|v| v.as_str());
        let port = meta
            .get("port")
            .and_then(|v| v.as_u64())
            .map(|port| port as u16);
        let username = meta.get("username").and_then(|v| v.as_str());
        let password = meta.get("password").and_then(|v| v.as_str());
        let vhost = meta.get("vhost").and_then(|v| v.as_str());
        let resource_prefix = meta.get("resource_prefix").and_then(|v| v.as_str());
        if let (
            Some(amqp_uri),
            Some(host),
            Some(port),
            Some(username),
            Some(password),
            Some(vhost),
            Some(resource_prefix),
        ) = (
            amqp_uri,
            host,
            port,
            username,
            password,
            vhost,
            resource_prefix,
        ) {
            ctx.set_rabbit_bindings(RabbitBindings::new(
                amqp_uri,
                host,
                port,
                username,
                password,
                vhost,
                resource_prefix,
            ));
        }
    }
}

/// Run [`Resource::teardown`](crate::resources::Resource::teardown) on every registered resource, then backend teardown.
async fn teardown_all_resources(ctx: &mut Context) {
    for resource in ctx.resources_mut() {
        let _ = resource.teardown().await;
    }
    let _ = ctx.environment_backend_mut().teardown().await;
    #[cfg(feature = "fault-injection")]
    ctx.clear_fault_handle();
}

/// Runs a scenario through all lifecycle phases.
///
/// This function executes the scenario in order:
/// - Provision resources (async)
/// - Check readiness (async)
/// - Seed data (async)
/// - Run scenario logic
/// - Assert outcomes
/// - Teardown resources (always attempted at the end, async)
///
/// # Errors
///
/// Returns an error if Provision or Readiness phases fail. The error type indicates the failure category:
/// - [`crate::error::ShakedownError`] (infrastructure category) for provisioning/readiness failures
///
/// If Provision or Readiness fails, teardown is attempted before returning the error.
///
/// # Lifecycle ordering
///
/// The function respects the lifecycle order: provision → readiness → seed → run → assert → teardown
/// as documented in ARCHITECTURE.md §§ 3.6–3.9.
///
/// # Event logging
///
/// Events emitted during the scenario run are appended to the provided [`EventLog`].
/// After completion, callers can retrieve all events for debugging or analysis.
///
/// If provision, readiness, or seed fails with an infrastructure error, diagnostics (when enabled)
/// are merged into the log first, then a human-readable failure timeline is written to stderr
/// (see [`crate::timeline::render_failure_timeline`]).
///
/// `scenario_fn` takes only `&mut Context` and must return a [`std::pin::Pin`]-boxed [`std::future::Future`]
/// so the run phase can borrow `Context` across `.await` while the harness keeps using
/// `event_log` after the scenario body completes.
///
/// # Example
///
/// ```ignore
/// use shakedown::{Context, run_scenario, EventLog};
///
/// async fn my_scenario(ctx: &mut Context) {
///     // Scenario implementation
/// }
///
/// #[tokio::test]
/// async fn test_scenario() {
///     let mut ctx = Context::new();
///     let mut event_log = EventLog::new();
///
///     run_scenario(&mut ctx, "test-scenario", |c| Box::pin(my_scenario(c)), &mut event_log)
///         .await
///         .unwrap();
///
///     // Application-level events (`EventLog::entries` for the full log including infrastructure)
///     let _application_events: Vec<_> = event_log.events().collect();
/// }
/// ```
#[cfg(all(
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
    not(feature = "no-diagnostics"),
))]
macro_rules! shakedown_merge_diagnostics {
    ($docker:ident, $stats:ident, $logs:ident, $log:expr) => {
        if let Some(monitor) = $stats.take() {
            monitor.shutdown_and_drain($log).await?;
        }
        if let Some(monitor) = $logs.take() {
            monitor.shutdown_and_drain($log).await?;
        }
        if let Some(monitor) = $docker.take() {
            monitor.shutdown_and_drain($log).await?;
        }
    };
}

#[cfg(all(
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
    not(feature = "no-diagnostics"),
))]
macro_rules! shakedown_docker_phase {
    ($sender:expr, $ctx:expr, $phase:expr) => {
        if let Some(phase_sender) = $sender.as_ref() {
            crate::diagnostics::push_lifecycle_phase(
                phase_sender,
                $phase,
                $ctx.scenario_id().as_str(),
            );
        }
    };
}

fn emit_failure_timeline_on_infra_err(
    event_log: &EventLog,
    scenario_id: &ScenarioId,
    error: &InfrastructureError,
) {
    crate::timeline::emit_failure_timeline_to_stderr(
        event_log,
        scenario_id.as_str(),
        &error.to_string(),
    );
}

#[cfg(feature = "mock-services")]
#[allow(unused_variables)] // `ctx` unused when no resource features are enabled alongside `bridges`
fn mock_service_resources_from_context(ctx: &Context) -> Arc<MockServiceResources> {
    #[allow(unused_mut)]
    // assignments are feature-gated; with `bridges` alone the struct stays default
    let mut resources = MockServiceResources::default();
    #[cfg(feature = "kafka")]
    {
        resources.kafka_bindings = ctx.kafka_bindings().cloned();
    }
    #[cfg(feature = "rabbitmq")]
    {
        resources.rabbit_bindings = ctx.rabbit_bindings().cloned();
    }
    #[cfg(feature = "postgres")]
    {
        resources.postgres_bindings = ctx.postgres_bindings().cloned();
    }
    #[cfg(feature = "timescaledb")]
    {
        resources.timescaledb_bindings = ctx.timescaledb_bindings().cloned();
    }
    #[cfg(feature = "mariadb")]
    {
        resources.mariadb_bindings = ctx.mariadb_bindings().cloned();
    }
    Arc::new(resources)
}

#[cfg(feature = "mock-services")]
async fn reset_bridges_at_run_start(ctx: &mut Context) {
    #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
    {
        let runtimes = std::mem::take(&mut ctx.grpc_mock_service_runtimes);
        for (_name, mut runtime) in runtimes {
            if let Some(sender) = runtime.shutdown_tx.take() {
                let _ = sender.send(());
            }
            let _ = runtime.join.await;
        }
    }
    let runtimes = std::mem::take(&mut ctx.http_mock_service_runtimes);
    for (_name, mut runtime) in runtimes {
        if let Some(sender) = runtime.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = runtime.join.await;
    }
    ctx.mock_service_handles.clear();
    ctx.bridge_servers_phase_complete = false;
}

#[cfg(feature = "mock-services")]
async fn tcp_probe_local_bind_address(bind_address: &str) -> std::io::Result<()> {
    let socket_address: std::net::SocketAddr = bind_address.parse().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid HTTP bridge bind address",
        )
    })?;
    let connect_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::net::TcpStream::connect(socket_address),
    )
    .await;
    match connect_result {
        Ok(Ok(stream)) => {
            drop(stream);
            Ok(())
        }
        Ok(Err(source)) => Err(source),
        Err(_elapsed) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "bridge tcp probe timed out",
        )),
    }
}

#[cfg(feature = "mock-services")]
fn recorded_bridge_call_count(
    handles: &HashMap<String, Arc<MockServiceHandle>>,
    mock_service_name: &str,
) -> usize {
    handles
        .get(mock_service_name)
        .map(|handle| handle.calls().len())
        .unwrap_or(0)
}

#[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
async fn shutdown_grpc_mock_service_runtimes_with_log(ctx: &mut Context, event_log: &mut EventLog) {
    let runtimes = std::mem::take(&mut ctx.grpc_mock_service_runtimes);
    for (name, mut runtime) in runtimes {
        let recorded_count = recorded_bridge_call_count(&ctx.mock_service_handles, name.as_str());
        if let Some(sender) = runtime.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = runtime.join.await;
        event_log.append(Event::info(
            "bridge",
            format!("bridge '{name}' stopped; {recorded_count} calls recorded"),
        ));
    }
}

#[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
async fn shutdown_grpc_mock_service_runtimes_silent(ctx: &mut Context) {
    let runtimes = std::mem::take(&mut ctx.grpc_mock_service_runtimes);
    for (_name, mut runtime) in runtimes {
        if let Some(sender) = runtime.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = runtime.join.await;
    }
}

#[cfg(feature = "mock-services")]
async fn shutdown_http_mock_service_runtimes_with_log(ctx: &mut Context, event_log: &mut EventLog) {
    let runtimes = std::mem::take(&mut ctx.http_mock_service_runtimes);
    for (name, mut runtime) in runtimes {
        let recorded_count = recorded_bridge_call_count(&ctx.mock_service_handles, name.as_str());
        if let Some(sender) = runtime.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = runtime.join.await;
        event_log.append(Event::info(
            "bridge",
            format!("bridge '{name}' stopped; {recorded_count} calls recorded"),
        ));
    }
}

#[cfg(feature = "mock-services")]
async fn shutdown_http_mock_service_runtimes_silent(ctx: &mut Context) {
    let runtimes = std::mem::take(&mut ctx.http_mock_service_runtimes);
    for (_name, mut runtime) in runtimes {
        if let Some(sender) = runtime.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = runtime.join.await;
    }
}

#[cfg(feature = "mock-services")]
async fn start_and_readiness_bridges(
    ctx: &mut Context,
    event_log: &mut EventLog,
) -> Result<(), ShakedownError> {
    let pending_http = std::mem::take(&mut ctx.pending_http_mock_services);
    #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
    let pending_grpc = std::mem::take(&mut ctx.pending_grpc_mock_services);

    #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
    let nothing_to_start = pending_http.is_empty() && pending_grpc.is_empty();
    #[cfg(not(all(feature = "mock-services", feature = "grpc-mock-services")))]
    let nothing_to_start = pending_http.is_empty();

    if nothing_to_start {
        ctx.bridge_servers_phase_complete = true;
        return Ok(());
    }

    let resources = mock_service_resources_from_context(ctx);
    let scenario_id_string = ctx.scenario_id().to_string();

    let mut started_http_runtimes: Vec<(String, HttpMockServiceServer)> = Vec::new();
    for (mock_service_name, bridge_impl) in pending_http {
        match spawn_http_mock_service_server(
            mock_service_name.clone(),
            bridge_impl,
            scenario_id_string.clone(),
            Arc::clone(&resources),
            None,
        )
        .await
        {
            Ok((handle, runtime)) => {
                let bind_display = handle.bind_address.clone();
                ctx.mock_service_handles
                    .insert(mock_service_name.clone(), handle);
                started_http_runtimes.push((mock_service_name.clone(), runtime));
                event_log.append(Event::info(
                    "bridge",
                    format!("bridge '{mock_service_name}' started on {bind_display}"),
                ));
            }
            Err(error) => {
                for (_rollback_name, mut runtime) in started_http_runtimes {
                    if let Some(sender) = runtime.shutdown_tx.take() {
                        let _ = sender.send(());
                    }
                    let _ = runtime.join.await;
                }
                ctx.mock_service_handles.clear();
                return Err(InfrastructureError::Provision {
                    resource: format!("bridge:{mock_service_name}"),
                    source: Box::new(error),
                }
                .into());
            }
        }
    }
    ctx.http_mock_service_runtimes = started_http_runtimes;

    #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
    {
        let mut started_grpc_runtimes: Vec<(String, GrpcMockServiceRuntime)> = Vec::new();
        for (mock_service_name, bridge_impl) in pending_grpc {
            match spawn_grpc_mock_service_server(
                mock_service_name.clone(),
                bridge_impl,
                scenario_id_string.clone(),
                Arc::clone(&resources),
                None,
            )
            .await
            {
                Ok((handle, runtime)) => {
                    let bind_display = handle.bind_address.clone();
                    ctx.mock_service_handles
                        .insert(mock_service_name.clone(), handle);
                    started_grpc_runtimes.push((mock_service_name.clone(), runtime));
                    event_log.append(Event::info(
                        "bridge",
                        format!("bridge '{mock_service_name}' started on {bind_display}"),
                    ));
                }
                Err(error) => {
                    shutdown_http_mock_service_runtimes_silent(ctx).await;
                    for (_rollback_name, mut runtime) in started_grpc_runtimes {
                        if let Some(sender) = runtime.shutdown_tx.take() {
                            let _ = sender.send(());
                        }
                        let _ = runtime.join.await;
                    }
                    ctx.mock_service_handles.clear();
                    return Err(InfrastructureError::Provision {
                        resource: format!("bridge:{mock_service_name}"),
                        source: Box::new(error),
                    }
                    .into());
                }
            }
        }
        ctx.grpc_mock_service_runtimes = started_grpc_runtimes;
    }

    let readiness_targets: Vec<(String, String)> = ctx
        .mock_service_handles
        .iter()
        .map(|(mock_service_name, handle)| (mock_service_name.clone(), handle.bind_address.clone()))
        .collect();
    for (mock_service_name, bind_address) in readiness_targets {
        if let Err(source) = tcp_probe_local_bind_address(bind_address.as_str()).await {
            shutdown_http_mock_service_runtimes_silent(ctx).await;
            #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
            shutdown_grpc_mock_service_runtimes_silent(ctx).await;
            ctx.mock_service_handles.clear();
            return Err(InfrastructureError::Readiness {
                resource: format!("bridge:{mock_service_name}"),
                source: Box::new(source),
            }
            .into());
        }
    }
    ctx.bridge_servers_phase_complete = true;
    Ok(())
}

pub async fn run_scenario<F>(
    ctx: &mut Context,
    scenario_id: impl Into<String>,
    scenario_fn: F,
    event_log: &mut EventLog,
) -> Result<(), ShakedownError>
where
    F: for<'s> FnOnce(
        &'s mut Context,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 's>>,
{
    // Update scenario ID
    ctx.set_scenario_id(scenario_id);

    #[cfg(feature = "mock-services")]
    reset_bridges_at_run_start(ctx).await;

    #[cfg(all(
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
        feature = "diagnostics",
        not(feature = "no-diagnostics"),
    ))]
    ctx.set_resource_stats_rings(None);

    #[cfg(all(
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
        not(feature = "no-diagnostics"),
    ))]
    let (
        mut docker_events_monitor,
        mut resource_stats_monitor,
        mut container_logs_monitor,
        docker_phase_sender,
    ) = {
        let mut docker_monitor_slot = None;
        let mut stats_monitor_slot = None;
        let mut logs_monitor_slot = None;
        let mut phase_sender_slot = None;
        if let Some(diagnostics_handle) = crate::diagnostics::suite_diagnostics_handle().await {
            let (phase_tx, phase_rx) =
                tokio::sync::watch::channel((Phase::Provision, ctx.scenario_id().to_string()));
            let docker = diagnostics_handle.docker().clone();
            docker_monitor_slot = Some(crate::diagnostics::DockerEventsMonitor::spawn(
                docker.clone(),
                phase_rx.clone(),
            ));
            let (stats_monitor, rings_handle) = crate::diagnostics::ResourceStatsMonitor::spawn(
                docker.clone(),
                phase_rx.clone(),
                crate::diagnostics::StatsConfig::from_env(),
            );
            stats_monitor_slot = Some(stats_monitor);
            logs_monitor_slot = Some(crate::diagnostics::ContainerLogsMonitor::spawn(
                docker, phase_rx,
            ));
            ctx.set_resource_stats_rings(Some(rings_handle));
            phase_sender_slot = Some(phase_tx);
        }
        (
            docker_monitor_slot,
            stats_monitor_slot,
            logs_monitor_slot,
            phase_sender_slot,
        )
    };

    // Provision phase (async)
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting provision phase");

    let scenario_id = ctx.scenario_id().clone();
    let n = ctx.resources().len();
    for i in 0..n {
        let result = {
            let resource = &mut ctx.resources_mut()[i];
            resource.provision(&scenario_id).await
        };
        match result {
            // `mut` only needed when `apply_fault_injection_proxy_rewrites` is compiled.
            #[cfg_attr(
                not(all(feature = "fault-injection", feature = "kafka")),
                allow(unused_mut)
            )]
            Ok(mut handles) => {
                #[cfg(all(feature = "fault-injection", feature = "kafka"))]
                if let Err(e) = apply_fault_injection_proxy_rewrites(ctx, &mut handles).await {
                    event_log.append_raw(
                        EventLevel::Error,
                        "lifecycle",
                        format!("Provision failed (fault injection rewrite): {e}"),
                    );
                    teardown_all_resources(ctx).await;
                    #[cfg(all(
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
                        not(feature = "no-diagnostics"),
                    ))]
                    shakedown_merge_diagnostics!(
                        docker_events_monitor,
                        resource_stats_monitor,
                        container_logs_monitor,
                        event_log
                    );
                    emit_failure_timeline_on_infra_err(event_log, &scenario_id, &e);
                    return Err(e.into());
                }
                apply_resource_handles_to_kafka_bindings(ctx, &handles);
                #[cfg(feature = "rabbitmq")]
                apply_resource_handles_to_rabbit_bindings(ctx, &handles);
                #[cfg(feature = "postgres")]
                apply_resource_handles_to_postgres_bindings(ctx, &handles);
                #[cfg(feature = "mariadb")]
                apply_resource_handles_to_mariadb_bindings(ctx, &handles);
                #[cfg(feature = "mysql")]
                apply_resource_handles_to_mysql_bindings(ctx, &handles);
                #[cfg(feature = "sqlite")]
                apply_resource_handles_to_sqlite_bindings(ctx, &handles);
                #[cfg(feature = "duckdb")]
                apply_resource_handles_to_duckdb_bindings(ctx, &handles);
                #[cfg(any(feature = "redis", feature = "valkey"))]
                apply_resource_handles_to_redis_bindings(ctx, &handles);
                #[cfg(feature = "mongodb")]
                apply_resource_handles_to_mongo_bindings(ctx, &handles);
                #[cfg(feature = "nats")]
                apply_resource_handles_to_nats_bindings(ctx, &handles);
                #[cfg(feature = "neo4j")]
                apply_resource_handles_to_neo4j_bindings(ctx, &handles);
                #[cfg(feature = "elasticsearch")]
                apply_resource_handles_to_elasticsearch_bindings(ctx, &handles);
                #[cfg(feature = "opensearch")]
                apply_resource_handles_to_opensearch_bindings(ctx, &handles);
                #[cfg(feature = "qdrant")]
                apply_resource_handles_to_qdrant_bindings(ctx, &handles);
                #[cfg(feature = "weaviate")]
                apply_resource_handles_to_weaviate_bindings(ctx, &handles);
                #[cfg(feature = "influxdb")]
                apply_resource_handles_to_influxdb_bindings(ctx, &handles);
                #[cfg(feature = "timescaledb")]
                apply_resource_handles_to_timescaledb_bindings(ctx, &handles);
                #[cfg(feature = "fault-injection")]
                install_fault_handle_if_toxiproxy_provisioned(ctx, &handles);
                event_log.append_raw(EventLevel::Debug, "lifecycle", "Provisioned resource(s)");
            }
            Err(e) => {
                event_log.append_raw(
                    EventLevel::Error,
                    "lifecycle",
                    format!("Provision failed: {e}"),
                );
                teardown_all_resources(ctx).await;
                #[cfg(all(
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
                    not(feature = "no-diagnostics"),
                ))]
                shakedown_merge_diagnostics!(
                    docker_events_monitor,
                    resource_stats_monitor,
                    container_logs_monitor,
                    event_log
                );
                emit_failure_timeline_on_infra_err(event_log, &scenario_id, &e);
                return Err(e.into());
            }
        }
    }

    #[cfg(all(feature = "fault-injection", feature = "kafka"))]
    if ctx.kafka_bindings_apply_deferred() {
        let error = InfrastructureError::FaultInjection(
            crate::fault_injection::Error::KafkaFaultProxyIncomplete,
        );
        event_log.append_raw(
            EventLevel::Error,
            "lifecycle",
            format!("Provision incomplete: {error}"),
        );
        teardown_all_resources(ctx).await;
        #[cfg(all(
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
            not(feature = "no-diagnostics"),
        ))]
        shakedown_merge_diagnostics!(
            docker_events_monitor,
            resource_stats_monitor,
            container_logs_monitor,
            event_log
        );
        emit_failure_timeline_on_infra_err(event_log, &scenario_id, &error);
        return Err(error.into());
    }

    // Readiness phase (async)
    #[cfg(all(
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
        not(feature = "no-diagnostics"),
    ))]
    shakedown_docker_phase!(docker_phase_sender, ctx, Phase::Readiness);
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting readiness phase");
    for resource in ctx.resources() {
        if let Err(e) = resource.check_ready().await {
            event_log.append_raw(
                EventLevel::Error,
                "lifecycle",
                format!("Readiness failed: {e}"),
            );
            teardown_all_resources(ctx).await;
            #[cfg(all(
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
                not(feature = "no-diagnostics"),
            ))]
            shakedown_merge_diagnostics!(
                docker_events_monitor,
                resource_stats_monitor,
                container_logs_monitor,
                event_log
            );
            emit_failure_timeline_on_infra_err(event_log, &scenario_id, &e);
            return Err(e.into());
        }
    }

    // Seed phase (async)
    #[cfg(all(
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
        not(feature = "no-diagnostics"),
    ))]
    shakedown_docker_phase!(docker_phase_sender, ctx, Phase::Seed);
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting seed phase");
    for resource in ctx.resources_mut() {
        if let Err(e) = resource.apply_default_seed().await {
            event_log.append_raw(EventLevel::Error, "lifecycle", format!("Seed failed: {e}"));
            teardown_all_resources(ctx).await;
            #[cfg(all(
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
                not(feature = "no-diagnostics"),
            ))]
            shakedown_merge_diagnostics!(
                docker_events_monitor,
                resource_stats_monitor,
                container_logs_monitor,
                event_log
            );
            emit_failure_timeline_on_infra_err(event_log, &scenario_id, &e);
            return Err(e.into());
        }
    }

    #[cfg(feature = "mock-services")]
    if let Err(error) = start_and_readiness_bridges(ctx, event_log).await {
        let infrastructure_error = match &error {
            ShakedownError::Infrastructure(ref infrastructure) => Some(infrastructure),
            _ => None,
        };
        event_log.append_raw(
            EventLevel::Error,
            "lifecycle",
            format!("MockService phase failed: {error}"),
        );
        teardown_all_resources(ctx).await;
        #[cfg(all(
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
            not(feature = "no-diagnostics"),
        ))]
        shakedown_merge_diagnostics!(
            docker_events_monitor,
            resource_stats_monitor,
            container_logs_monitor,
            event_log
        );
        if let Some(infrastructure) = infrastructure_error {
            emit_failure_timeline_on_infra_err(event_log, &scenario_id, infrastructure);
        }
        return Err(error);
    }

    #[cfg(feature = "services")]
    if let Err(error) =
        crate::service::lifecycle::start_and_readiness_services(ctx, event_log).await
    {
        event_log.append_raw(
            EventLevel::Error,
            "lifecycle",
            format!("Service container phase failed: {error}"),
        );
        teardown_all_resources(ctx).await;
        #[cfg(all(
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
            not(feature = "no-diagnostics"),
        ))]
        shakedown_merge_diagnostics!(
            docker_events_monitor,
            resource_stats_monitor,
            container_logs_monitor,
            event_log
        );
        emit_failure_timeline_on_infra_err(event_log, &scenario_id, &error);
        return Err(error.into());
    }

    // Run phase
    #[cfg(all(
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
        not(feature = "no-diagnostics"),
    ))]
    shakedown_docker_phase!(docker_phase_sender, ctx, Phase::Run);
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting run phase");
    // Fault-injection timeline: `FaultHandle` sits on `Context` and scenario code calls it from
    // async paths (add/remove toxic, reset proxy). The harness owns `EventLog` separately; we need
    // those fault actions to show up in the same log as lifecycle and infra lines. You cannot hand
    // `FaultHandle` a `&mut EventLog` for the whole run: that would alias `event_log` with `ctx`
    // across the scenario body, and tying lifetimes through `Context` is awkward. Alternatives like
    // a raw pointer to `EventLog` would work only under `unsafe` with a brittle "call order"
    // contract.
    //
    // A bounded `SyncSender`/`Receiver` pair fixes this: the handle only holds the sender and
    // enqueues owned `Event` values with `try_send` (no shared mutable state with the log). The
    // harness keeps the receiver and drains after `scenario_fn` returns, appending into the
    // caller-owned `EventLog` in one place—same merge point as other diagnostics. Queue depth is
    // [`crate::fault_injection::FAULT_EVENT_CHANNEL_CAPACITY`]; if it fills, `try_send` fails and
    // that event is skipped (explicit back-pressure; same idea as a fixed-size buffer).
    #[cfg(feature = "fault-injection")]
    let fault_event_receiver = {
        let (sender, receiver) =
            std::sync::mpsc::sync_channel(crate::fault_injection::FAULT_EVENT_CHANNEL_CAPACITY);
        if let Some(handle) = ctx.fault_handle_mut() {
            handle.set_event_sender(sender);
        }
        receiver
    };
    // Execute the scenario function
    let _ = scenario_fn(ctx).await;

    // Drain fault events into the caller-owned log after the scenario body and before assert/teardown,
    // so timelines and tests that read `event_log` after `run_scenario` see a complete run-phase slice.
    #[cfg(feature = "fault-injection")]
    for event in fault_event_receiver.try_iter() {
        event_log.append(event);
    }

    // Assert phase (in a real implementation, this would run assertions)
    #[cfg(all(
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
        not(feature = "no-diagnostics"),
    ))]
    shakedown_docker_phase!(docker_phase_sender, ctx, Phase::Assert);
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting assert phase");

    // Teardown phase
    #[cfg(all(
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
        not(feature = "no-diagnostics"),
    ))]
    shakedown_docker_phase!(docker_phase_sender, ctx, Phase::Teardown);
    event_log.append_raw(EventLevel::Info, "lifecycle", "Starting teardown phase");
    #[cfg(feature = "services")]
    if let Err(error) = crate::service::lifecycle::teardown_service_containers(ctx, event_log).await
    {
        event_log.append_raw(
            EventLevel::Error,
            "lifecycle",
            format!("Service container teardown failed: {error}"),
        );
    }
    #[cfg(all(feature = "mock-services", feature = "grpc-mock-services"))]
    shutdown_grpc_mock_service_runtimes_with_log(ctx, event_log).await;
    #[cfg(feature = "mock-services")]
    shutdown_http_mock_service_runtimes_with_log(ctx, event_log).await;
    teardown_all_resources(ctx).await;

    #[cfg(all(
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
        not(feature = "no-diagnostics"),
    ))]
    shakedown_merge_diagnostics!(
        docker_events_monitor,
        resource_stats_monitor,
        container_logs_monitor,
        event_log
    );

    #[cfg(all(
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
        feature = "diagnostics",
        not(feature = "no-diagnostics"),
    ))]
    ctx.set_resource_stats_rings(None);

    Ok(())
}

/// Builds a [`Context`] with [`crate::resources::BuiltinResource::Fake`] already registered.
///
/// Convenience for tests that call [`run_scenario`] directly without `#[shakedown::test(resources = [Fake])]`.
pub fn new_context_with_fake_resource() -> Context {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    ctx
}

#[cfg(test)]
mod tests;
