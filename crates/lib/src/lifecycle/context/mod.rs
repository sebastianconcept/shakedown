//! Scenario [`Context`]: registered resources, bindings, and per-run metadata.
//!
//! This module is useful when you add a field to the per-run harness state or a constructor for
//! [`Context`].
//! This type is the mutable state for one [`super::run_scenario`] invocation.
//! This type lets the scenario body and the harness share bindings without a global singleton.

use crate::error::ScenarioId;
#[cfg(all(feature = "mock-services", feature = "grpc"))]
use crate::mock_service::grpc::server::GrpcMockServiceServer;
#[cfg(all(feature = "mock-services", feature = "grpc"))]
use crate::mock_service::grpc::GrpcMockServiceTrait;
#[cfg(feature = "mock-services")]
use crate::mock_service::handle::MockServiceHandle;
#[cfg(feature = "mock-services")]
use crate::mock_service::http::server::HttpMockServiceServer;
#[cfg(feature = "mock-services")]
use crate::mock_service::http::HttpMockServiceTrait;
#[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
use crate::mock_service::tcp::{TcpMockServiceRegistration, TcpMockServiceServer};
#[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
use crate::mock_service::udp::{UdpMockServiceRegistration, UdpMockServiceServer};
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
#[cfg(feature = "redis-sentinel")]
use crate::resources::RedisSentinelBindings;
#[cfg(feature = "sqlite")]
use crate::resources::SqliteBindings;
#[cfg(feature = "weaviate")]
use crate::resources::WeaviateBindings;
use crate::resources::{KafkaBindings, RegisteredResource};
use std::collections::HashMap;
#[cfg(feature = "mock-services")]
use std::sync::Arc;

use super::environment::{EnvironmentBackend, NoopEnvironmentBackend, ScenarioKind};

mod bindings;
mod cluster;
mod fault;
mod helpers;
mod mock;
mod register;
mod services;

#[cfg(feature = "mock-services")]
pub(super) mod mock_servers;

/// Mutable scenario state for one run: resources, backend, ids, and optional messaging/DB bindings.
///
/// Why: the harness and scenario body share one place for registered infrastructure and
/// per-run metadata so [`run_scenario`] can drive provision → teardown without global singletons.
///
/// How: constructed with [`Context::new`] or by `#[shakedown::test]`; the scenario function receives
/// `&mut Context` to register extras, configure builtins via `Context::*_resource_mut()` before provision, or read [`Context::scenario_id`], [`Context::kafka_bindings`],
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
    kafka_bindings: HashMap<String, KafkaBindings>,
    /// RabbitMQ bindings when a Rabbit resource populated them (`rabbitmq` feature).
    #[cfg(feature = "rabbitmq")]
    rabbit_bindings: HashMap<String, RabbitBindings>,
    /// Postgres bindings when a Postgres resource populated them (`postgres` feature).
    #[cfg(feature = "postgres")]
    postgres_bindings: HashMap<String, PostgresBindings>,
    /// MariaDB bindings when a MariaDB resource populated them (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    mariadb_bindings: HashMap<String, MariaDBBindings>,
    /// Oracle MySQL bindings when a MySQL resource populated them (`mysql` feature).
    #[cfg(feature = "mysql")]
    mysql_bindings: HashMap<String, MysqlBindings>,
    /// SQLite bindings when a SQLite resource populated them (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    sqlite_bindings: HashMap<String, SqliteBindings>,
    /// DuckDB bindings when a DuckDB resource populated them (`duckdb` feature).
    #[cfg(feature = "duckdb")]
    duckdb_bindings: HashMap<String, DuckDbBindings>,
    /// Redis / Valkey bindings when a matching resource populated them (`redis` / `valkey` features).
    #[cfg(any(feature = "redis", feature = "valkey"))]
    redis_bindings: HashMap<String, RedisBindings>,
    /// Redis Sentinel discovery + primary URL when [`crate::BuiltinResource::RedisSentinel`] provisioned (`redis-sentinel`).
    #[cfg(feature = "redis-sentinel")]
    redis_sentinel_bindings: HashMap<String, RedisSentinelBindings>,
    /// MongoDB bindings when a Mongo resource populated them (`mongodb` feature).
    #[cfg(feature = "mongodb")]
    mongo_bindings: HashMap<String, MongoBindings>,
    /// NATS bindings when a NATS resource populated them (`nats` feature).
    #[cfg(feature = "nats")]
    nats_bindings: HashMap<String, NatsBindings>,
    /// Neo4j bindings when a Neo4j resource populated them (`neo4j` feature).
    #[cfg(feature = "neo4j")]
    neo4j_bindings: HashMap<String, Neo4jBindings>,
    /// Elasticsearch bindings when an Elasticsearch resource populated them (`elasticsearch` feature).
    #[cfg(feature = "elasticsearch")]
    elasticsearch_bindings: HashMap<String, ElasticsearchBindings>,
    /// OpenSearch bindings when an OpenSearch resource populated them (`opensearch` feature).
    #[cfg(feature = "opensearch")]
    opensearch_bindings: HashMap<String, OpenSearchBindings>,
    /// Qdrant bindings when a Qdrant resource populated them (`qdrant` feature).
    #[cfg(feature = "qdrant")]
    qdrant_bindings: HashMap<String, QdrantBindings>,
    /// Weaviate bindings when a Weaviate resource populated them (`weaviate` feature).
    #[cfg(feature = "weaviate")]
    weaviate_bindings: HashMap<String, WeaviateBindings>,
    /// InfluxDB bindings when an InfluxDB resource populated them (`influxdb` feature).
    #[cfg(feature = "influxdb")]
    influxdb_bindings: HashMap<String, InfluxDbBindings>,
    /// TimescaleDB bindings when a TimescaleDB resource populated them (`timescaledb` feature).
    #[cfg(feature = "timescaledb")]
    timescaledb_bindings: HashMap<String, PostgresBindings>,
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
    /// HTTP mock services registered before [`run_scenario`] starts servers (after seed).
    #[cfg(feature = "mock-services")]
    pub(crate) pending_http_mock_services: Vec<(String, Arc<dyn HttpMockServiceTrait>)>,
    /// Running HTTP mock service handles (populated when servers start).
    #[cfg(feature = "mock-services")]
    pub(crate) mock_service_handles: HashMap<String, Arc<MockServiceHandle>>,
    /// HTTP mock service server tasks: shutdown channel + join handle (paired with names for timeline messages).
    #[cfg(feature = "mock-services")]
    pub(crate) http_mock_service_servers: Vec<(String, HttpMockServiceServer)>,
    /// TCP mock services registered before [`run_scenario`] starts listeners (after seed).
    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    pub(crate) pending_tcp_mock_services: Vec<(String, TcpMockServiceRegistration)>,
    /// TCP mock accept loops (shutdown channel + join handle).
    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    pub(crate) tcp_mock_service_servers: Vec<(String, TcpMockServiceServer)>,
    /// UDP mock services registered before [`run_scenario`] starts sockets (after seed).
    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    pub(crate) pending_udp_mock_services: Vec<(String, UdpMockServiceRegistration)>,
    /// UDP mock recv loops (shutdown channel + join handle).
    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    pub(crate) udp_mock_service_servers: Vec<(String, UdpMockServiceServer)>,
    /// Local address HTTP and gRPC mock listeners bind to; port is always ephemeral (`0`). Defaults to loopback.
    #[cfg(feature = "mock-services")]
    pub(crate) mock_service_bind_ip: std::net::IpAddr,
    /// gRPC mock services registered before [`run_scenario`] starts tonic servers (after seed).
    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    pub(crate) pending_grpc_mock_services: Vec<(String, Arc<dyn GrpcMockServiceTrait>)>,
    /// Running gRPC mock service servers (tonic transport).
    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    pub(crate) grpc_mock_service_servers: Vec<(String, GrpcMockServiceServer)>,
    /// True once HTTP and gRPC mock service servers have been started for this [`run_scenario`] invocation.
    #[cfg(feature = "mock-services")]
    pub(crate) mock_service_servers_phase_complete: bool,
    /// Registered [`crate::service::ServiceSpec`] entries (service configured); [`ServiceHandle`] set when the service is running and healthy.
    #[cfg(feature = "services")]
    pub(crate) services: HashMap<String, crate::service::ServiceReference>,
    /// Cluster topology handles keyed by resource marker type ([`Context::register_cluster_handle`]).
    #[cfg(feature = "cluster")]
    cluster_handles: HashMap<std::any::TypeId, crate::cluster::ClusterHandle>,
    /// Shared MongoDB replica set cluster control ([`crate::cluster::mongo_replica_set::MongoClusterOps`]).
    #[cfg(feature = "mongodb-cluster")]
    pub(crate) mongo_replica_set_cluster: Option<
        std::sync::Arc<
            crate::cluster::mongo_replica_set::cluster_control::MongoReplicaSetClusterControl,
        >,
    >,
    /// Shared KRaft Kafka cluster control ([`crate::cluster::kafka_cluster::KafkaClusterOps`]).
    #[cfg(feature = "kafka-cluster")]
    pub(crate) kafka_cluster_control: Option<
        std::sync::Arc<crate::cluster::kafka_cluster::cluster_control::KafkaClusterClusterControl>,
    >,
    /// Shared Redis Sentinel cluster control ([`crate::cluster::redis_sentinel::RedisSentinelOps`]).
    #[cfg(feature = "redis-sentinel")]
    pub(crate) redis_sentinel_cluster: Option<
        std::sync::Arc<
            crate::cluster::redis_sentinel::cluster_control::RedisSentinelClusterControl,
        >,
    >,
    /// Per-scenario PostgreSQL primary + replica control ([`crate::cluster::postgres_replica_set::PostgresReplicaSetOps`]).
    #[cfg(feature = "postgres-cluster")]
    pub(crate) postgres_replica_set_cluster: Option<
        std::sync::Arc<
            crate::cluster::postgres_replica_set::cluster_control::PostgresReplicaSetClusterControl,
        >,
    >,
    /// Shared RabbitMQ Erlang cluster control ([`crate::cluster::rabbit_cluster::RabbitClusterOps`]).
    #[cfg(feature = "rabbitmq-cluster")]
    pub(crate) rabbit_cluster_control: Option<
        std::sync::Arc<
            crate::cluster::rabbit_cluster::cluster_control::RabbitClusterClusterControl,
        >,
    >,
    /// Merge point for [`crate::load::LoadHandle`] timeline rows; set for the run phase only.
    #[cfg(feature = "load")]
    load_event_sender: Option<std::sync::mpsc::Sender<crate::event::Event>>,
    /// Sender for application [`crate::event::Event`] rows during the run phase (e.g. [`crate::assert_eventually`] poll attempts).
    scenario_application_event_sender: Option<std::sync::mpsc::Sender<crate::event::Event>>,
}

impl Context {
    fn empty(scenario_id: ScenarioId, environment_backend: Box<dyn EnvironmentBackend>) -> Self {
        Self {
            resources: Vec::new(),
            environment_backend,
            scenario_id,
            kafka_bindings: HashMap::new(),
            #[cfg(feature = "rabbitmq")]
            rabbit_bindings: HashMap::new(),
            #[cfg(feature = "postgres")]
            postgres_bindings: HashMap::new(),
            #[cfg(feature = "mariadb")]
            mariadb_bindings: HashMap::new(),
            #[cfg(feature = "mysql")]
            mysql_bindings: HashMap::new(),
            #[cfg(feature = "sqlite")]
            sqlite_bindings: HashMap::new(),
            #[cfg(feature = "duckdb")]
            duckdb_bindings: HashMap::new(),
            #[cfg(any(feature = "redis", feature = "valkey"))]
            redis_bindings: HashMap::new(),
            #[cfg(feature = "redis-sentinel")]
            redis_sentinel_bindings: HashMap::new(),
            #[cfg(feature = "mongodb")]
            mongo_bindings: HashMap::new(),
            #[cfg(feature = "nats")]
            nats_bindings: HashMap::new(),
            #[cfg(feature = "neo4j")]
            neo4j_bindings: HashMap::new(),
            #[cfg(feature = "elasticsearch")]
            elasticsearch_bindings: HashMap::new(),
            #[cfg(feature = "opensearch")]
            opensearch_bindings: HashMap::new(),
            #[cfg(feature = "qdrant")]
            qdrant_bindings: HashMap::new(),
            #[cfg(feature = "weaviate")]
            weaviate_bindings: HashMap::new(),
            #[cfg(feature = "influxdb")]
            influxdb_bindings: HashMap::new(),
            #[cfg(feature = "timescaledb")]
            timescaledb_bindings: HashMap::new(),
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
            http_mock_service_servers: Vec::new(),
            #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
            pending_tcp_mock_services: Vec::new(),
            #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
            tcp_mock_service_servers: Vec::new(),
            #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
            pending_udp_mock_services: Vec::new(),
            #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
            udp_mock_service_servers: Vec::new(),
            #[cfg(all(feature = "mock-services", feature = "grpc"))]
            pending_grpc_mock_services: Vec::new(),
            #[cfg(all(feature = "mock-services", feature = "grpc"))]
            grpc_mock_service_servers: Vec::new(),
            #[cfg(feature = "mock-services")]
            mock_service_servers_phase_complete: false,
            #[cfg(feature = "mock-services")]
            mock_service_bind_ip: std::net::Ipv4Addr::LOCALHOST.into(),
            #[cfg(feature = "services")]
            services: HashMap::new(),
            #[cfg(feature = "cluster")]
            cluster_handles: HashMap::new(),
            #[cfg(feature = "mongodb-cluster")]
            mongo_replica_set_cluster: None,
            #[cfg(feature = "kafka-cluster")]
            kafka_cluster_control: None,
            #[cfg(feature = "redis-sentinel")]
            redis_sentinel_cluster: None,
            #[cfg(feature = "postgres-cluster")]
            postgres_replica_set_cluster: None,
            #[cfg(feature = "rabbitmq-cluster")]
            rabbit_cluster_control: None,
            #[cfg(feature = "load")]
            load_event_sender: None,
            scenario_application_event_sender: None,
        }
    }

    /// Create a new empty context with the default noop environment backend.
    pub fn new() -> Self {
        Self::empty(ScenarioId::empty(), Box::new(NoopEnvironmentBackend))
    }

    /// Create a new context with a custom environment backend.
    pub fn with_backend(backend: impl EnvironmentBackend + 'static) -> Self {
        Self::empty(ScenarioId::empty(), Box::new(backend))
    }

    /// Create a new context with a specific scenario ID.
    pub fn with_scenario_id(scenario_id: impl Into<String>) -> Self {
        Self::empty(
            ScenarioId::new(scenario_id),
            Box::new(NoopEnvironmentBackend),
        )
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
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}
