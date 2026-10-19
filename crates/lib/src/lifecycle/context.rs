//! Scenario [`Context`]: registered resources, bindings, and per-run metadata.
//!
//! [`crate::lifecycle::run_scenario`] and `#[shakedown::test]` share one context so a scenario
//! can register engines, read bindings, and record events without process globals.

#[cfg(any(feature = "mock-services", feature = "services"))]
use crate::error::InternalError;
use crate::error::{ScenarioId, ShakedownError};
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
use crate::mock_service::udp::{UdpMockService, UdpMockServiceRegistration, UdpMockServiceServer};
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
use crate::outbox::Outbox;
#[cfg(feature = "duckdb")]
use crate::resources::duckdb::DuckDbHelpers;
#[cfg(feature = "elasticsearch")]
use crate::resources::elasticsearch::ElasticsearchHelpers;
#[cfg(feature = "influxdb")]
use crate::resources::influxdb::InfluxDbHelpers;
#[cfg(feature = "kafka")]
use crate::resources::kafka::KafkaHelpers;
#[cfg(feature = "mariadb")]
use crate::resources::mariadb::MariaDbHelpers;
#[cfg(feature = "mongodb")]
use crate::resources::mongo::MongoHelpers;
#[cfg(feature = "mysql")]
use crate::resources::mysql::MysqlHelpers;
#[cfg(feature = "nats")]
use crate::resources::nats::NatsHelpers;
#[cfg(feature = "neo4j")]
use crate::resources::neo4j::Neo4jHelpers;
#[cfg(feature = "opensearch")]
use crate::resources::opensearch::OpenSearchHelpers;
#[cfg(feature = "postgres")]
use crate::resources::postgres::PostgresHelpers;
#[cfg(feature = "qdrant")]
use crate::resources::qdrant::QdrantHelpers;
#[cfg(feature = "rabbitmq")]
use crate::resources::rabbit::RabbitHelpers;
#[cfg(any(feature = "redis", feature = "valkey"))]
use crate::resources::redis::RedisHelpers;
#[cfg(feature = "sqlite")]
use crate::resources::sqlite::SqliteHelpers;
#[cfg(feature = "timescaledb")]
use crate::resources::timescaledb::TimescaledbHelpers;
#[cfg(feature = "weaviate")]
use crate::resources::weaviate::WeaviateHelpers;
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
use crate::resources::{
    BuiltinResource, FakeResource, KafkaBindings, RegisteredResource, Resource,
};
#[cfg(feature = "services")]
use crate::service::ServiceHandle;
use std::collections::HashMap;
#[cfg(feature = "mock-services")]
use std::sync::Arc;

use super::environment::{EnvironmentBackend, NoopEnvironmentBackend, ScenarioKind};

/// Map key for the single-resource / unnamed binding path ([`Context::set_kafka_bindings`], etc.).
pub const DEFAULT_RESOURCE_BINDING_NAME: &str = "default";

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
    pub(in crate::lifecycle) pending_http_mock_services:
        Vec<(String, Arc<dyn HttpMockServiceTrait>)>,
    /// Running HTTP mock service handles (populated when servers start).
    #[cfg(feature = "mock-services")]
    pub(in crate::lifecycle) mock_service_handles: HashMap<String, Arc<MockServiceHandle>>,
    /// HTTP mock service server tasks: shutdown channel + join handle (paired with names for timeline messages).
    #[cfg(feature = "mock-services")]
    pub(in crate::lifecycle) http_mock_service_servers: Vec<(String, HttpMockServiceServer)>,
    /// TCP mock services registered before [`run_scenario`] starts listeners (after seed).
    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    pub(in crate::lifecycle) pending_tcp_mock_services: Vec<(String, TcpMockServiceRegistration)>,
    /// TCP mock accept loops (shutdown channel + join handle).
    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    pub(in crate::lifecycle) tcp_mock_service_servers: Vec<(String, TcpMockServiceServer)>,
    /// UDP mock services registered before [`run_scenario`] starts sockets (after seed).
    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    pub(in crate::lifecycle) pending_udp_mock_services: Vec<(String, UdpMockServiceRegistration)>,
    /// UDP mock recv loops (shutdown channel + join handle).
    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    pub(in crate::lifecycle) udp_mock_service_servers: Vec<(String, UdpMockServiceServer)>,
    /// Local address HTTP and gRPC mock listeners bind to; port is always ephemeral (`0`). Defaults to loopback.
    #[cfg(feature = "mock-services")]
    pub(in crate::lifecycle) mock_service_bind_ip: std::net::IpAddr,
    /// gRPC mock services registered before [`run_scenario`] starts tonic servers (after seed).
    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    pub(in crate::lifecycle) pending_grpc_mock_services:
        Vec<(String, Arc<dyn GrpcMockServiceTrait>)>,
    /// Running gRPC mock service servers (tonic transport).
    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    pub(in crate::lifecycle) grpc_mock_service_servers: Vec<(String, GrpcMockServiceServer)>,
    /// True once HTTP and gRPC mock service servers have been started for this [`run_scenario`] invocation.
    #[cfg(feature = "mock-services")]
    pub(in crate::lifecycle) mock_service_servers_phase_complete: bool,
    /// Registered [`crate::service::ServiceSpec`] entries (service configured); [`ServiceHandle`] set when the service is running and healthy.
    #[cfg(feature = "services")]
    pub(crate) services: HashMap<String, crate::service::ServiceReference>,
    /// Cluster topology handles keyed by resource marker type ([`Context::register_cluster_handle`]).
    #[cfg(feature = "cluster")]
    cluster_handles: HashMap<std::any::TypeId, crate::cluster::ClusterHandle>,
    /// Shared MongoDB replica set cluster control ([`crate::cluster::mongo_replica_set::MongoClusterOps`]).
    #[cfg(feature = "mongodb-cluster")]
    pub(in crate::lifecycle) mongo_replica_set_cluster: Option<
        std::sync::Arc<
            crate::cluster::mongo_replica_set::cluster_control::MongoReplicaSetClusterControl,
        >,
    >,
    /// Shared KRaft Kafka cluster control ([`crate::cluster::kafka_cluster::KafkaClusterOps`]).
    #[cfg(feature = "kafka-cluster")]
    pub(in crate::lifecycle) kafka_cluster_control: Option<
        std::sync::Arc<crate::cluster::kafka_cluster::cluster_control::KafkaClusterClusterControl>,
    >,
    /// Shared Redis Sentinel cluster control ([`crate::cluster::redis_sentinel::RedisSentinelOps`]).
    #[cfg(feature = "redis-sentinel")]
    pub(in crate::lifecycle) redis_sentinel_cluster: Option<
        std::sync::Arc<
            crate::cluster::redis_sentinel::cluster_control::RedisSentinelClusterControl,
        >,
    >,
    /// Per-scenario PostgreSQL primary + replica control ([`crate::cluster::postgres_replica_set::PostgresReplicaSetOps`]).
    #[cfg(feature = "postgres-cluster")]
    pub(in crate::lifecycle) postgres_replica_set_cluster: Option<
        std::sync::Arc<
            crate::cluster::postgres_replica_set::cluster_control::PostgresReplicaSetClusterControl,
        >,
    >,
    /// Shared RabbitMQ Erlang cluster control ([`crate::cluster::rabbit_cluster::RabbitClusterOps`]).
    #[cfg(feature = "rabbitmq-cluster")]
    pub(in crate::lifecycle) rabbit_cluster_control: Option<
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
    /// Create a new empty context with the default noop environment backend.
    pub fn new() -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(NoopEnvironmentBackend),
            scenario_id: ScenarioId::empty(),
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

    /// Create a new context with a custom environment backend.
    pub fn with_backend(backend: impl EnvironmentBackend + 'static) -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(backend),
            scenario_id: ScenarioId::empty(),
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

    /// Create a new context with a specific scenario ID.
    pub fn with_scenario_id(scenario_id: impl Into<String>) -> Self {
        Self {
            resources: Vec::new(),
            environment_backend: Box::new(NoopEnvironmentBackend),
            scenario_id: ScenarioId::new(scenario_id),
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

    /// Register a first-party [`BuiltinResource`].
    pub fn register_builtin(&mut self, resource: BuiltinResource) {
        self.resources.push(RegisteredResource::Builtin(resource));
    }

    /// Register an extension resource with the context.
    pub fn register_extension<R: Resource + 'static>(&mut self, resource: R) {
        self.resources
            .push(RegisteredResource::Extension(Box::new(resource)));
    }

    /// Mutable reference to the registered [`crate::FakeResource`], if present.
    pub fn fake_resource_mut(&mut self) -> Option<&mut crate::FakeResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Fake(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::KafkaResource`], if present (`kafka` feature).
    #[cfg(feature = "kafka")]
    pub fn kafka_resource_mut(&mut self) -> Option<&mut crate::KafkaResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Kafka(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::RabbitResource`], if present (`rabbitmq` feature).
    #[cfg(feature = "rabbitmq")]
    pub fn rabbit_resource_mut(&mut self) -> Option<&mut crate::RabbitResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Rabbit(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::PostgresResource`], if present (`postgres` feature).
    #[cfg(feature = "postgres")]
    pub fn postgres_resource_mut(&mut self) -> Option<&mut crate::PostgresResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Postgres(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::PostgresReplicaSetResource`], if present (`postgres-cluster` feature).
    #[cfg(feature = "postgres-cluster")]
    pub fn postgres_replica_set_resource_mut(
        &mut self,
    ) -> Option<&mut crate::PostgresReplicaSetResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::PostgresReplicaSet(resource)) => {
                    Some(resource)
                }
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::MariaDBResource`], if present (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    pub fn mariadb_resource_mut(&mut self) -> Option<&mut crate::MariaDBResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::MariaDB(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::MysqlResource`], if present (`mysql` feature).
    #[cfg(feature = "mysql")]
    pub fn mysql_resource_mut(&mut self) -> Option<&mut crate::MysqlResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Mysql(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::SqliteResource`], if present (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    pub fn sqlite_resource_mut(&mut self) -> Option<&mut crate::SqliteResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Sqlite(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::DuckDbResource`], if present (`duckdb` feature).
    #[cfg(feature = "duckdb")]
    pub fn duckdb_resource_mut(&mut self) -> Option<&mut crate::DuckDbResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::DuckDb(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::ToxiproxyResource`], if present (`fault-injection` feature).
    #[cfg(feature = "fault-injection")]
    pub fn toxiproxy_resource_mut(&mut self) -> Option<&mut crate::ToxiproxyResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Toxiproxy(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::RedisResource`], if present (`redis` feature).
    #[cfg(feature = "redis")]
    pub fn redis_resource_mut(&mut self) -> Option<&mut crate::RedisResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Redis(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::ValkeyResource`], if present (`valkey` feature).
    #[cfg(feature = "valkey")]
    pub fn valkey_resource_mut(&mut self) -> Option<&mut crate::ValkeyResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Valkey(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::MongoResource`], if present (`mongodb` feature).
    #[cfg(feature = "mongodb")]
    pub fn mongo_resource_mut(&mut self) -> Option<&mut crate::MongoResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Mongo(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::MongoReplicaSetResource`], if present (`mongodb-cluster` feature).
    #[cfg(feature = "mongodb-cluster")]
    pub fn mongo_replica_set_resource_mut(
        &mut self,
    ) -> Option<&mut crate::MongoReplicaSetResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::MongoReplicaSet(resource)) => {
                    Some(resource)
                }
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::KafkaClusterResource`], if present (`kafka-cluster` feature).
    #[cfg(feature = "kafka-cluster")]
    pub fn kafka_cluster_resource_mut(&mut self) -> Option<&mut crate::KafkaClusterResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::KafkaCluster(resource)) => {
                    Some(resource)
                }
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::RabbitClusterResource`], if present (`rabbitmq-cluster` feature).
    #[cfg(feature = "rabbitmq-cluster")]
    pub fn rabbit_cluster_resource_mut(&mut self) -> Option<&mut crate::RabbitClusterResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::RabbitCluster(resource)) => {
                    Some(resource)
                }
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::RedisSentinelResource`], if present (`redis-sentinel` feature).
    #[cfg(feature = "redis-sentinel")]
    pub fn redis_sentinel_resource_mut(&mut self) -> Option<&mut crate::RedisSentinelResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::RedisSentinel(resource)) => {
                    Some(resource)
                }
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::NatsResource`], if present (`nats` feature).
    #[cfg(feature = "nats")]
    pub fn nats_resource_mut(&mut self) -> Option<&mut crate::NatsResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Nats(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::Neo4jResource`], if present (`neo4j` feature).
    #[cfg(feature = "neo4j")]
    pub fn neo4j_resource_mut(&mut self) -> Option<&mut crate::Neo4jResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Neo4j(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::ElasticsearchResource`], if present (`elasticsearch` feature).
    #[cfg(feature = "elasticsearch")]
    pub fn elasticsearch_resource_mut(&mut self) -> Option<&mut crate::ElasticsearchResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Elasticsearch(resource)) => {
                    Some(resource)
                }
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::OpenSearchResource`], if present (`opensearch` feature).
    #[cfg(feature = "opensearch")]
    pub fn opensearch_resource_mut(&mut self) -> Option<&mut crate::OpenSearchResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::OpenSearch(resource)) => {
                    Some(resource)
                }
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::QdrantResource`], if present (`qdrant` feature).
    #[cfg(feature = "qdrant")]
    pub fn qdrant_resource_mut(&mut self) -> Option<&mut crate::QdrantResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Qdrant(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::WeaviateResource`], if present (`weaviate` feature).
    #[cfg(feature = "weaviate")]
    pub fn weaviate_resource_mut(&mut self) -> Option<&mut crate::WeaviateResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Weaviate(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::InfluxDbResource`], if present (`influxdb` feature).
    #[cfg(feature = "influxdb")]
    pub fn influxdb_resource_mut(&mut self) -> Option<&mut crate::InfluxDbResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::InfluxDb(resource)) => Some(resource),
                _ => None,
            })
    }

    /// Mutable reference to the registered [`crate::TimescaledbResource`], if present (`timescaledb` feature).
    #[cfg(feature = "timescaledb")]
    pub fn timescaledb_resource_mut(&mut self) -> Option<&mut crate::TimescaledbResource> {
        self.resources
            .iter_mut()
            .find_map(|registered| match registered {
                RegisteredResource::Builtin(BuiltinResource::Timescaledb(resource)) => {
                    Some(resource)
                }
                _ => None,
            })
    }

    /// Register a [`crate::cluster::ClusterHandle`] for retrieval via [`Context::cluster_handle`].
    ///
    /// `T` is a zero-sized marker (often a private struct) used to disambiguate multiple cluster
    /// resources in one scenario.
    #[cfg(feature = "cluster")]
    pub fn register_cluster_handle<T: 'static>(&mut self, handle: crate::cluster::ClusterHandle) {
        self.cluster_handles
            .insert(std::any::TypeId::of::<T>(), handle);
    }

    /// Look up a cluster handle registered with [`Context::register_cluster_handle`].
    #[cfg(feature = "cluster")]
    pub fn cluster_handle<T: 'static>(&self) -> Option<&crate::cluster::ClusterHandle> {
        self.cluster_handles.get(&std::any::TypeId::of::<T>())
    }

    /// Mutable cluster handle for wiring [`crate::cluster::ClusterHandle::set_event_sender`] before the run phase.
    #[cfg(feature = "cluster")]
    pub fn cluster_handle_mut<T: 'static>(&mut self) -> Option<&mut crate::cluster::ClusterHandle> {
        self.cluster_handles.get_mut(&std::any::TypeId::of::<T>())
    }

    /// Start an open-model (or closed-model VU) load driver; see [`crate::load::LoadHandle`].
    #[cfg(feature = "load")]
    pub async fn start_load<E, F, Fut>(
        &mut self,
        load_name: &str,
        profile: crate::load::LoadProfile,
        work: F,
    ) -> Result<crate::load::LoadHandle, crate::load::LoadError>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), E>> + Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        crate::load::handle::start_load(self, load_name, profile, work).await
    }

    /// Used by [`run_scenario`] to wire [`Event`] merge for load phase transitions.
    #[cfg(feature = "load")]
    pub(crate) fn set_load_event_sender(
        &mut self,
        sender: Option<std::sync::mpsc::Sender<crate::event::Event>>,
    ) {
        self.load_event_sender = sender;
    }

    /// Sender cloned by [`crate::load::handle::start_load`] for structured load timeline rows.
    #[cfg(feature = "load")]
    pub(crate) fn load_event_sender(
        &self,
    ) -> Option<&std::sync::mpsc::Sender<crate::event::Event>> {
        self.load_event_sender.as_ref()
    }

    /// Used by [`run_scenario`] to merge [`crate::assert_eventually`] poll rows into [`crate::EventLog`].
    pub(crate) fn set_scenario_application_event_sender(
        &mut self,
        sender: Option<std::sync::mpsc::Sender<crate::event::Event>>,
    ) {
        self.scenario_application_event_sender = sender;
    }

    /// Sender for structured scenario-body events (run phase only), e.g. [`crate::assert_eventually`].
    pub(crate) fn scenario_application_event_sender(
        &self,
    ) -> Option<&std::sync::mpsc::Sender<crate::event::Event>> {
        self.scenario_application_event_sender.as_ref()
    }

    #[cfg(feature = "mongodb-cluster")]
    pub(crate) fn set_mongo_replica_set_cluster(
        &mut self,
        cluster: Option<
            std::sync::Arc<
                crate::cluster::mongo_replica_set::cluster_control::MongoReplicaSetClusterControl,
            >,
        >,
    ) {
        self.mongo_replica_set_cluster = cluster;
    }

    /// MongoDB replica set topology control ([`crate::cluster::mongo_replica_set::MongoClusterOps`]).
    #[cfg(feature = "mongodb-cluster")]
    pub fn mongo_cluster(
        &self,
    ) -> Option<crate::cluster::mongo_replica_set::MongoClusterHandle<'_>> {
        self.mongo_replica_set_cluster
            .as_ref()
            .map(crate::cluster::mongo_replica_set::MongoClusterHandle::new)
    }

    #[cfg(feature = "kafka-cluster")]
    pub(crate) fn set_kafka_cluster_control(
        &mut self,
        cluster: Option<
            std::sync::Arc<
                crate::cluster::kafka_cluster::cluster_control::KafkaClusterClusterControl,
            >,
        >,
    ) {
        self.kafka_cluster_control = cluster;
    }

    /// KRaft multi-broker Kafka partition leadership ([`crate::cluster::kafka_cluster::KafkaClusterOps`]).
    #[cfg(feature = "kafka-cluster")]
    pub fn kafka_cluster(&self) -> Option<crate::cluster::kafka_cluster::KafkaClusterHandle<'_>> {
        self.kafka_cluster_control
            .as_ref()
            .map(crate::cluster::kafka_cluster::KafkaClusterHandle::new)
    }

    #[cfg(feature = "redis-sentinel")]
    pub(crate) fn set_redis_sentinel_cluster(
        &mut self,
        cluster: Option<
            std::sync::Arc<
                crate::cluster::redis_sentinel::cluster_control::RedisSentinelClusterControl,
            >,
        >,
    ) {
        self.redis_sentinel_cluster = cluster;
    }

    /// Redis Sentinel failover and primary discovery ([`crate::cluster::redis_sentinel::RedisSentinelOps`]).
    #[cfg(feature = "redis-sentinel")]
    pub fn redis_sentinel_cluster(
        &self,
    ) -> Option<crate::cluster::redis_sentinel::RedisSentinelHandle<'_>> {
        self.redis_sentinel_cluster
            .as_ref()
            .map(crate::cluster::redis_sentinel::RedisSentinelHandle::new)
    }

    #[cfg(feature = "postgres-cluster")]
    pub(crate) fn set_postgres_replica_set_cluster(
        &mut self,
        cluster: Option<
            std::sync::Arc<
                crate::cluster::postgres_replica_set::cluster_control::PostgresReplicaSetClusterControl,
            >,
        >,
    ) {
        self.postgres_replica_set_cluster = cluster;
    }

    /// PostgreSQL replica-set promotion and primary discovery ([`crate::cluster::postgres_replica_set::PostgresReplicaSetOps`]).
    #[cfg(feature = "postgres-cluster")]
    pub fn postgres_replica_set_cluster(
        &self,
    ) -> Option<crate::cluster::postgres_replica_set::PostgresReplicaSetHandle<'_>> {
        self.postgres_replica_set_cluster
            .as_ref()
            .map(crate::cluster::postgres_replica_set::PostgresReplicaSetHandle::new)
    }

    #[cfg(feature = "rabbitmq-cluster")]
    pub(crate) fn set_rabbit_cluster_control(
        &mut self,
        cluster: Option<
            std::sync::Arc<
                crate::cluster::rabbit_cluster::cluster_control::RabbitClusterClusterControl,
            >,
        >,
    ) {
        self.rabbit_cluster_control = cluster;
    }

    /// RabbitMQ Erlang cluster quorum queue leadership ([`crate::cluster::rabbit_cluster::RabbitClusterOps`]).
    #[cfg(feature = "rabbitmq-cluster")]
    pub fn rabbit_cluster(
        &self,
    ) -> Option<crate::cluster::rabbit_cluster::RabbitClusterHandle<'_>> {
        self.rabbit_cluster_control
            .as_ref()
            .map(crate::cluster::rabbit_cluster::RabbitClusterHandle::new)
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
        self.kafka_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), kafka_bindings);
    }

    /// Set Kafka bindings under a logical name (multi-Kafka topologies).
    pub fn set_kafka_bindings_named(&mut self, name: &str, kafka_bindings: KafkaBindings) {
        self.kafka_bindings.insert(name.to_string(), kafka_bindings);
    }

    /// Get a reference to Kafka bindings if available.
    pub fn kafka_bindings(&self) -> Option<&KafkaBindings> {
        self.kafka_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Kafka bindings for a named resource registration.
    pub fn kafka_bindings_named(&self, name: &str) -> Option<&KafkaBindings> {
        self.kafka_bindings.get(name)
    }

    /// Set RabbitMQ bindings for this scenario (typically after Rabbit provision).
    #[cfg(feature = "rabbitmq")]
    pub fn set_rabbit_bindings(&mut self, rabbit_bindings: RabbitBindings) {
        self.rabbit_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), rabbit_bindings);
    }

    /// Set RabbitMQ bindings under a logical name.
    #[cfg(feature = "rabbitmq")]
    pub fn set_rabbit_bindings_named(&mut self, name: &str, rabbit_bindings: RabbitBindings) {
        self.rabbit_bindings
            .insert(name.to_string(), rabbit_bindings);
    }

    /// Get RabbitMQ bindings if available (`rabbitmq` feature).
    #[cfg(feature = "rabbitmq")]
    pub fn rabbit_bindings(&self) -> Option<&RabbitBindings> {
        self.rabbit_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// RabbitMQ bindings for a named resource registration (`rabbitmq` feature).
    #[cfg(feature = "rabbitmq")]
    pub fn rabbit_bindings_named(&self, name: &str) -> Option<&RabbitBindings> {
        self.rabbit_bindings.get(name)
    }

    /// Set Postgres bindings for this scenario (typically after Postgres provision).
    #[cfg(feature = "postgres")]
    pub fn set_postgres_bindings(&mut self, postgres_bindings: PostgresBindings) {
        self.postgres_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), postgres_bindings);
    }

    /// Set Postgres bindings under a logical name.
    #[cfg(feature = "postgres")]
    pub fn set_postgres_bindings_named(&mut self, name: &str, postgres_bindings: PostgresBindings) {
        self.postgres_bindings
            .insert(name.to_string(), postgres_bindings);
    }

    /// Get Postgres bindings if available (`postgres` feature).
    #[cfg(feature = "postgres")]
    pub fn postgres_bindings(&self) -> Option<&PostgresBindings> {
        self.postgres_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Postgres bindings for a named resource registration (`postgres` feature).
    #[cfg(feature = "postgres")]
    pub fn postgres_bindings_named(&self, name: &str) -> Option<&PostgresBindings> {
        self.postgres_bindings.get(name)
    }

    /// Set MariaDB bindings for this scenario (typically after MariaDB provision).
    #[cfg(feature = "mariadb")]
    pub fn set_mariadb_bindings(&mut self, mariadb_bindings: MariaDBBindings) {
        self.mariadb_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), mariadb_bindings);
    }

    /// Set MariaDB bindings under a logical name.
    #[cfg(feature = "mariadb")]
    pub fn set_mariadb_bindings_named(&mut self, name: &str, mariadb_bindings: MariaDBBindings) {
        self.mariadb_bindings
            .insert(name.to_string(), mariadb_bindings);
    }

    /// Get MariaDB bindings if available (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    pub fn mariadb_bindings(&self) -> Option<&MariaDBBindings> {
        self.mariadb_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// MariaDB bindings for a named resource registration (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    pub fn mariadb_bindings_named(&self, name: &str) -> Option<&MariaDBBindings> {
        self.mariadb_bindings.get(name)
    }

    /// Set MySQL (Oracle) bindings for this scenario (typically after MySQL provision).
    #[cfg(feature = "mysql")]
    pub fn set_mysql_bindings(&mut self, mysql_bindings: MysqlBindings) {
        self.mysql_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), mysql_bindings);
    }

    /// Set MySQL bindings under a logical name.
    #[cfg(feature = "mysql")]
    pub fn set_mysql_bindings_named(&mut self, name: &str, mysql_bindings: MysqlBindings) {
        self.mysql_bindings.insert(name.to_string(), mysql_bindings);
    }

    /// Get Oracle MySQL bindings if available (`mysql` feature).
    #[cfg(feature = "mysql")]
    pub fn mysql_bindings(&self) -> Option<&MysqlBindings> {
        self.mysql_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// MySQL bindings for a named resource registration (`mysql` feature).
    #[cfg(feature = "mysql")]
    pub fn mysql_bindings_named(&self, name: &str) -> Option<&MysqlBindings> {
        self.mysql_bindings.get(name)
    }

    /// Set SQLite bindings for this scenario (typically after SQLite provision).
    #[cfg(feature = "sqlite")]
    pub fn set_sqlite_bindings(&mut self, sqlite_bindings: SqliteBindings) {
        self.sqlite_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), sqlite_bindings);
    }

    /// Set SQLite bindings under a logical name.
    #[cfg(feature = "sqlite")]
    pub fn set_sqlite_bindings_named(&mut self, name: &str, sqlite_bindings: SqliteBindings) {
        self.sqlite_bindings
            .insert(name.to_string(), sqlite_bindings);
    }

    /// Get SQLite bindings if available (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    pub fn sqlite_bindings(&self) -> Option<&SqliteBindings> {
        self.sqlite_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// SQLite bindings for a named resource registration (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    pub fn sqlite_bindings_named(&self, name: &str) -> Option<&SqliteBindings> {
        self.sqlite_bindings.get(name)
    }

    /// Set DuckDB bindings for this scenario (typically after DuckDB provision).
    #[cfg(feature = "duckdb")]
    pub fn set_duckdb_bindings(&mut self, duckdb_bindings: DuckDbBindings) {
        self.duckdb_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), duckdb_bindings);
    }

    /// Set DuckDB bindings under a logical name.
    #[cfg(feature = "duckdb")]
    pub fn set_duckdb_bindings_named(&mut self, name: &str, duckdb_bindings: DuckDbBindings) {
        self.duckdb_bindings
            .insert(name.to_string(), duckdb_bindings);
    }

    /// DuckDB connection path when a DuckDB resource populated them (`duckdb` feature).
    #[cfg(feature = "duckdb")]
    pub fn duckdb_bindings(&self) -> Option<&DuckDbBindings> {
        self.duckdb_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// DuckDB bindings for a named resource registration (`duckdb` feature).
    #[cfg(feature = "duckdb")]
    pub fn duckdb_bindings_named(&self, name: &str) -> Option<&DuckDbBindings> {
        self.duckdb_bindings.get(name)
    }

    /// Set Redis / Valkey bindings for this scenario (typically after Redis or Valkey provision).
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn set_redis_bindings(&mut self, redis_bindings: RedisBindings) {
        self.redis_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), redis_bindings);
    }

    /// Set Redis / Valkey bindings under a logical name.
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn set_redis_bindings_named(&mut self, name: &str, redis_bindings: RedisBindings) {
        self.redis_bindings.insert(name.to_string(), redis_bindings);
    }

    /// Redis or Valkey connection details when a matching resource populated them (`redis` / `valkey` features).
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn redis_bindings(&self) -> Option<&RedisBindings> {
        self.redis_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Redis or Valkey bindings for a named resource registration.
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn redis_bindings_named(&self, name: &str) -> Option<&RedisBindings> {
        self.redis_bindings.get(name)
    }

    /// Set Redis Sentinel discovery bindings when [`crate::BuiltinResource::RedisSentinel`] populated them.
    #[cfg(feature = "redis-sentinel")]
    pub fn set_redis_sentinel_bindings(&mut self, redis_sentinel_bindings: RedisSentinelBindings) {
        self.redis_sentinel_bindings.insert(
            DEFAULT_RESOURCE_BINDING_NAME.to_string(),
            redis_sentinel_bindings,
        );
    }

    /// Set Redis Sentinel bindings under a logical name.
    #[cfg(feature = "redis-sentinel")]
    pub fn set_redis_sentinel_bindings_named(
        &mut self,
        name: &str,
        redis_sentinel_bindings: RedisSentinelBindings,
    ) {
        self.redis_sentinel_bindings
            .insert(name.to_string(), redis_sentinel_bindings);
    }

    /// Sentinel endpoints + master name + primary URL (`redis-sentinel` feature).
    #[cfg(feature = "redis-sentinel")]
    pub fn redis_sentinel_bindings(&self) -> Option<&RedisSentinelBindings> {
        self.redis_sentinel_bindings
            .get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Redis Sentinel bindings for a named resource registration (`redis-sentinel` feature).
    #[cfg(feature = "redis-sentinel")]
    pub fn redis_sentinel_bindings_named(&self, name: &str) -> Option<&RedisSentinelBindings> {
        self.redis_sentinel_bindings.get(name)
    }

    /// Set MongoDB bindings for this scenario (typically after Mongo provision).
    #[cfg(feature = "mongodb")]
    pub fn set_mongo_bindings(&mut self, mongo_bindings: MongoBindings) {
        self.mongo_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), mongo_bindings);
    }

    /// Set MongoDB bindings under a logical name.
    #[cfg(feature = "mongodb")]
    pub fn set_mongo_bindings_named(&mut self, name: &str, mongo_bindings: MongoBindings) {
        self.mongo_bindings.insert(name.to_string(), mongo_bindings);
    }

    /// MongoDB connection details when a Mongo resource populated them (`mongodb` feature).
    #[cfg(feature = "mongodb")]
    pub fn mongo_bindings(&self) -> Option<&MongoBindings> {
        self.mongo_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// MongoDB bindings for a named resource registration (`mongodb` feature).
    #[cfg(feature = "mongodb")]
    pub fn mongo_bindings_named(&self, name: &str) -> Option<&MongoBindings> {
        self.mongo_bindings.get(name)
    }

    /// Set NATS bindings for this scenario (typically after NATS provision).
    #[cfg(feature = "nats")]
    pub fn set_nats_bindings(&mut self, nats_bindings: NatsBindings) {
        self.nats_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), nats_bindings);
    }

    /// Set NATS bindings under a logical name.
    #[cfg(feature = "nats")]
    pub fn set_nats_bindings_named(&mut self, name: &str, nats_bindings: NatsBindings) {
        self.nats_bindings.insert(name.to_string(), nats_bindings);
    }

    /// NATS connection details when a NATS resource populated them (`nats` feature).
    #[cfg(feature = "nats")]
    pub fn nats_bindings(&self) -> Option<&NatsBindings> {
        self.nats_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// NATS bindings for a named resource registration (`nats` feature).
    #[cfg(feature = "nats")]
    pub fn nats_bindings_named(&self, name: &str) -> Option<&NatsBindings> {
        self.nats_bindings.get(name)
    }

    /// Set Neo4j bindings for this scenario (typically after Neo4j provision).
    #[cfg(feature = "neo4j")]
    pub fn set_neo4j_bindings(&mut self, neo4j_bindings: Neo4jBindings) {
        self.neo4j_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), neo4j_bindings);
    }

    /// Set Neo4j bindings under a logical name.
    #[cfg(feature = "neo4j")]
    pub fn set_neo4j_bindings_named(&mut self, name: &str, neo4j_bindings: Neo4jBindings) {
        self.neo4j_bindings.insert(name.to_string(), neo4j_bindings);
    }

    /// Neo4j connection details when a Neo4j resource populated them (`neo4j` feature).
    #[cfg(feature = "neo4j")]
    pub fn neo4j_bindings(&self) -> Option<&Neo4jBindings> {
        self.neo4j_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Neo4j bindings for a named resource registration (`neo4j` feature).
    #[cfg(feature = "neo4j")]
    pub fn neo4j_bindings_named(&self, name: &str) -> Option<&Neo4jBindings> {
        self.neo4j_bindings.get(name)
    }

    /// Set Elasticsearch bindings for this scenario (typically after Elasticsearch provision).
    #[cfg(feature = "elasticsearch")]
    pub fn set_elasticsearch_bindings(&mut self, elasticsearch_bindings: ElasticsearchBindings) {
        self.elasticsearch_bindings.insert(
            DEFAULT_RESOURCE_BINDING_NAME.to_string(),
            elasticsearch_bindings,
        );
    }

    /// Set Elasticsearch bindings under a logical name.
    #[cfg(feature = "elasticsearch")]
    pub fn set_elasticsearch_bindings_named(
        &mut self,
        name: &str,
        elasticsearch_bindings: ElasticsearchBindings,
    ) {
        self.elasticsearch_bindings
            .insert(name.to_string(), elasticsearch_bindings);
    }

    /// Elasticsearch HTTP URL and index prefix when an Elasticsearch resource populated them (`elasticsearch` feature).
    #[cfg(feature = "elasticsearch")]
    pub fn elasticsearch_bindings(&self) -> Option<&ElasticsearchBindings> {
        self.elasticsearch_bindings
            .get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Elasticsearch bindings for a named resource registration (`elasticsearch` feature).
    #[cfg(feature = "elasticsearch")]
    pub fn elasticsearch_bindings_named(&self, name: &str) -> Option<&ElasticsearchBindings> {
        self.elasticsearch_bindings.get(name)
    }

    /// Set OpenSearch bindings for this scenario (typically after OpenSearch provision).
    #[cfg(feature = "opensearch")]
    pub fn set_opensearch_bindings(&mut self, opensearch_bindings: OpenSearchBindings) {
        self.opensearch_bindings.insert(
            DEFAULT_RESOURCE_BINDING_NAME.to_string(),
            opensearch_bindings,
        );
    }

    /// Set OpenSearch bindings under a logical name.
    #[cfg(feature = "opensearch")]
    pub fn set_opensearch_bindings_named(
        &mut self,
        name: &str,
        opensearch_bindings: OpenSearchBindings,
    ) {
        self.opensearch_bindings
            .insert(name.to_string(), opensearch_bindings);
    }

    /// OpenSearch HTTP URL and index prefix when an OpenSearch resource populated them (`opensearch` feature).
    #[cfg(feature = "opensearch")]
    pub fn opensearch_bindings(&self) -> Option<&OpenSearchBindings> {
        self.opensearch_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// OpenSearch bindings for a named resource registration (`opensearch` feature).
    #[cfg(feature = "opensearch")]
    pub fn opensearch_bindings_named(&self, name: &str) -> Option<&OpenSearchBindings> {
        self.opensearch_bindings.get(name)
    }

    /// Set Qdrant bindings for this scenario (typically after Qdrant provision).
    #[cfg(feature = "qdrant")]
    pub fn set_qdrant_bindings(&mut self, qdrant_bindings: QdrantBindings) {
        self.qdrant_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), qdrant_bindings);
    }

    /// Set Qdrant bindings under a logical name.
    #[cfg(feature = "qdrant")]
    pub fn set_qdrant_bindings_named(&mut self, name: &str, qdrant_bindings: QdrantBindings) {
        self.qdrant_bindings
            .insert(name.to_string(), qdrant_bindings);
    }

    /// Qdrant gRPC/REST endpoints and collection prefix when a Qdrant resource populated them (`qdrant` feature).
    #[cfg(feature = "qdrant")]
    pub fn qdrant_bindings(&self) -> Option<&QdrantBindings> {
        self.qdrant_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Qdrant bindings for a named resource registration (`qdrant` feature).
    #[cfg(feature = "qdrant")]
    pub fn qdrant_bindings_named(&self, name: &str) -> Option<&QdrantBindings> {
        self.qdrant_bindings.get(name)
    }

    /// Set Weaviate bindings for this scenario (typically after Weaviate provision).
    #[cfg(feature = "weaviate")]
    pub fn set_weaviate_bindings(&mut self, weaviate_bindings: WeaviateBindings) {
        self.weaviate_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), weaviate_bindings);
    }

    /// Set Weaviate bindings under a logical name.
    #[cfg(feature = "weaviate")]
    pub fn set_weaviate_bindings_named(&mut self, name: &str, weaviate_bindings: WeaviateBindings) {
        self.weaviate_bindings
            .insert(name.to_string(), weaviate_bindings);
    }

    /// Weaviate REST URL and class name prefix when a Weaviate resource populated them (`weaviate` feature).
    #[cfg(feature = "weaviate")]
    pub fn weaviate_bindings(&self) -> Option<&WeaviateBindings> {
        self.weaviate_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// Weaviate bindings for a named resource registration (`weaviate` feature).
    #[cfg(feature = "weaviate")]
    pub fn weaviate_bindings_named(&self, name: &str) -> Option<&WeaviateBindings> {
        self.weaviate_bindings.get(name)
    }

    /// Set InfluxDB bindings for this scenario (typically after InfluxDB provision).
    #[cfg(feature = "influxdb")]
    pub fn set_influxdb_bindings(&mut self, influxdb_bindings: InfluxDbBindings) {
        self.influxdb_bindings
            .insert(DEFAULT_RESOURCE_BINDING_NAME.to_string(), influxdb_bindings);
    }

    /// Set InfluxDB bindings under a logical name.
    #[cfg(feature = "influxdb")]
    pub fn set_influxdb_bindings_named(&mut self, name: &str, influxdb_bindings: InfluxDbBindings) {
        self.influxdb_bindings
            .insert(name.to_string(), influxdb_bindings);
    }

    /// InfluxDB HTTP URL, token, org, and scenario bucket when an InfluxDB resource populated them (`influxdb` feature).
    #[cfg(feature = "influxdb")]
    pub fn influxdb_bindings(&self) -> Option<&InfluxDbBindings> {
        self.influxdb_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// InfluxDB bindings for a named resource registration (`influxdb` feature).
    #[cfg(feature = "influxdb")]
    pub fn influxdb_bindings_named(&self, name: &str) -> Option<&InfluxDbBindings> {
        self.influxdb_bindings.get(name)
    }

    /// Set TimescaleDB bindings for this scenario (typically after TimescaleDB provision).
    #[cfg(feature = "timescaledb")]
    pub fn set_timescaledb_bindings(&mut self, timescaledb_bindings: PostgresBindings) {
        self.timescaledb_bindings.insert(
            DEFAULT_RESOURCE_BINDING_NAME.to_string(),
            timescaledb_bindings,
        );
    }

    /// Set TimescaleDB bindings under a logical name.
    #[cfg(feature = "timescaledb")]
    pub fn set_timescaledb_bindings_named(
        &mut self,
        name: &str,
        timescaledb_bindings: PostgresBindings,
    ) {
        self.timescaledb_bindings
            .insert(name.to_string(), timescaledb_bindings);
    }

    /// TimescaleDB connection details when a TimescaleDB resource populated them (`timescaledb` feature).
    #[cfg(feature = "timescaledb")]
    pub fn timescaledb_bindings(&self) -> Option<&PostgresBindings> {
        self.timescaledb_bindings.get(DEFAULT_RESOURCE_BINDING_NAME)
    }

    /// TimescaleDB bindings for a named resource registration (`timescaledb` feature).
    #[cfg(feature = "timescaledb")]
    pub fn timescaledb_bindings_named(&self, name: &str) -> Option<&PostgresBindings> {
        self.timescaledb_bindings.get(name)
    }

    /// Set the local IP address HTTP and gRPC mock listeners bind to (port is always ephemeral).
    ///
    /// Defaults to [`std::net::Ipv4Addr::LOCALHOST`]. Call before [`run_scenario`], for example
    /// `Ipv4Addr::UNSPECIFIED` (`0.0.0.0`) when something on the host network must dial the stub.
    #[cfg(feature = "mock-services")]
    pub fn set_mock_service_bind_ip(&mut self, ip: std::net::IpAddr) {
        self.mock_service_bind_ip = ip;
    }

    /// Register an HTTP mock service server to be started after the seed phase of [`run_scenario`].
    ///
    /// Call before [`run_scenario`]. After servers start, use [`Self::mock_service_url`] and [`Self::mock_service`].
    #[cfg(feature = "mock-services")]
    pub fn register_http_mock_service(
        &mut self,
        name: &str,
        mock_service: impl HttpMockServiceTrait,
    ) -> Result<(), ShakedownError> {
        if self.mock_service_servers_phase_complete {
            return Err(ShakedownError::Internal(InternalError::InvalidConfiguration(
                String::from(
                    "register_http_mock_service called after mock service servers were started for this scenario run",
                ),
            )));
        }
        #[cfg(all(feature = "mock-services", feature = "grpc"))]
        let grpc_name_taken = self
            .pending_grpc_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "grpc")))]
        let grpc_name_taken = false;

        #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
        let tcp_name_taken = self
            .pending_tcp_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "tcp-mock-services")))]
        let tcp_name_taken = false;

        #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
        let udp_name_taken = self
            .pending_udp_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "udp-mock-services")))]
        let udp_name_taken = false;

        if self.mock_service_handles.contains_key(name)
            || self
                .pending_http_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || grpc_name_taken
            || tcp_name_taken
            || udp_name_taken
        {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(format!(
                    "duplicate mock service name '{name}'"
                )),
            ));
        }
        self.pending_http_mock_services
            .push((name.to_owned(), Arc::new(mock_service)));
        Ok(())
    }

    /// Register a gRPC mock service server to be started after the seed phase of [`run_scenario`].
    ///
    /// Call before [`run_scenario`]. After servers start, use [`Self::mock_service_url`] and [`Self::mock_service`].
    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    pub fn register_grpc_mock_service(
        &mut self,
        name: &str,
        mock_service: impl GrpcMockServiceTrait,
    ) -> Result<(), ShakedownError> {
        if self.mock_service_servers_phase_complete {
            return Err(ShakedownError::Internal(InternalError::InvalidConfiguration(
                String::from(
                    "register_grpc_mock_service called after mock service servers were started for this scenario run",
                ),
            )));
        }
        #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
        let tcp_name_taken = self
            .pending_tcp_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "tcp-mock-services")))]
        let tcp_name_taken = false;

        #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
        let udp_name_taken = self
            .pending_udp_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "udp-mock-services")))]
        let udp_name_taken = false;

        if self.mock_service_handles.contains_key(name)
            || self
                .pending_http_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || self
                .pending_grpc_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || tcp_name_taken
            || udp_name_taken
        {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(format!(
                    "duplicate mock service name '{name}'"
                )),
            ));
        }
        self.pending_grpc_mock_services
            .push((name.to_owned(), Arc::new(mock_service)));
        Ok(())
    }

    /// Register a TCP mock service to be started after the seed phase of [`run_scenario`].
    ///
    /// Call before [`run_scenario`]. After servers start, use [`Self::mock_service_url`] and [`Self::mock_service`].
    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    pub fn register_tcp_mock_service(
        &mut self,
        name: &str,
        registration: TcpMockServiceRegistration,
    ) -> Result<(), ShakedownError> {
        if self.mock_service_servers_phase_complete {
            return Err(ShakedownError::Internal(InternalError::InvalidConfiguration(
                String::from(
                    "register_tcp_mock_service called after mock service servers were started for this scenario run",
                ),
            )));
        }
        #[cfg(all(feature = "mock-services", feature = "grpc"))]
        let grpc_name_taken = self
            .pending_grpc_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "grpc")))]
        let grpc_name_taken = false;

        #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
        let udp_name_taken = self
            .pending_udp_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "udp-mock-services")))]
        let udp_name_taken = false;

        if self.mock_service_handles.contains_key(name)
            || self
                .pending_http_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || grpc_name_taken
            || self
                .pending_tcp_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || udp_name_taken
        {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(format!(
                    "duplicate mock service name '{name}'"
                )),
            ));
        }
        self.pending_tcp_mock_services
            .push((name.to_owned(), registration));
        Ok(())
    }

    /// Register a UDP mock service to be started after the seed phase of [`run_scenario`].
    ///
    /// Call before [`run_scenario`]. After servers start, use [`Self::mock_service_url`] and [`Self::mock_service`].
    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    pub fn register_udp_mock_service(
        &mut self,
        name: &str,
        mock_service: UdpMockService,
    ) -> Result<(), ShakedownError> {
        if self.mock_service_servers_phase_complete {
            return Err(ShakedownError::Internal(InternalError::InvalidConfiguration(
                String::from(
                    "register_udp_mock_service called after mock service servers were started for this scenario run",
                ),
            )));
        }
        #[cfg(all(feature = "mock-services", feature = "grpc"))]
        let grpc_name_taken = self
            .pending_grpc_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "grpc")))]
        let grpc_name_taken = false;

        #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
        let tcp_name_taken = self
            .pending_tcp_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "tcp-mock-services")))]
        let tcp_name_taken = false;

        if self.mock_service_handles.contains_key(name)
            || self
                .pending_http_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || grpc_name_taken
            || tcp_name_taken
            || self
                .pending_udp_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
        {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(format!(
                    "duplicate mock service name '{name}'"
                )),
            ));
        }
        self.pending_udp_mock_services
            .push((name.to_owned(), mock_service.into_registration()));
        Ok(())
    }

    /// Test-only: control [`Self::register_http_mock_service`] / [`Self::register_grpc_mock_service`] phase guards.
    #[cfg(all(test, feature = "mock-services"))]
    pub(crate) fn set_mock_service_servers_phase_complete_for_test(&mut self, complete: bool) {
        self.mock_service_servers_phase_complete = complete;
    }

    /// Client-facing base URL for a registered mock service (HTTP or gRPC).
    ///
    /// Returns [`None`] if no mock service was registered under `name` or mock service servers have not
    /// started yet for this run (e.g. called before [`run_scenario`] has bound listeners).
    #[cfg(feature = "mock-services")]
    pub fn mock_service_url(&self, name: &str) -> Option<String> {
        self.mock_service_handles
            .get(name)
            .map(|handle| handle.client_url.clone())
    }

    /// Handle for assertions on recorded mock service traffic (HTTP or gRPC).
    ///
    /// Returns [`None`] if no mock service was registered under `name` or mock service servers have not
    /// started yet for this run.
    #[cfg(feature = "mock-services")]
    pub fn mock_service(&self, name: &str) -> Option<&MockServiceHandle> {
        self.mock_service_handles.get(name).map(|arc| arc.as_ref())
    }

    /// Build an outbox helper bound to this context (`outbox` + at least one storage engine feature).
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
    pub fn outbox(&self) -> Outbox<'_> {
        Outbox::new(self)
    }

    /// Kafka produce/consume helpers (`kafka` feature), scoped to the [`DEFAULT_RESOURCE_BINDING_NAME`] binding.
    #[cfg(feature = "kafka")]
    pub fn kafka(&self) -> Result<KafkaHelpers<'_>, ShakedownError> {
        KafkaHelpers::new(self)
    }

    /// Kafka helpers scoped to the binding registered under `name`.
    #[cfg(feature = "kafka")]
    pub fn kafka_named(&self, name: &str) -> Result<KafkaHelpers<'_>, ShakedownError> {
        KafkaHelpers::named(self, name)
    }

    /// RabbitMQ publish/topology helpers (`rabbitmq` feature).
    #[cfg(feature = "rabbitmq")]
    pub fn rabbit(&self) -> Result<RabbitHelpers<'_>, ShakedownError> {
        RabbitHelpers::new(self)
    }

    /// RabbitMQ helpers scoped to the binding registered under `name`.
    #[cfg(feature = "rabbitmq")]
    pub fn rabbit_named(&self, name: &str) -> Result<RabbitHelpers<'_>, ShakedownError> {
        RabbitHelpers::named(self, name)
    }

    /// PostgreSQL execute/query helpers (`postgres` feature).
    #[cfg(feature = "postgres")]
    pub fn postgres(&self) -> Result<PostgresHelpers<'_>, ShakedownError> {
        PostgresHelpers::new(self)
    }

    /// PostgreSQL helpers scoped to the binding registered under `name`.
    #[cfg(feature = "postgres")]
    pub fn postgres_named(&self, name: &str) -> Result<PostgresHelpers<'_>, ShakedownError> {
        PostgresHelpers::named(self, name)
    }

    /// MariaDB execute/query helpers (`mariadb` feature).
    #[cfg(feature = "mariadb")]
    pub fn mariadb(&self) -> Result<MariaDbHelpers<'_>, ShakedownError> {
        MariaDbHelpers::new(self)
    }

    /// MariaDB helpers scoped to the binding registered under `name`.
    #[cfg(feature = "mariadb")]
    pub fn mariadb_named(&self, name: &str) -> Result<MariaDbHelpers<'_>, ShakedownError> {
        MariaDbHelpers::named(self, name)
    }

    /// Oracle MySQL execute/query helpers (`mysql` feature).
    #[cfg(feature = "mysql")]
    pub fn mysql(&self) -> Result<MysqlHelpers<'_>, ShakedownError> {
        MysqlHelpers::new(self)
    }

    /// Oracle MySQL helpers scoped to the binding registered under `name`.
    #[cfg(feature = "mysql")]
    pub fn mysql_named(&self, name: &str) -> Result<MysqlHelpers<'_>, ShakedownError> {
        MysqlHelpers::named(self, name)
    }

    /// SQLite execute/query helpers (`sqlite` feature).
    #[cfg(feature = "sqlite")]
    pub fn sqlite(&self) -> Result<SqliteHelpers<'_>, ShakedownError> {
        SqliteHelpers::new(self)
    }

    /// SQLite helpers scoped to the binding registered under `name`.
    #[cfg(feature = "sqlite")]
    pub fn sqlite_named(&self, name: &str) -> Result<SqliteHelpers<'_>, ShakedownError> {
        SqliteHelpers::named(self, name)
    }

    /// DuckDB execute/query helpers (`duckdb` feature).
    #[cfg(feature = "duckdb")]
    pub fn duckdb(&self) -> Result<DuckDbHelpers<'_>, ShakedownError> {
        DuckDbHelpers::new(self)
    }

    /// DuckDB helpers scoped to the binding registered under `name`.
    #[cfg(feature = "duckdb")]
    pub fn duckdb_named(&self, name: &str) -> Result<DuckDbHelpers<'_>, ShakedownError> {
        DuckDbHelpers::named(self, name)
    }

    /// Redis / Valkey key/value helpers (`redis` and/or `valkey` features).
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn redis(&self) -> Result<RedisHelpers<'_>, ShakedownError> {
        RedisHelpers::new(self)
    }

    /// Redis / Valkey helpers scoped to the binding registered under `name`.
    #[cfg(any(feature = "redis", feature = "valkey"))]
    pub fn redis_named(&self, name: &str) -> Result<RedisHelpers<'_>, ShakedownError> {
        RedisHelpers::named(self, name)
    }

    /// MongoDB document helpers (`mongodb` feature).
    #[cfg(feature = "mongodb")]
    pub fn mongo(&self) -> Result<MongoHelpers<'_>, ShakedownError> {
        MongoHelpers::new(self)
    }

    /// MongoDB helpers scoped to the binding registered under `name`.
    #[cfg(feature = "mongodb")]
    pub fn mongo_named(&self, name: &str) -> Result<MongoHelpers<'_>, ShakedownError> {
        MongoHelpers::named(self, name)
    }

    /// NATS client helpers — publish/subscribe on scenario-namespaced subjects (`nats` feature). The suite broker runs with JetStream for [`crate::outbox::Outbox::assert_nats_published`].
    #[cfg(feature = "nats")]
    pub fn nats(&self) -> Result<NatsHelpers<'_>, ShakedownError> {
        NatsHelpers::new(self)
    }

    /// NATS helpers scoped to the binding registered under `name`.
    #[cfg(feature = "nats")]
    pub fn nats_named(&self, name: &str) -> Result<NatsHelpers<'_>, ShakedownError> {
        NatsHelpers::named(self, name)
    }

    /// Neo4j Cypher helpers (`neo4j` feature).
    #[cfg(feature = "neo4j")]
    pub fn neo4j(&self) -> Result<Neo4jHelpers<'_>, ShakedownError> {
        Neo4jHelpers::new(self)
    }

    /// Neo4j helpers scoped to the binding registered under `name`.
    #[cfg(feature = "neo4j")]
    pub fn neo4j_named(&self, name: &str) -> Result<Neo4jHelpers<'_>, ShakedownError> {
        Neo4jHelpers::named(self, name)
    }

    /// Elasticsearch index/search helpers (`elasticsearch` feature).
    #[cfg(feature = "elasticsearch")]
    pub fn elasticsearch(&self) -> Result<ElasticsearchHelpers<'_>, ShakedownError> {
        ElasticsearchHelpers::new(self)
    }

    /// Elasticsearch helpers scoped to the binding registered under `name`.
    #[cfg(feature = "elasticsearch")]
    pub fn elasticsearch_named(
        &self,
        name: &str,
    ) -> Result<ElasticsearchHelpers<'_>, ShakedownError> {
        ElasticsearchHelpers::named(self, name)
    }

    /// OpenSearch index/search helpers (`opensearch` feature).
    #[cfg(feature = "opensearch")]
    pub fn opensearch(&self) -> Result<OpenSearchHelpers<'_>, ShakedownError> {
        OpenSearchHelpers::new(self)
    }

    /// OpenSearch helpers scoped to the binding registered under `name`.
    #[cfg(feature = "opensearch")]
    pub fn opensearch_named(&self, name: &str) -> Result<OpenSearchHelpers<'_>, ShakedownError> {
        OpenSearchHelpers::named(self, name)
    }

    /// Qdrant vector helpers (`qdrant` feature).
    #[cfg(feature = "qdrant")]
    pub fn qdrant(&self) -> Result<QdrantHelpers<'_>, ShakedownError> {
        QdrantHelpers::new(self)
    }

    /// Qdrant helpers scoped to the binding registered under `name`.
    #[cfg(feature = "qdrant")]
    pub fn qdrant_named(&self, name: &str) -> Result<QdrantHelpers<'_>, ShakedownError> {
        QdrantHelpers::named(self, name)
    }

    /// Weaviate schema/object helpers (`weaviate` feature).
    #[cfg(feature = "weaviate")]
    pub fn weaviate(&self) -> Result<WeaviateHelpers<'_>, ShakedownError> {
        WeaviateHelpers::new(self)
    }

    /// Weaviate helpers scoped to the binding registered under `name`.
    #[cfg(feature = "weaviate")]
    pub fn weaviate_named(&self, name: &str) -> Result<WeaviateHelpers<'_>, ShakedownError> {
        WeaviateHelpers::named(self, name)
    }

    /// InfluxDB 2.x write/query helpers (`influxdb` feature).
    #[cfg(feature = "influxdb")]
    pub fn influxdb(&self) -> Result<InfluxDbHelpers<'_>, ShakedownError> {
        InfluxDbHelpers::new(self)
    }

    /// InfluxDB helpers scoped to the binding registered under `name`.
    #[cfg(feature = "influxdb")]
    pub fn influxdb_named(&self, name: &str) -> Result<InfluxDbHelpers<'_>, ShakedownError> {
        InfluxDbHelpers::named(self, name)
    }

    /// TimescaleDB SQL + hypertable helpers (`timescaledb` feature).
    #[cfg(feature = "timescaledb")]
    pub fn timescaledb(&self) -> Result<TimescaledbHelpers<'_>, ShakedownError> {
        TimescaledbHelpers::new(self)
    }

    /// TimescaleDB helpers scoped to the binding registered under `name`.
    #[cfg(feature = "timescaledb")]
    pub fn timescaledb_named(&self, name: &str) -> Result<TimescaledbHelpers<'_>, ShakedownError> {
        TimescaledbHelpers::named(self, name)
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
    /// and mock services (service container lifecycle).
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
            crate::service::ServiceReference::new(service_spec),
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

    /// Invoke `visit` for each running HTTP/gRPC mock service base URL (service container env injection).
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

/// Builds a [`Context`] with [`crate::resources::BuiltinResource::Fake`] already registered.
///
/// Convenience for tests that call [`run_scenario`] directly without `#[shakedown::test(resources = [Fake])]`.
pub fn new_context_with_fake_resource() -> Context {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    ctx
}
