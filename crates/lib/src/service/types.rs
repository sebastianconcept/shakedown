//! Pull policy and env-var binding overrides for service-under-test (`services` feature).
//!
//! Pure configuration; applied when starting containers and building injection maps.

use std::collections::HashMap;

use super::health::HealthCheck;

/// Container port exposure for a service-under-test (optional logical name for multi-port services).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortMapping {
    pub(crate) name: Option<String>,
    pub(crate) container_port: u16,
}

/// Validated description of a service container to run in a scenario (built via [`crate::service::ServiceUnderTest`]).
///
/// Fields are crate-private; callers pass [`ServiceSpec`] to the harness — configuration is opaque at the crate boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceSpec {
    pub(crate) name: String,
    pub(crate) image: String,
    pub(crate) ports: Vec<PortMapping>,
    pub(crate) env_vars: HashMap<String, String>,
    pub(crate) env_mappings: Vec<EnvMapping>,
    pub(crate) health_check: HealthCheck,
    pub(crate) depends_on: Vec<String>,
    pub(crate) auto_inject: bool,
    pub(crate) pull_policy: PullPolicy,
}

impl ServiceSpec {
    /// Logical service name (URLs, labels, dependencies).
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

/// How Docker should pull the service image before start.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum PullPolicy {
    /// Always attempt to pull the image (equivalent to Docker `--pull always`).
    Always,
    /// Pull only if missing locally (Docker default; D-brief-12).
    #[default]
    IfNotPresent,
    /// Never pull; use only local image (fail if absent).
    Never,
}

/// Overrides the default environment variable name for the Shakedown binding when injecting into a service container.
///
/// ## Which mapping do I need?
///
/// | Your service reads… | Use |
/// |---|---|
/// | Kafka bootstrap / brokers from env | [`EnvMapping::kafka`] |
/// | A topic name from env (scenario-namespaced) | [`EnvMapping::kafka_topic`] |
/// | A consumer group id from env (scenario-namespaced) | [`EnvMapping::kafka_consumer_group`] |
/// | Postgres URL from env | [`EnvMapping::postgres`] |
/// | MariaDB URL from env | [`EnvMapping::mariadb`] |
/// | MySQL URL from env | [`EnvMapping::mysql`] |
/// | RabbitMQ AMQP URL from env | [`EnvMapping::rabbitmq`] |
/// | Redis / Valkey URL from env | [`EnvMapping::redis`] |
/// | A registered HTTP mock’s base URL from env | [`EnvMapping::mock_service`] |
/// | Another registered service’s base URL from env | [`EnvMapping::service`] |
/// | A named port on another service from env | [`EnvMapping::service_named_port`] |
///
/// Optional engines (MongoDB, NATS, Neo4j, Elasticsearch, …) expose matching variants when the
/// corresponding `shakedown` Cargo features are enabled — same idea: map harness bindings into the
/// env var names your binary already reads.
///
/// If your service uses the default env var names Shakedown already injects (for example
/// `DATABASE_URL`, `KAFKA_BOOTSTRAP_SERVERS`), you do not need [`EnvMapping`] — convention injection
/// covers it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum EnvMapping {
    /// Kafka bootstrap / connection string → custom env var name.
    Kafka {
        env_var: String,
        binding_name: Option<String>,
    },
    /// Scenario-namespaced Kafka topic for a logical name ([`crate::KafkaBindings::topic_namespace`]) → env var.
    ///
    /// Use when a service subscribes to a logical topic (e.g. `portfolio`) that Shakedown prefixes per scenario.
    KafkaTopic {
        env_var: String,
        logical_topic: String,
        binding_name: Option<String>,
    },
    /// Scenario-namespaced Kafka consumer group ([`crate::KafkaBindings::consumer_group_namespace`]) → env var.
    KafkaConsumerGroup {
        env_var: String,
        logical_group_id: String,
        binding_name: Option<String>,
    },
    /// Postgres `DATABASE_URL` (or equivalent) → custom env var name.
    Postgres {
        env_var: String,
        binding_name: Option<String>,
    },
    /// MariaDB URL → custom env var name.
    MariaDb {
        env_var: String,
        binding_name: Option<String>,
    },
    /// MySQL URL → custom env var name.
    Mysql {
        env_var: String,
        binding_name: Option<String>,
    },
    /// RabbitMQ AMQP URL → custom env var name.
    Rabbitmq {
        env_var: String,
        binding_name: Option<String>,
    },
    /// Redis or Valkey `redis://` URL (`redis` / `valkey` features) → custom env var name.
    Redis {
        env_var: String,
        binding_name: Option<String>,
    },
    /// Redis Sentinel discovery: comma-separated `host:port` list and logical master name (`redis-sentinel` + `services`).
    #[cfg(feature = "redis-sentinel")]
    RedisSentinel {
        sentinel_hosts_var: String,
        master_name_var: String,
        binding_name: Option<String>,
    },
    /// MongoDB connection URI (`mongodb` feature) → custom env var name.
    Mongo {
        env_var: String,
        binding_name: Option<String>,
    },
    /// NATS server URL (`nats` feature) → custom env var name.
    Nats {
        env_var: String,
        binding_name: Option<String>,
    },
    /// Neo4j Bolt URL (`neo4j` feature) → custom env var name.
    Neo4j {
        env_var: String,
        binding_name: Option<String>,
    },
    /// DuckDB database path string (`duckdb` feature) → custom env var name.
    DuckDb {
        env_var: String,
        binding_name: Option<String>,
    },
    /// Elasticsearch HTTP API base URL (`elasticsearch` feature) → custom env var name.
    Elasticsearch {
        env_var: String,
        binding_name: Option<String>,
    },
    /// OpenSearch HTTP API base URL (`opensearch` feature) → custom env var name.
    OpenSearch {
        env_var: String,
        binding_name: Option<String>,
    },
    /// Qdrant gRPC URL (`qdrant` feature) → custom env var name.
    Qdrant {
        env_var: String,
        binding_name: Option<String>,
    },
    /// Weaviate REST base URL (`weaviate` feature) → custom env var name.
    Weaviate {
        env_var: String,
        binding_name: Option<String>,
    },
    /// InfluxDB 2.x HTTP API base URL (`influxdb` feature) → custom env var name.
    InfluxDb {
        env_var: String,
        binding_name: Option<String>,
    },
    /// TimescaleDB libpq URL (`timescaledb` feature) → custom env var name (default `TIMESCALEDB_URL`).
    Timescaledb {
        env_var: String,
        binding_name: Option<String>,
    },
    /// Named HTTP/gRPC mock service base URL → custom env var name.
    MockService {
        mock_service_name: String,
        env_var: String,
    },
    /// Another registered service base URL → custom env var name.
    Service {
        service_name: String,
        env_var: String,
    },
    /// Registered service URL for a [`crate::service::ServiceUnderTest::port_named`] port → custom env var name.
    ///
    /// Resolved via [`crate::lifecycle::Context::service_handle`] after the dependency container is running
    /// (same ordering rules as [`EnvMapping::service`]).
    ServiceNamedPort {
        service_name: String,
        port_name: String,
        env_var: String,
    },
}

impl EnvMapping {
    /// Map Kafka bindings to `environment_variable_name`.
    pub fn kafka(environment_variable_name: impl Into<String>) -> Self {
        Self::Kafka {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map the scenario-namespaced Kafka topic for `logical_topic` to `environment_variable_name`.
    pub fn kafka_topic(
        environment_variable_name: impl Into<String>,
        logical_topic: impl Into<String>,
    ) -> Self {
        Self::KafkaTopic {
            env_var: environment_variable_name.into(),
            logical_topic: logical_topic.into(),
            binding_name: None,
        }
    }

    /// Map the scenario-namespaced consumer group for `logical_group_id` to `environment_variable_name`.
    pub fn kafka_consumer_group(
        environment_variable_name: impl Into<String>,
        logical_group_id: impl Into<String>,
    ) -> Self {
        Self::KafkaConsumerGroup {
            env_var: environment_variable_name.into(),
            logical_group_id: logical_group_id.into(),
            binding_name: None,
        }
    }

    /// Map Postgres bindings to `environment_variable_name`.
    pub fn postgres(environment_variable_name: impl Into<String>) -> Self {
        Self::Postgres {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map MariaDB bindings to `environment_variable_name`.
    pub fn mariadb(environment_variable_name: impl Into<String>) -> Self {
        Self::MariaDb {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map MySQL bindings to `environment_variable_name`.
    pub fn mysql(environment_variable_name: impl Into<String>) -> Self {
        Self::Mysql {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map RabbitMQ bindings to `environment_variable_name`.
    pub fn rabbitmq(environment_variable_name: impl Into<String>) -> Self {
        Self::Rabbitmq {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map Redis / Valkey bindings to `environment_variable_name`.
    pub fn redis(environment_variable_name: impl Into<String>) -> Self {
        Self::Redis {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map Redis Sentinel bindings to custom env var names (defaults **`REDIS_SENTINEL_HOSTS`** and **`REDIS_SENTINEL_MASTER_NAME`**).
    #[cfg(feature = "redis-sentinel")]
    pub fn redis_sentinel(
        sentinel_hosts_environment_variable: impl Into<String>,
        master_name_environment_variable: impl Into<String>,
    ) -> Self {
        Self::RedisSentinel {
            sentinel_hosts_var: sentinel_hosts_environment_variable.into(),
            master_name_var: master_name_environment_variable.into(),
            binding_name: None,
        }
    }

    /// Map MongoDB bindings to `environment_variable_name`.
    pub fn mongo(environment_variable_name: impl Into<String>) -> Self {
        Self::Mongo {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map NATS bindings to `environment_variable_name`.
    pub fn nats(environment_variable_name: impl Into<String>) -> Self {
        Self::Nats {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map Neo4j Bolt URL bindings to `environment_variable_name`.
    pub fn neo4j(environment_variable_name: impl Into<String>) -> Self {
        Self::Neo4j {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map DuckDB bindings to `environment_variable_name`.
    pub fn duckdb(environment_variable_name: impl Into<String>) -> Self {
        Self::DuckDb {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map Elasticsearch HTTP URL bindings to `environment_variable_name`.
    pub fn elasticsearch(environment_variable_name: impl Into<String>) -> Self {
        Self::Elasticsearch {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map OpenSearch HTTP URL bindings to `environment_variable_name`.
    pub fn opensearch(environment_variable_name: impl Into<String>) -> Self {
        Self::OpenSearch {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map Qdrant gRPC URL bindings to `environment_variable_name`.
    pub fn qdrant(environment_variable_name: impl Into<String>) -> Self {
        Self::Qdrant {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map Weaviate REST URL bindings to `environment_variable_name`.
    pub fn weaviate(environment_variable_name: impl Into<String>) -> Self {
        Self::Weaviate {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map InfluxDB HTTP API URL bindings to `environment_variable_name`.
    pub fn influxdb(environment_variable_name: impl Into<String>) -> Self {
        Self::InfluxDb {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Map TimescaleDB connection URL bindings to `environment_variable_name`.
    pub fn timescaledb(environment_variable_name: impl Into<String>) -> Self {
        Self::Timescaledb {
            env_var: environment_variable_name.into(),
            binding_name: None,
        }
    }

    /// Select a named harness binding ([`crate::lifecycle::Context::kafka_bindings_named`], etc.).
    ///
    /// When omitted, [`crate::lifecycle::DEFAULT_RESOURCE_BINDING_NAME`] is used.
    pub fn binding(mut self, name: impl Into<String>) -> Self {
        let resolved = Some(name.into());
        match &mut self {
            EnvMapping::Kafka { binding_name, .. }
            | EnvMapping::KafkaTopic { binding_name, .. }
            | EnvMapping::KafkaConsumerGroup { binding_name, .. }
            | EnvMapping::Postgres { binding_name, .. }
            | EnvMapping::MariaDb { binding_name, .. }
            | EnvMapping::Mysql { binding_name, .. }
            | EnvMapping::Rabbitmq { binding_name, .. }
            | EnvMapping::Redis { binding_name, .. }
            | EnvMapping::Mongo { binding_name, .. }
            | EnvMapping::Nats { binding_name, .. }
            | EnvMapping::Neo4j { binding_name, .. }
            | EnvMapping::DuckDb { binding_name, .. }
            | EnvMapping::Elasticsearch { binding_name, .. }
            | EnvMapping::OpenSearch { binding_name, .. }
            | EnvMapping::Qdrant { binding_name, .. }
            | EnvMapping::Weaviate { binding_name, .. }
            | EnvMapping::InfluxDb { binding_name, .. }
            | EnvMapping::Timescaledb { binding_name, .. } => {
                *binding_name = resolved;
            }
            #[cfg(feature = "redis-sentinel")]
            EnvMapping::RedisSentinel { binding_name, .. } => {
                *binding_name = resolved;
            }
            EnvMapping::MockService { .. }
            | EnvMapping::Service { .. }
            | EnvMapping::ServiceNamedPort { .. } => {}
        }
        self
    }

    /// Map the mock service named `mock_service_name` to `environment_variable_name`.
    pub fn mock_service(
        mock_service_name: impl Into<String>,
        environment_variable_name: impl Into<String>,
    ) -> Self {
        Self::MockService {
            mock_service_name: mock_service_name.into(),
            env_var: environment_variable_name.into(),
        }
    }

    /// Map the service named `service_name` to `environment_variable_name`.
    pub fn service(
        service_name: impl Into<String>,
        environment_variable_name: impl Into<String>,
    ) -> Self {
        Self::Service {
            service_name: service_name.into(),
            env_var: environment_variable_name.into(),
        }
    }

    /// Map a named port on the service named `service_name` to `environment_variable_name`.
    pub fn service_named_port(
        service_name: impl Into<String>,
        port_name: impl Into<String>,
        environment_variable_name: impl Into<String>,
    ) -> Self {
        Self::ServiceNamedPort {
            service_name: service_name.into(),
            port_name: port_name.into(),
            env_var: environment_variable_name.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pull_policy_default_matches_docker_if_not_present() {
        // Given PullPolicy::default
        // Then it matches IfNotPresent (D-brief-12)
        assert_eq!(PullPolicy::default(), PullPolicy::IfNotPresent);
    }

    #[test]
    fn pull_policy_variants_distinct() {
        // Given the three pull policy variants
        // Then they are pairwise distinct
        assert_ne!(PullPolicy::Always, PullPolicy::IfNotPresent);
        assert_ne!(PullPolicy::IfNotPresent, PullPolicy::Never);
        assert_ne!(PullPolicy::Always, PullPolicy::Never);
    }

    #[test]
    fn env_mapping_kafka() {
        // Given kafka("CUSTOM_KAFKA")
        let mapping = EnvMapping::kafka("CUSTOM_KAFKA");
        // Then the enum holds the expected variant
        assert_eq!(
            mapping,
            EnvMapping::Kafka {
                env_var: "CUSTOM_KAFKA".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_mariadb_binding_sets_name() {
        let mapping = EnvMapping::mariadb("ORDERS_DB_URL").binding("orders");
        assert_eq!(
            mapping,
            EnvMapping::MariaDb {
                env_var: "ORDERS_DB_URL".to_string(),
                binding_name: Some("orders".to_string()),
            }
        );
    }

    #[test]
    fn env_mapping_postgres() {
        let mapping = EnvMapping::postgres("PG_URL");
        assert_eq!(
            mapping,
            EnvMapping::Postgres {
                env_var: "PG_URL".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_mariadb() {
        // Given mariadb rename factory
        let mapping = EnvMapping::mariadb("MARIADB_JDBC");
        assert_eq!(
            mapping,
            EnvMapping::MariaDb {
                env_var: "MARIADB_JDBC".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_mysql() {
        let mapping = EnvMapping::mysql("MYSQL_DSN");
        assert_eq!(
            mapping,
            EnvMapping::Mysql {
                env_var: "MYSQL_DSN".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_rabbitmq() {
        let mapping = EnvMapping::rabbitmq("AMQP_URI");
        assert_eq!(
            mapping,
            EnvMapping::Rabbitmq {
                env_var: "AMQP_URI".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_redis() {
        let mapping = EnvMapping::redis("CACHE_URL");
        assert_eq!(
            mapping,
            EnvMapping::Redis {
                env_var: "CACHE_URL".to_string(),
                binding_name: None,
            }
        );
    }

    #[cfg(feature = "redis-sentinel")]
    #[test]
    fn env_mapping_redis_sentinel() {
        let mapping = EnvMapping::redis_sentinel("S_HOSTS", "S_MASTER");
        assert_eq!(
            mapping,
            EnvMapping::RedisSentinel {
                sentinel_hosts_var: "S_HOSTS".to_string(),
                master_name_var: "S_MASTER".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_mongo() {
        let mapping = EnvMapping::mongo("MONGO_DSN");
        assert_eq!(
            mapping,
            EnvMapping::Mongo {
                env_var: "MONGO_DSN".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_nats() {
        let mapping = EnvMapping::nats("MSG_BUS_URL");
        assert_eq!(
            mapping,
            EnvMapping::Nats {
                env_var: "MSG_BUS_URL".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_neo4j() {
        let mapping = EnvMapping::neo4j("GRAPH_URI");
        assert_eq!(
            mapping,
            EnvMapping::Neo4j {
                env_var: "GRAPH_URI".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_duckdb() {
        let mapping = EnvMapping::duckdb("ANALYTICS_DUCK");
        assert_eq!(
            mapping,
            EnvMapping::DuckDb {
                env_var: "ANALYTICS_DUCK".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_elasticsearch() {
        let mapping = EnvMapping::elasticsearch("ES_URL");
        assert_eq!(
            mapping,
            EnvMapping::Elasticsearch {
                env_var: "ES_URL".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_opensearch() {
        let mapping = EnvMapping::opensearch("OS_URL");
        assert_eq!(
            mapping,
            EnvMapping::OpenSearch {
                env_var: "OS_URL".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_qdrant() {
        let mapping = EnvMapping::qdrant("VECTOR_DB_GRPC");
        assert_eq!(
            mapping,
            EnvMapping::Qdrant {
                env_var: "VECTOR_DB_GRPC".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_weaviate() {
        let mapping = EnvMapping::weaviate("VECTOR_SEARCH_URL");
        assert_eq!(
            mapping,
            EnvMapping::Weaviate {
                env_var: "VECTOR_SEARCH_URL".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_influxdb() {
        let mapping = EnvMapping::influxdb("TSDB_URL");
        assert_eq!(
            mapping,
            EnvMapping::InfluxDb {
                env_var: "TSDB_URL".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_timescaledb() {
        let mapping = EnvMapping::timescaledb("TS_URL");
        assert_eq!(
            mapping,
            EnvMapping::Timescaledb {
                env_var: "TS_URL".to_string(),
                binding_name: None,
            }
        );
    }

    #[test]
    fn env_mapping_mock_service() {
        let mapping = EnvMapping::mock_service("payments", "PAYMENTS_STUB_URL");
        assert_eq!(
            mapping,
            EnvMapping::MockService {
                mock_service_name: "payments".to_string(),
                env_var: "PAYMENTS_STUB_URL".to_string(),
            }
        );
    }

    #[test]
    fn env_mapping_service() {
        let mapping = EnvMapping::service("orders", "ORDERS_SERVICE_URL");
        assert_eq!(
            mapping,
            EnvMapping::Service {
                service_name: "orders".to_string(),
                env_var: "ORDERS_SERVICE_URL".to_string(),
            }
        );
    }

    #[test]
    fn env_mapping_service_named_port() {
        let mapping =
            EnvMapping::service_named_port("fulfillment-service", "grpc", "FULFILLMENT_GRPC_URI");
        assert_eq!(
            mapping,
            EnvMapping::ServiceNamedPort {
                service_name: "fulfillment-service".to_string(),
                port_name: "grpc".to_string(),
                env_var: "FULFILLMENT_GRPC_URI".to_string(),
            }
        );
    }
}
