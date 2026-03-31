//! Pull policy and env-var binding overrides for service-under-test (`services` feature).
//!
//! Pure configuration; applied when starting containers and building injection maps (US-004, US-007).

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

/// Overrides the default environment variable name for a harness binding when injecting into a service container.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvMapping {
    /// Kafka bootstrap / connection string → custom env var name.
    Kafka(String),
    /// Postgres `DATABASE_URL` (or equivalent) → custom env var name.
    Postgres(String),
    /// MariaDB URL → custom env var name.
    MariaDb(String),
    /// MySQL URL → custom env var name.
    Mysql(String),
    /// RabbitMQ AMQP URL → custom env var name.
    Rabbitmq(String),
    /// Redis or Valkey `redis://` URL (`redis` / `valkey` features) → custom env var name.
    Redis(String),
    /// MongoDB connection URI (`mongodb` feature) → custom env var name.
    Mongo(String),
    /// NATS server URL (`nats` feature) → custom env var name.
    Nats(String),
    /// Neo4j Bolt URL (`neo4j` feature) → custom env var name.
    Neo4j(String),
    /// DuckDB database path string (`duckdb` feature) → custom env var name.
    DuckDb(String),
    /// Named HTTP/gRPC bridge base URL → custom env var name.
    Bridge {
        bridge_name: String,
        env_var: String,
    },
    /// Another registered service base URL → custom env var name.
    Service {
        service_name: String,
        env_var: String,
    },
}

impl EnvMapping {
    /// Map Kafka bindings to `environment_variable_name`.
    pub fn kafka(environment_variable_name: impl Into<String>) -> Self {
        Self::Kafka(environment_variable_name.into())
    }

    /// Map Postgres bindings to `environment_variable_name`.
    pub fn postgres(environment_variable_name: impl Into<String>) -> Self {
        Self::Postgres(environment_variable_name.into())
    }

    /// Map MariaDB bindings to `environment_variable_name`.
    pub fn mariadb(environment_variable_name: impl Into<String>) -> Self {
        Self::MariaDb(environment_variable_name.into())
    }

    /// Map MySQL bindings to `environment_variable_name`.
    pub fn mysql(environment_variable_name: impl Into<String>) -> Self {
        Self::Mysql(environment_variable_name.into())
    }

    /// Map RabbitMQ bindings to `environment_variable_name`.
    pub fn rabbitmq(environment_variable_name: impl Into<String>) -> Self {
        Self::Rabbitmq(environment_variable_name.into())
    }

    /// Map Redis / Valkey bindings to `environment_variable_name`.
    pub fn redis(environment_variable_name: impl Into<String>) -> Self {
        Self::Redis(environment_variable_name.into())
    }

    /// Map MongoDB bindings to `environment_variable_name`.
    pub fn mongo(environment_variable_name: impl Into<String>) -> Self {
        Self::Mongo(environment_variable_name.into())
    }

    /// Map NATS bindings to `environment_variable_name`.
    pub fn nats(environment_variable_name: impl Into<String>) -> Self {
        Self::Nats(environment_variable_name.into())
    }

    /// Map Neo4j Bolt URL bindings to `environment_variable_name`.
    pub fn neo4j(environment_variable_name: impl Into<String>) -> Self {
        Self::Neo4j(environment_variable_name.into())
    }

    /// Map DuckDB bindings to `environment_variable_name`.
    pub fn duckdb(environment_variable_name: impl Into<String>) -> Self {
        Self::DuckDb(environment_variable_name.into())
    }

    /// Map the bridge named `bridge_name` to `environment_variable_name`.
    pub fn bridge(
        bridge_name: impl Into<String>,
        environment_variable_name: impl Into<String>,
    ) -> Self {
        Self::Bridge {
            bridge_name: bridge_name.into(),
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
        assert_eq!(mapping, EnvMapping::Kafka("CUSTOM_KAFKA".to_string()));
    }

    #[test]
    fn env_mapping_postgres() {
        let mapping = EnvMapping::postgres("PG_URL");
        assert_eq!(mapping, EnvMapping::Postgres("PG_URL".to_string()));
    }

    #[test]
    fn env_mapping_mariadb() {
        // Given mariadb rename factory
        let mapping = EnvMapping::mariadb("MARIADB_JDBC");
        assert_eq!(mapping, EnvMapping::MariaDb("MARIADB_JDBC".to_string()));
    }

    #[test]
    fn env_mapping_mysql() {
        let mapping = EnvMapping::mysql("MYSQL_DSN");
        assert_eq!(mapping, EnvMapping::Mysql("MYSQL_DSN".to_string()));
    }

    #[test]
    fn env_mapping_rabbitmq() {
        let mapping = EnvMapping::rabbitmq("AMQP_URI");
        assert_eq!(mapping, EnvMapping::Rabbitmq("AMQP_URI".to_string()));
    }

    #[test]
    fn env_mapping_redis() {
        let mapping = EnvMapping::redis("CACHE_URL");
        assert_eq!(mapping, EnvMapping::Redis("CACHE_URL".to_string()));
    }

    #[test]
    fn env_mapping_mongo() {
        let mapping = EnvMapping::mongo("MONGO_DSN");
        assert_eq!(mapping, EnvMapping::Mongo("MONGO_DSN".to_string()));
    }

    #[test]
    fn env_mapping_nats() {
        let mapping = EnvMapping::nats("MSG_BUS_URL");
        assert_eq!(mapping, EnvMapping::Nats("MSG_BUS_URL".to_string()));
    }

    #[test]
    fn env_mapping_neo4j() {
        let mapping = EnvMapping::neo4j("GRAPH_URI");
        assert_eq!(mapping, EnvMapping::Neo4j("GRAPH_URI".to_string()));
    }

    #[test]
    fn env_mapping_duckdb() {
        let mapping = EnvMapping::duckdb("ANALYTICS_DUCK");
        assert_eq!(mapping, EnvMapping::DuckDb("ANALYTICS_DUCK".to_string()));
    }

    #[test]
    fn env_mapping_bridge() {
        let mapping = EnvMapping::bridge("payments", "PAYMENTS_STUB_URL");
        assert_eq!(
            mapping,
            EnvMapping::Bridge {
                bridge_name: "payments".to_string(),
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
}
