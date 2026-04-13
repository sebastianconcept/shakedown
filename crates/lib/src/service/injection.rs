//! Convention-based environment variable injection for service containers (`services` feature).
//!
//! Pure function from [`crate::lifecycle::Context`] + [`crate::service::ServiceSpec`] → env map.
//! All values are rewritten for service containers via
//! [`crate::service::address::rewrite_injected_addresses_for_service_container`].
//!
//! `inject_service_env` is consumed by service container lifecycle (`run_scenario`).

use std::collections::HashMap;

use crate::lifecycle::{Context, DEFAULT_RESOURCE_BINDING_NAME};
use crate::resources::KafkaBindings;
use crate::service::address::rewrite_injected_addresses_for_service_container;
use crate::service::types::{EnvMapping, ServiceSpec};

fn binding_lookup_key(binding_name: &Option<String>) -> &str {
    binding_name
        .as_deref()
        .unwrap_or(DEFAULT_RESOURCE_BINDING_NAME)
}

/// Build the effective environment map for a service container: convention injection,
/// [`EnvMapping`] renames, then explicit `.env()` entries from the builder (highest precedence).
pub(crate) fn inject_service_env(
    context: &Context,
    service_spec: &ServiceSpec,
) -> HashMap<String, String> {
    let mut environment = HashMap::new();
    let overrides = MappingOverrides::collect(service_spec.env_mappings.as_slice());

    if service_spec.auto_inject {
        inject_conventions(context, service_spec, &overrides, &mut environment);
    }
    // Apply explicit `env_mappings` (including [`EnvMapping::ServiceNamedPort`]) even when
    // `auto_inject` is true — conventions alone do not resolve named service ports.
    inject_only_env_mappings(context, service_spec, &mut environment);

    for (key, value) in &service_spec.env_vars {
        environment.insert(key.clone(), value.clone());
    }

    for value in environment.values_mut() {
        *value = rewrite_injected_addresses_for_service_container(value.as_str());
    }

    environment
}

struct MappingOverrides {
    kafka: Option<String>,
    postgres: Option<String>,
    mariadb: Option<String>,
    mysql: Option<String>,
    rabbitmq: Option<String>,
    redis: Option<String>,
    mongo: Option<String>,
    nats: Option<String>,
    neo4j: Option<String>,
    duckdb: Option<String>,
    elasticsearch: Option<String>,
    opensearch: Option<String>,
    qdrant: Option<String>,
    weaviate: Option<String>,
    influxdb: Option<String>,
    timescaledb: Option<String>,
    mock_services: HashMap<String, String>,
    services: HashMap<String, String>,
    #[cfg(feature = "redis-sentinel")]
    redis_sentinel_hosts: Option<String>,
    #[cfg(feature = "redis-sentinel")]
    redis_sentinel_master: Option<String>,
}

impl MappingOverrides {
    fn collect(mappings: &[EnvMapping]) -> Self {
        let mut overrides = MappingOverrides {
            kafka: None,
            postgres: None,
            mariadb: None,
            mysql: None,
            rabbitmq: None,
            redis: None,
            mongo: None,
            nats: None,
            neo4j: None,
            duckdb: None,
            elasticsearch: None,
            opensearch: None,
            qdrant: None,
            weaviate: None,
            influxdb: None,
            timescaledb: None,
            mock_services: HashMap::new(),
            services: HashMap::new(),
            #[cfg(feature = "redis-sentinel")]
            redis_sentinel_hosts: None,
            #[cfg(feature = "redis-sentinel")]
            redis_sentinel_master: None,
        };
        for mapping in mappings {
            match mapping {
                EnvMapping::Kafka { env_var, .. } => overrides.kafka = Some(env_var.clone()),
                EnvMapping::KafkaTopic { .. } => {}
                EnvMapping::KafkaConsumerGroup { .. } => {}
                EnvMapping::Postgres { env_var, .. } => overrides.postgres = Some(env_var.clone()),
                EnvMapping::MariaDb { env_var, .. } => overrides.mariadb = Some(env_var.clone()),
                EnvMapping::Mysql { env_var, .. } => overrides.mysql = Some(env_var.clone()),
                EnvMapping::Rabbitmq { env_var, .. } => overrides.rabbitmq = Some(env_var.clone()),
                EnvMapping::Redis { env_var, .. } => overrides.redis = Some(env_var.clone()),
                #[cfg(feature = "redis-sentinel")]
                EnvMapping::RedisSentinel {
                    sentinel_hosts_var,
                    master_name_var,
                    ..
                } => {
                    overrides.redis_sentinel_hosts = Some(sentinel_hosts_var.clone());
                    overrides.redis_sentinel_master = Some(master_name_var.clone());
                }
                EnvMapping::Mongo { env_var, .. } => overrides.mongo = Some(env_var.clone()),
                EnvMapping::Nats { env_var, .. } => overrides.nats = Some(env_var.clone()),
                EnvMapping::Neo4j { env_var, .. } => overrides.neo4j = Some(env_var.clone()),
                EnvMapping::DuckDb { env_var, .. } => overrides.duckdb = Some(env_var.clone()),
                EnvMapping::Elasticsearch { env_var, .. } => {
                    overrides.elasticsearch = Some(env_var.clone());
                }
                EnvMapping::OpenSearch { env_var, .. } => {
                    overrides.opensearch = Some(env_var.clone())
                }
                EnvMapping::Qdrant { env_var, .. } => overrides.qdrant = Some(env_var.clone()),
                EnvMapping::Weaviate { env_var, .. } => overrides.weaviate = Some(env_var.clone()),
                EnvMapping::InfluxDb { env_var, .. } => overrides.influxdb = Some(env_var.clone()),
                EnvMapping::Timescaledb { env_var, .. } => {
                    overrides.timescaledb = Some(env_var.clone());
                }
                EnvMapping::MockService {
                    mock_service_name,
                    env_var,
                } => {
                    overrides
                        .mock_services
                        .insert(mock_service_name.clone(), env_var.clone());
                }
                EnvMapping::Service {
                    service_name,
                    env_var,
                } => {
                    overrides
                        .services
                        .insert(service_name.clone(), env_var.clone());
                }
                EnvMapping::ServiceNamedPort { .. } => {}
            }
        }
        overrides
    }
}

fn inject_conventions(
    context: &Context,
    service_spec: &ServiceSpec,
    overrides: &MappingOverrides,
    environment: &mut HashMap<String, String>,
) {
    #[cfg(any(
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "mongodb",
        feature = "timescaledb",
    ))]
    let partition_relational = context.registered_service_count() > 1;

    if let Some(bindings) = context.kafka_bindings() {
        let key = overrides
            .kafka
            .as_deref()
            .unwrap_or("KAFKA_BOOTSTRAP_SERVERS");
        environment.insert(key.to_string(), kafka_bootstrap_value(bindings));
    }

    #[cfg(feature = "postgres")]
    if let Some(bindings) = context.postgres_bindings() {
        let key = overrides.postgres.as_deref().unwrap_or("DATABASE_URL");
        let url = relational_url_for_service(
            bindings.connection_url.as_str(),
            bindings.database.as_str(),
            service_spec.name(),
            partition_relational,
        );
        environment.insert(key.to_string(), url);
    }

    #[cfg(feature = "mariadb")]
    if let Some(bindings) = context.mariadb_bindings() {
        let key = overrides.mariadb.as_deref().unwrap_or("MARIADB_URL");
        let url = relational_url_for_service(
            bindings.connection_url.as_str(),
            bindings.database.as_str(),
            service_spec.name(),
            partition_relational,
        );
        environment.insert(key.to_string(), url);
    }

    #[cfg(feature = "mysql")]
    if let Some(bindings) = context.mysql_bindings() {
        let key = overrides.mysql.as_deref().unwrap_or("MYSQL_URL");
        let url = relational_url_for_service(
            bindings.connection_url.as_str(),
            bindings.database.as_str(),
            service_spec.name(),
            partition_relational,
        );
        environment.insert(key.to_string(), url);
    }

    #[cfg(feature = "rabbitmq")]
    if let Some(bindings) = context.rabbit_bindings() {
        let key = overrides.rabbitmq.as_deref().unwrap_or("RABBITMQ_URL");
        environment.insert(key.to_string(), bindings.connection_url.clone());
    }

    #[cfg(any(feature = "redis", feature = "valkey"))]
    if let Some(bindings) = context.redis_bindings() {
        let key = overrides.redis.as_deref().unwrap_or("REDIS_URL");
        environment.insert(key.to_string(), bindings.connection_url.clone());
    }

    #[cfg(feature = "redis-sentinel")]
    if let Some(bindings) = context.redis_sentinel_bindings() {
        let hosts_key = overrides
            .redis_sentinel_hosts
            .as_deref()
            .unwrap_or("REDIS_SENTINEL_HOSTS");
        let master_key = overrides
            .redis_sentinel_master
            .as_deref()
            .unwrap_or("REDIS_SENTINEL_MASTER_NAME");
        environment.insert(hosts_key.to_string(), bindings.sentinel_hosts.clone());
        environment.insert(master_key.to_string(), bindings.master_name.clone());
    }

    #[cfg(feature = "mongodb")]
    if let Some(bindings) = context.mongo_bindings() {
        let key = overrides.mongo.as_deref().unwrap_or("MONGODB_URI");
        let url = relational_url_for_service(
            bindings.connection_url.as_str(),
            bindings.database.as_str(),
            service_spec.name(),
            partition_relational,
        );
        environment.insert(key.to_string(), url);
    }

    #[cfg(feature = "nats")]
    if let Some(bindings) = context.nats_bindings() {
        let key = overrides.nats.as_deref().unwrap_or("NATS_URL");
        environment.insert(key.to_string(), bindings.connection_url.clone());
    }

    #[cfg(feature = "neo4j")]
    if let Some(bindings) = context.neo4j_bindings() {
        let key = overrides.neo4j.as_deref().unwrap_or("NEO4J_URI");
        environment.insert(key.to_string(), bindings.bolt_url.clone());
    }

    #[cfg(feature = "duckdb")]
    if let Some(bindings) = context.duckdb_bindings() {
        let key = overrides.duckdb.as_deref().unwrap_or("DUCKDB_URL");
        environment.insert(key.to_string(), bindings.database_url.clone());
    }

    #[cfg(feature = "elasticsearch")]
    if let Some(bindings) = context.elasticsearch_bindings() {
        let key = overrides
            .elasticsearch
            .as_deref()
            .unwrap_or("ELASTICSEARCH_URL");
        environment.insert(key.to_string(), bindings.url.clone());
    }

    #[cfg(feature = "opensearch")]
    if let Some(bindings) = context.opensearch_bindings() {
        let key = overrides.opensearch.as_deref().unwrap_or("OPENSEARCH_URL");
        environment.insert(key.to_string(), bindings.url.clone());
    }

    #[cfg(feature = "qdrant")]
    if let Some(bindings) = context.qdrant_bindings() {
        let key = overrides.qdrant.as_deref().unwrap_or("QDRANT_URL");
        environment.insert(key.to_string(), bindings.grpc_url.clone());
    }

    #[cfg(feature = "weaviate")]
    if let Some(bindings) = context.weaviate_bindings() {
        let key = overrides.weaviate.as_deref().unwrap_or("WEAVIATE_URL");
        environment.insert(key.to_string(), bindings.rest_url.clone());
    }

    #[cfg(feature = "influxdb")]
    if let Some(bindings) = context.influxdb_bindings() {
        let key = overrides.influxdb.as_deref().unwrap_or("INFLUXDB_URL");
        environment.insert(key.to_string(), bindings.url.clone());
        environment.insert("INFLUXDB_TOKEN".to_string(), bindings.token.clone());
        environment.insert("INFLUXDB_ORG".to_string(), bindings.organization.clone());
        environment.insert("INFLUXDB_BUCKET".to_string(), bindings.bucket.clone());
    }

    #[cfg(feature = "timescaledb")]
    if let Some(bindings) = context.timescaledb_bindings() {
        let key = overrides
            .timescaledb
            .as_deref()
            .unwrap_or("TIMESCALEDB_URL");
        let url = relational_url_for_service(
            bindings.connection_url.as_str(),
            bindings.database.as_str(),
            service_spec.name(),
            partition_relational,
        );
        environment.insert(key.to_string(), url);
    }

    // MockService URLs: default `{UPPER_NAME}_URL`, optional rename via `EnvMapping::MockService`.
    #[cfg(all(feature = "mock-services", feature = "services"))]
    {
        context.for_each_mock_service_url(|mock_service_name, client_url| {
            let key = overrides
                .mock_services
                .get(mock_service_name)
                .cloned()
                .unwrap_or_else(|| default_mock_service_env_key(mock_service_name));
            environment.insert(key, client_url.to_string());
        });
    }

    // Inter-service URLs: `{UPPER_NAME}_URL` for each `depends_on` target that is already healthy
    // (topological startup order guarantees URLs exist when a dependent container is configured).
    #[cfg(feature = "services")]
    {
        for dependency_name in service_spec.depends_on.iter() {
            let Some(url) = context.service_url(dependency_name.as_str()) else {
                continue;
            };
            let key = overrides
                .services
                .get(dependency_name.as_str())
                .cloned()
                .unwrap_or_else(|| default_service_env_key(dependency_name.as_str()));
            environment.insert(key, url);
        }
    }
}

fn inject_only_env_mappings(
    context: &Context,
    service_spec: &ServiceSpec,
    environment: &mut HashMap<String, String>,
) {
    for mapping in &service_spec.env_mappings {
        #[cfg(feature = "redis-sentinel")]
        if let EnvMapping::RedisSentinel {
            sentinel_hosts_var,
            master_name_var,
            binding_name,
        } = mapping
        {
            let key_lookup = binding_lookup_key(binding_name);
            if let Some(bindings) = context.redis_sentinel_bindings_named(key_lookup) {
                environment.insert(sentinel_hosts_var.clone(), bindings.sentinel_hosts.clone());
                environment.insert(master_name_var.clone(), bindings.master_name.clone());
            }
            continue;
        }
        if let Some((key, value)) = resolve_env_mapping(context, service_spec.name(), mapping) {
            environment.insert(key, value);
        }
    }
}

fn resolve_env_mapping(
    context: &Context,
    service_name: &str,
    mapping: &EnvMapping,
) -> Option<(String, String)> {
    #[cfg(any(
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "mongodb",
        feature = "timescaledb",
    ))]
    let partition_relational = context.registered_service_count() > 1;
    #[cfg(not(any(
        feature = "postgres",
        feature = "mariadb",
        feature = "mysql",
        feature = "mongodb",
        feature = "timescaledb",
    )))]
    let _ = service_name;
    match mapping {
        EnvMapping::Kafka {
            env_var,
            binding_name,
        } => context
            .kafka_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| (env_var.clone(), kafka_bootstrap_value(bindings))),
        EnvMapping::KafkaTopic {
            env_var,
            logical_topic,
            binding_name,
        } => context
            .kafka_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| {
                (
                    env_var.clone(),
                    bindings.topic_namespace(logical_topic.as_str()),
                )
            }),
        EnvMapping::KafkaConsumerGroup {
            env_var,
            logical_group_id,
            binding_name,
        } => context
            .kafka_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| {
                (
                    env_var.clone(),
                    bindings.consumer_group_namespace(logical_group_id.as_str()),
                )
            }),
        #[cfg(feature = "postgres")]
        EnvMapping::Postgres {
            env_var,
            binding_name,
        } => context
            .postgres_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| {
                (
                    env_var.clone(),
                    relational_url_for_service(
                        bindings.connection_url.as_str(),
                        bindings.database.as_str(),
                        service_name,
                        partition_relational,
                    ),
                )
            }),
        #[cfg(not(feature = "postgres"))]
        EnvMapping::Postgres { .. } => None,
        #[cfg(feature = "mariadb")]
        EnvMapping::MariaDb {
            env_var,
            binding_name,
        } => context
            .mariadb_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| {
                (
                    env_var.clone(),
                    relational_url_for_service(
                        bindings.connection_url.as_str(),
                        bindings.database.as_str(),
                        service_name,
                        partition_relational,
                    ),
                )
            }),
        #[cfg(not(feature = "mariadb"))]
        EnvMapping::MariaDb { .. } => None,
        #[cfg(feature = "mysql")]
        EnvMapping::Mysql {
            env_var,
            binding_name,
        } => context
            .mysql_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| {
                (
                    env_var.clone(),
                    relational_url_for_service(
                        bindings.connection_url.as_str(),
                        bindings.database.as_str(),
                        service_name,
                        partition_relational,
                    ),
                )
            }),
        #[cfg(not(feature = "mysql"))]
        EnvMapping::Mysql { .. } => None,
        #[cfg(feature = "rabbitmq")]
        EnvMapping::Rabbitmq {
            env_var,
            binding_name,
        } => context
            .rabbit_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| (env_var.clone(), bindings.connection_url.clone())),
        #[cfg(not(feature = "rabbitmq"))]
        EnvMapping::Rabbitmq { .. } => None,
        #[cfg(any(feature = "redis", feature = "valkey"))]
        EnvMapping::Redis {
            env_var,
            binding_name,
        } => context
            .redis_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| (env_var.clone(), bindings.connection_url.clone())),
        #[cfg(not(any(feature = "redis", feature = "valkey")))]
        EnvMapping::Redis { .. } => None,
        #[cfg(feature = "redis-sentinel")]
        EnvMapping::RedisSentinel { .. } => None,
        #[cfg(feature = "mongodb")]
        EnvMapping::Mongo {
            env_var,
            binding_name,
        } => context
            .mongo_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| {
                (
                    env_var.clone(),
                    relational_url_for_service(
                        bindings.connection_url.as_str(),
                        bindings.database.as_str(),
                        service_name,
                        partition_relational,
                    ),
                )
            }),
        #[cfg(not(feature = "mongodb"))]
        EnvMapping::Mongo { .. } => None,
        #[cfg(feature = "nats")]
        EnvMapping::Nats {
            env_var,
            binding_name,
        } => context
            .nats_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| (env_var.clone(), bindings.connection_url.clone())),
        #[cfg(not(feature = "nats"))]
        EnvMapping::Nats { .. } => None,
        #[cfg(feature = "neo4j")]
        EnvMapping::Neo4j {
            env_var,
            binding_name,
        } => context
            .neo4j_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| (env_var.clone(), bindings.bolt_url.clone())),
        #[cfg(not(feature = "neo4j"))]
        EnvMapping::Neo4j { .. } => None,
        #[cfg(feature = "duckdb")]
        EnvMapping::DuckDb {
            env_var,
            binding_name,
        } => context
            .duckdb_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| (env_var.clone(), bindings.database_url.clone())),
        #[cfg(not(feature = "duckdb"))]
        EnvMapping::DuckDb { .. } => None,
        #[cfg(feature = "elasticsearch")]
        EnvMapping::Elasticsearch {
            env_var,
            binding_name,
        } => context
            .elasticsearch_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| (env_var.clone(), bindings.url.clone())),
        #[cfg(not(feature = "elasticsearch"))]
        EnvMapping::Elasticsearch { .. } => None,
        #[cfg(feature = "opensearch")]
        EnvMapping::OpenSearch {
            env_var,
            binding_name,
        } => context
            .opensearch_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| (env_var.clone(), bindings.url.clone())),
        #[cfg(not(feature = "opensearch"))]
        EnvMapping::OpenSearch { .. } => None,
        #[cfg(feature = "qdrant")]
        EnvMapping::Qdrant {
            env_var,
            binding_name,
        } => context
            .qdrant_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| (env_var.clone(), bindings.grpc_url.clone())),
        #[cfg(not(feature = "qdrant"))]
        EnvMapping::Qdrant { .. } => None,
        #[cfg(feature = "weaviate")]
        EnvMapping::Weaviate {
            env_var,
            binding_name,
        } => context
            .weaviate_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| (env_var.clone(), bindings.rest_url.clone())),
        #[cfg(not(feature = "weaviate"))]
        EnvMapping::Weaviate { .. } => None,
        #[cfg(feature = "influxdb")]
        EnvMapping::InfluxDb {
            env_var,
            binding_name,
        } => context
            .influxdb_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| (env_var.clone(), bindings.url.clone())),
        #[cfg(not(feature = "influxdb"))]
        EnvMapping::InfluxDb { .. } => None,
        #[cfg(feature = "timescaledb")]
        EnvMapping::Timescaledb {
            env_var,
            binding_name,
        } => context
            .timescaledb_bindings_named(binding_lookup_key(binding_name))
            .map(|bindings| {
                (
                    env_var.clone(),
                    relational_url_for_service(
                        bindings.connection_url.as_str(),
                        bindings.database.as_str(),
                        service_name,
                        partition_relational,
                    ),
                )
            }),
        #[cfg(not(feature = "timescaledb"))]
        EnvMapping::Timescaledb { .. } => None,
        EnvMapping::MockService {
            mock_service_name,
            env_var,
        } => {
            #[cfg(all(feature = "mock-services", feature = "services"))]
            {
                context
                    .mock_service_url(mock_service_name.as_str())
                    .map(|url| (env_var.clone(), url))
            }
            #[cfg(not(all(feature = "mock-services", feature = "services")))]
            {
                let _ = mock_service_name;
                let _ = env_var;
                None
            }
        }
        EnvMapping::Service {
            service_name,
            env_var,
        } => context
            .service_url(service_name.as_str())
            .map(|url| (env_var.clone(), url)),
        EnvMapping::ServiceNamedPort {
            service_name,
            port_name,
            env_var,
        } => context
            .service_handle(service_name.as_str())
            .and_then(|handle| handle.port_url(port_name.as_str()))
            .map(|url| (env_var.clone(), url.to_string())),
    }
}

fn kafka_bootstrap_value(bindings: &KafkaBindings) -> String {
    bindings.bootstrap_servers.clone()
}

/// Same database **name** env var (`DATABASE_URL`) for every service; distinct URLs when multiple
/// services share one engine (partition by logical service name in the path).
#[allow(dead_code)] // Call sites are `#[cfg(feature = "postgres"|…)]`; unused in `services`-only builds.
fn relational_url_for_service(
    connection_url: &str,
    base_database: &str,
    service_name: &str,
    partition: bool,
) -> String {
    if !partition {
        return connection_url.to_string();
    }
    let suffix = sanitize_service_suffix(service_name);
    if suffix.is_empty() {
        return connection_url.to_string();
    }
    let new_database = format!("{base_database}_{suffix}");
    replace_database_path(connection_url, base_database, new_database.as_str())
}

#[allow(dead_code)]
fn sanitize_service_suffix(service_name: &str) -> String {
    let trimmed: String = service_name
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    trimmed.trim_matches('_').to_string()
}

#[allow(dead_code)]
fn replace_database_path(connection_url: &str, old_database: &str, new_database: &str) -> String {
    let path_end = connection_url
        .find('?')
        .or_else(|| connection_url.find('#'))
        .unwrap_or(connection_url.len());
    let head = &connection_url[..path_end];
    let tail = &connection_url[path_end..];
    let needle = format!("/{old_database}");
    if let Some(position) = head.rfind(needle.as_str()) {
        let after_needle = position + needle.len();
        if after_needle == head.len() || head.as_bytes().get(after_needle) == Some(&b'?') {
            let mut out = String::with_capacity(connection_url.len() + new_database.len());
            out.push_str(&head[..position]);
            out.push('/');
            out.push_str(new_database);
            out.push_str(tail);
            return out;
        }
    }
    connection_url.to_string()
}

fn default_mock_service_env_key(mock_service_name: &str) -> String {
    format!("{}_URL", upper_snake_case(mock_service_name))
}

fn default_service_env_key(service_name: &str) -> String {
    format!("{}_URL", upper_snake_case(service_name))
}

fn upper_snake_case(name: &str) -> String {
    let tokens: Vec<String> = name
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|segment| !segment.is_empty())
        .map(|segment| segment.to_uppercase())
        .collect();
    if tokens.is_empty() {
        "SERVICE".to_string()
    } else {
        tokens.join("_")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::Context;
    use crate::service::address::rewrite_injected_addresses_for_service_container;
    use crate::service::address::HOST_DOCKER_INTERNAL;
    use crate::service::{ServiceHandle, ServiceUnderTest};

    fn spec_named(name: &str) -> ServiceSpec {
        ServiceUnderTest::docker("alpine:3.20")
            .name(name)
            .port(80)
            .build()
            .unwrap_or_else(|error| panic!("test fixture spec must build: {error:?}"))
    }

    #[test]
    fn kafka_default_injection() {
        // Given Kafka bindings on context and a service spec
        let mut context = Context::new();
        context.set_kafka_bindings(KafkaBindings::new("127.0.0.1:9092", "scenario_"));
        let specification = spec_named("api");
        // When we build the injection map
        let environment = inject_service_env(&context, &specification);
        // Then bootstrap servers use host.docker.internal for the container
        let expected_kafka = format!("{HOST_DOCKER_INTERNAL}:9092");
        assert_eq!(
            environment.get("KAFKA_BOOTSTRAP_SERVERS"),
            Some(&expected_kafka)
        );
    }

    #[test]
    fn env_mapping_renames_kafka() {
        // Given EnvMapping::kafka for a custom variable name
        let mut context = Context::new();
        context.set_kafka_bindings(KafkaBindings::new("localhost:9092", "pfx_"));
        let specification = ServiceUnderTest::docker("alpine:3.20")
            .name("api")
            .port(80)
            .env_mapping(EnvMapping::kafka("CUSTOM_KAFKA"))
            .build()
            .unwrap_or_else(|error| panic!("spec must build: {error:?}"));
        let environment = inject_service_env(&context, &specification);
        assert!(!environment.contains_key("KAFKA_BOOTSTRAP_SERVERS"));
        let expected_custom = format!("{HOST_DOCKER_INTERNAL}:9092");
        assert_eq!(environment.get("CUSTOM_KAFKA"), Some(&expected_custom));
    }

    #[test]
    fn auto_inject_false_only_explicit_and_mapping() {
        // Given auto_inject(false) and only mapping + explicit env
        let mut context = Context::new();
        context.set_kafka_bindings(KafkaBindings::new("127.0.0.1:9092", "t_"));
        let specification = ServiceUnderTest::docker("alpine:3.20")
            .name("api")
            .port(80)
            .auto_inject(false)
            .env_mapping(EnvMapping::kafka("ONLY_KAFKA"))
            .env("EXTRA", "1")
            .build()
            .unwrap_or_else(|error| panic!("spec must build: {error:?}"));
        let environment = inject_service_env(&context, &specification);
        assert!(!environment.contains_key("KAFKA_BOOTSTRAP_SERVERS"));
        let expected_only = format!("{HOST_DOCKER_INTERNAL}:9092");
        assert_eq!(environment.get("ONLY_KAFKA"), Some(&expected_only));
        assert_eq!(environment.get("EXTRA").map(String::as_str), Some("1"));
    }

    #[test]
    fn explicit_env_overrides_convention() {
        // Given .env on the builder for the same key as convention injection
        let mut context = Context::new();
        context.set_kafka_bindings(KafkaBindings::new("127.0.0.1:9092", "t_"));
        let specification = ServiceUnderTest::docker("alpine:3.20")
            .name("api")
            .port(80)
            .env("KAFKA_BOOTSTRAP_SERVERS", "override:9093")
            .build()
            .unwrap_or_else(|error| panic!("spec must build: {error:?}"));
        let environment = inject_service_env(&context, &specification);
        assert_eq!(
            environment
                .get("KAFKA_BOOTSTRAP_SERVERS")
                .map(String::as_str),
            Some("override:9093")
        );
    }

    #[test]
    fn upper_snake_case_formats_mock_service_and_service_keys() {
        // Given mock service and service names with hyphens
        assert_eq!(
            default_mock_service_env_key("payments-api"),
            "PAYMENTS_API_URL"
        );
        assert_eq!(default_service_env_key("order-svc"), "ORDER_SVC_URL");
    }

    #[test]
    fn inter_service_urls_only_inject_declared_depends_on() {
        // Given two services registered and handles set; worker depends_on api
        let mut context = Context::new();
        context.register_service(spec_named("api")).unwrap();
        context.register_service(spec_named("worker")).unwrap();
        context
            .set_service_handle(
                "api",
                ServiceHandle::new(
                    "api".to_string(),
                    "http://127.0.0.1:1111".to_string(),
                    HashMap::new(),
                    None,
                    true,
                ),
            )
            .unwrap();
        context
            .set_service_handle(
                "worker",
                ServiceHandle::new(
                    "worker".to_string(),
                    "http://127.0.0.1:2222".to_string(),
                    HashMap::new(),
                    None,
                    true,
                ),
            )
            .unwrap();

        let environment_api = inject_service_env(&context, &spec_named("api"));
        assert!(!environment_api.contains_key("WORKER_URL"));

        let specification_worker = ServiceUnderTest::docker("alpine:3.20")
            .name("worker")
            .port(80)
            .depends_on("api")
            .build()
            .unwrap_or_else(|error| panic!("spec must build: {error:?}"));
        let environment_worker = inject_service_env(&context, &specification_worker);
        let expected_upstream = "http://127.0.0.1:1111".to_string();
        let expected_upstream_container =
            rewrite_injected_addresses_for_service_container(expected_upstream.as_str());
        assert_eq!(
            environment_worker.get("API_URL").map(String::as_str),
            Some(expected_upstream_container.as_str())
        );
    }

    #[test]
    fn replace_database_path_updates_segment() {
        // Given a Postgres URL with a database path segment
        let url = "postgresql://u:p@127.0.0.1:5432/scenario_db";
        // When we replace the database name in the path
        // Then only the final segment changes
        assert_eq!(
            replace_database_path(url, "scenario_db", "scenario_db_api"),
            "postgresql://u:p@127.0.0.1:5432/scenario_db_api"
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_multi_service_partitioning_distinct_urls() {
        use crate::resources::PostgresBindings;

        let mut context = Context::new();
        let bindings = PostgresBindings {
            host: "127.0.0.1".to_string(),
            port: 5432,
            database: "scenario_db".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
            connection_url: "postgresql://u:p@127.0.0.1:5432/scenario_db".to_string(),
            replica_port: None,
        };
        context.set_postgres_bindings(bindings);
        context.register_service(spec_named("api")).unwrap();
        context.register_service(spec_named("worker")).unwrap();

        let specification_api = spec_named("api");
        let environment_api = inject_service_env(&context, &specification_api);
        let specification_worker = spec_named("worker");
        let environment_worker = inject_service_env(&context, &specification_worker);

        let expected_api = format!("postgresql://u:p@{HOST_DOCKER_INTERNAL}:5432/scenario_db_api");
        let expected_worker =
            format!("postgresql://u:p@{HOST_DOCKER_INTERNAL}:5432/scenario_db_worker");
        assert_eq!(environment_api.get("DATABASE_URL"), Some(&expected_api));
        assert_eq!(
            environment_worker.get("DATABASE_URL"),
            Some(&expected_worker)
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_single_service_uses_base_url() {
        use crate::resources::PostgresBindings;

        let mut context = Context::new();
        let url = "postgresql://u:p@127.0.0.1:5432/scenario_db";
        let bindings = PostgresBindings {
            host: "127.0.0.1".to_string(),
            port: 5432,
            database: "scenario_db".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
            connection_url: url.to_string(),
            replica_port: None,
        };
        context.set_postgres_bindings(bindings);
        context.register_service(spec_named("only")).unwrap();
        let specification = spec_named("only");
        let environment = inject_service_env(&context, &specification);
        let expected_url = rewrite_injected_addresses_for_service_container(url);
        assert_eq!(environment.get("DATABASE_URL"), Some(&expected_url));
    }

    #[cfg(all(feature = "services", feature = "nats"))]
    #[test]
    fn nats_default_injection_and_mapping_rename() {
        use crate::resources::NatsBindings;

        let mut context = Context::new();
        let bindings = NatsBindings {
            connection_url: "nats://127.0.0.1:4222".to_string(),
            host: "127.0.0.1".to_string(),
            port: 4222,
            subject_prefix: "shakedown.scenario.".to_string(),
        };
        context.set_nats_bindings(bindings);

        let specification_default = spec_named("api");
        let environment_default = inject_service_env(&context, &specification_default);
        let expected_nats =
            rewrite_injected_addresses_for_service_container("nats://127.0.0.1:4222");
        assert_eq!(environment_default.get("NATS_URL"), Some(&expected_nats));

        let specification_custom = ServiceUnderTest::docker("alpine:3.20")
            .name("api")
            .port(80)
            .env_mapping(EnvMapping::nats("CUSTOM_NATS"))
            .build()
            .unwrap_or_else(|error| panic!("spec must build: {error:?}"));
        let environment_custom = inject_service_env(&context, &specification_custom);
        assert!(!environment_custom.contains_key("NATS_URL"));
        assert_eq!(environment_custom.get("CUSTOM_NATS"), Some(&expected_nats));
    }
}
