//! Convention-based environment variable injection for service containers (`services` feature).
//!
//! Pure function from [`crate::lifecycle::Context`] + [`crate::service::ServiceSpec`] → env map.
//! All values are rewritten for service containers via
//! [`crate::service::address::rewrite_injected_addresses_for_service_container`].
//!
//! `inject_service_env` is consumed by service container lifecycle (`run_scenario`, US-009). The
//! library build does not reference it until that wiring lands; keep `dead_code` allowed here.
#![allow(dead_code)]

use std::collections::HashMap;

use crate::lifecycle::Context;
use crate::resources::KafkaBindings;
use crate::service::address::rewrite_injected_addresses_for_service_container;
use crate::service::types::{EnvMapping, ServiceSpec};

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
    } else {
        inject_only_env_mappings(context, service_spec, &mut environment);
    }

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
    bridges: HashMap<String, String>,
    services: HashMap<String, String>,
}

impl MappingOverrides {
    fn collect(mappings: &[EnvMapping]) -> Self {
        let mut overrides = MappingOverrides {
            kafka: None,
            postgres: None,
            mariadb: None,
            mysql: None,
            rabbitmq: None,
            bridges: HashMap::new(),
            services: HashMap::new(),
        };
        for mapping in mappings {
            match mapping {
                EnvMapping::Kafka(name) => overrides.kafka = Some(name.clone()),
                EnvMapping::Postgres(name) => overrides.postgres = Some(name.clone()),
                EnvMapping::MariaDb(name) => overrides.mariadb = Some(name.clone()),
                EnvMapping::Mysql(name) => overrides.mysql = Some(name.clone()),
                EnvMapping::Rabbitmq(name) => overrides.rabbitmq = Some(name.clone()),
                EnvMapping::Bridge {
                    bridge_name,
                    env_var,
                } => {
                    overrides
                        .bridges
                        .insert(bridge_name.clone(), env_var.clone());
                }
                EnvMapping::Service {
                    service_name,
                    env_var,
                } => {
                    overrides
                        .services
                        .insert(service_name.clone(), env_var.clone());
                }
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
    #[cfg(any(feature = "postgres", feature = "mariadb", feature = "mysql"))]
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
        environment.insert(key.to_string(), bindings.amqp_uri.clone());
    }

    // Bridge URLs: default `{UPPER_NAME}_URL`, optional rename via `EnvMapping::Bridge`.
    #[cfg(all(feature = "bridges", feature = "services"))]
    {
        context.for_each_bridge_url(|bridge_name, client_url| {
            let key = overrides
                .bridges
                .get(bridge_name)
                .cloned()
                .unwrap_or_else(|| default_bridge_env_key(bridge_name));
            environment.insert(key, client_url.to_string());
        });
    }

    // Inter-service URLs: `{UPPER_NAME}_URL` for each other registered service when a URL exists.
    #[cfg(feature = "services")]
    {
        for peer_name in context.registered_service_names_except(service_spec.name()) {
            let Some(url) = context.service_url(peer_name) else {
                continue;
            };
            let key = overrides
                .services
                .get(peer_name)
                .cloned()
                .unwrap_or_else(|| default_service_env_key(peer_name));
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
    #[cfg(any(feature = "postgres", feature = "mariadb", feature = "mysql"))]
    let partition_relational = context.registered_service_count() > 1;
    #[cfg(not(any(feature = "postgres", feature = "mariadb", feature = "mysql")))]
    let _ = service_name;
    match mapping {
        EnvMapping::Kafka(key) => context
            .kafka_bindings()
            .map(|bindings| (key.clone(), kafka_bootstrap_value(bindings))),
        #[cfg(feature = "postgres")]
        EnvMapping::Postgres(key) => context.postgres_bindings().map(|bindings| {
            (
                key.clone(),
                relational_url_for_service(
                    bindings.connection_url.as_str(),
                    bindings.database.as_str(),
                    service_name,
                    partition_relational,
                ),
            )
        }),
        #[cfg(not(feature = "postgres"))]
        EnvMapping::Postgres(_) => None,
        #[cfg(feature = "mariadb")]
        EnvMapping::MariaDb(key) => context.mariadb_bindings().map(|bindings| {
            (
                key.clone(),
                relational_url_for_service(
                    bindings.connection_url.as_str(),
                    bindings.database.as_str(),
                    service_name,
                    partition_relational,
                ),
            )
        }),
        #[cfg(not(feature = "mariadb"))]
        EnvMapping::MariaDb(_) => None,
        #[cfg(feature = "mysql")]
        EnvMapping::Mysql(key) => context.mysql_bindings().map(|bindings| {
            (
                key.clone(),
                relational_url_for_service(
                    bindings.connection_url.as_str(),
                    bindings.database.as_str(),
                    service_name,
                    partition_relational,
                ),
            )
        }),
        #[cfg(not(feature = "mysql"))]
        EnvMapping::Mysql(_) => None,
        #[cfg(feature = "rabbitmq")]
        EnvMapping::Rabbitmq(key) => context
            .rabbit_bindings()
            .map(|bindings| (key.clone(), bindings.amqp_uri.clone())),
        #[cfg(not(feature = "rabbitmq"))]
        EnvMapping::Rabbitmq(_) => None,
        EnvMapping::Bridge {
            bridge_name,
            env_var,
        } => {
            #[cfg(all(feature = "bridges", feature = "services"))]
            {
                context
                    .bridge_url(bridge_name.as_str())
                    .map(|url| (env_var.clone(), url))
            }
            #[cfg(not(all(feature = "bridges", feature = "services")))]
            {
                let _ = bridge_name;
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
    }
}

fn kafka_bootstrap_value(bindings: &KafkaBindings) -> String {
    bindings.bootstrap_servers.clone()
}

/// Same database **name** env var (`DATABASE_URL`) for every service; distinct URLs when multiple
/// services share one engine (partition by logical service name in the path).
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

fn sanitize_service_suffix(service_name: &str) -> String {
    let trimmed: String = service_name
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    trimmed.trim_matches('_').to_string()
}

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

fn default_bridge_env_key(bridge_name: &str) -> String {
    format!("{}_URL", upper_snake_case(bridge_name))
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
    #[cfg(feature = "postgres")]
    use crate::service::address::rewrite_injected_addresses_for_service_container;
    use crate::service::address::HOST_DOCKER_INTERNAL;
    use crate::service::ServiceUnderTest;

    fn spec_named(name: &str) -> ServiceSpec {
        ServiceUnderTest::docker("alpine:3.20")
            .name(name)
            .port(80)
            .build()
            .expect("valid spec")
    }

    #[test]
    fn kafka_default_injection() {
        let mut context = Context::new();
        context.set_kafka_bindings(KafkaBindings::new("127.0.0.1:9092", "scenario_"));
        let specification = spec_named("api");
        let environment = inject_service_env(&context, &specification);
        let expected_kafka = format!("{HOST_DOCKER_INTERNAL}:9092");
        assert_eq!(
            environment.get("KAFKA_BOOTSTRAP_SERVERS"),
            Some(&expected_kafka)
        );
    }

    #[test]
    fn env_mapping_renames_kafka() {
        let mut context = Context::new();
        context.set_kafka_bindings(KafkaBindings::new("localhost:9092", "pfx_"));
        let specification = ServiceUnderTest::docker("alpine:3.20")
            .name("api")
            .port(80)
            .env_mapping(EnvMapping::kafka("CUSTOM_KAFKA"))
            .build()
            .expect("valid spec");
        let environment = inject_service_env(&context, &specification);
        assert!(!environment.contains_key("KAFKA_BOOTSTRAP_SERVERS"));
        let expected_custom = format!("{HOST_DOCKER_INTERNAL}:9092");
        assert_eq!(environment.get("CUSTOM_KAFKA"), Some(&expected_custom));
    }

    #[test]
    fn auto_inject_false_only_explicit_and_mapping() {
        let mut context = Context::new();
        context.set_kafka_bindings(KafkaBindings::new("127.0.0.1:9092", "t_"));
        let specification = ServiceUnderTest::docker("alpine:3.20")
            .name("api")
            .port(80)
            .auto_inject(false)
            .env_mapping(EnvMapping::kafka("ONLY_KAFKA"))
            .env("EXTRA", "1")
            .build()
            .expect("valid spec");
        let environment = inject_service_env(&context, &specification);
        assert!(!environment.contains_key("KAFKA_BOOTSTRAP_SERVERS"));
        let expected_only = format!("{HOST_DOCKER_INTERNAL}:9092");
        assert_eq!(environment.get("ONLY_KAFKA"), Some(&expected_only));
        assert_eq!(environment.get("EXTRA").map(String::as_str), Some("1"));
    }

    #[test]
    fn explicit_env_overrides_convention() {
        let mut context = Context::new();
        context.set_kafka_bindings(KafkaBindings::new("127.0.0.1:9092", "t_"));
        let specification = ServiceUnderTest::docker("alpine:3.20")
            .name("api")
            .port(80)
            .env("KAFKA_BOOTSTRAP_SERVERS", "override:9093")
            .build()
            .expect("valid spec");
        let environment = inject_service_env(&context, &specification);
        assert_eq!(
            environment
                .get("KAFKA_BOOTSTRAP_SERVERS")
                .map(String::as_str),
            Some("override:9093")
        );
    }

    #[test]
    fn upper_snake_case_formats_bridge_and_service_keys() {
        assert_eq!(default_bridge_env_key("payments-api"), "PAYMENTS_API_URL");
        assert_eq!(default_service_env_key("order-svc"), "ORDER_SVC_URL");
    }

    #[test]
    fn replace_database_path_updates_segment() {
        let url = "postgresql://u:p@127.0.0.1:5432/scenario_db";
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
        };
        context.set_postgres_bindings(bindings);
        context.register_service(spec_named("only")).unwrap();
        let specification = spec_named("only");
        let environment = inject_service_env(&context, &specification);
        let expected_url = rewrite_injected_addresses_for_service_container(url);
        assert_eq!(environment.get("DATABASE_URL"), Some(&expected_url));
    }
}
