//! Drive one scenario from provision through teardown.
//!
//! [`run_scenario`] is the harness entry used by `#[shakedown::test]`. Engine-specific binding
//! updates and mock-service startup live here so [`super::context::Context`] stays the state type.

use crate::error::{InfrastructureError, ResourceHandle, ScenarioId, ShakedownError};
#[cfg(feature = "mock-services")]
use crate::event::Event;
use crate::event::{EventLevel, EventLog};
#[cfg(all(feature = "mock-services", feature = "grpc"))]
use crate::mock_service::grpc::server::{spawn_grpc_mock_service_server, GrpcMockServiceServer};
#[cfg(feature = "mock-services")]
use crate::mock_service::handle::MockServiceHandle;
#[cfg(feature = "mock-services")]
use crate::mock_service::http::server::{spawn_http_mock_service_server, HttpMockServiceServer};
#[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
use crate::mock_service::tcp::{spawn_tcp_mock_service_server, TcpMockServiceServer};
#[cfg(feature = "mock-services")]
use crate::mock_service::types::MockServiceResources;
#[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
use crate::mock_service::udp::{spawn_udp_mock_service_server, UdpMockServiceServer};
#[cfg(feature = "duckdb")]
use crate::resources::DuckDbBindings;
#[cfg(feature = "elasticsearch")]
use crate::resources::ElasticsearchBindings;
#[cfg(feature = "influxdb")]
use crate::resources::InfluxDbBindings;
use crate::resources::KafkaBindings;
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
#[cfg(all(feature = "fault-injection", feature = "kafka"))]
use crate::resources::{BuiltinResource, RegisteredResource};
#[cfg(feature = "mock-services")]
use std::collections::HashMap;
#[cfg(feature = "mock-services")]
use std::sync::Arc;

use super::context::Context;
#[cfg(any(
    feature = "postgres",
    feature = "mariadb",
    feature = "mysql",
    feature = "timescaledb",
))]
use super::context::DEFAULT_RESOURCE_BINDING_NAME;
#[cfg(all(feature = "fault-injection", feature = "kafka"))]
use super::environment::ScenarioKind;
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
use super::phase::Phase;

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

/// Stable Toxiproxy proxy name for the suite Kafka broker (must match fault-injection wiring and tests).
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
            let replica_port = meta
                .get("replica_port")
                .and_then(|value| value.as_u64())
                .map(|port| port as u16);
            let binding_key = meta
                .get("binding_name")
                .and_then(|value| value.as_str())
                .unwrap_or(DEFAULT_RESOURCE_BINDING_NAME);
            ctx.set_postgres_bindings_named(
                binding_key,
                PostgresBindings {
                    host: host.to_string(),
                    port,
                    database: database.to_string(),
                    username: username.to_string(),
                    password: password.to_string(),
                    connection_url: connection_url.to_string(),
                    replica_port,
                },
            );
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
            let binding_key = meta
                .get("binding_name")
                .and_then(|value| value.as_str())
                .unwrap_or(DEFAULT_RESOURCE_BINDING_NAME);
            ctx.set_mariadb_bindings_named(
                binding_key,
                MariaDBBindings {
                    host: host.to_string(),
                    port,
                    database: database.to_string(),
                    username: username.to_string(),
                    password: password.to_string(),
                    connection_url: connection_url.to_string(),
                },
            );
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
            let binding_key = meta
                .get("binding_name")
                .and_then(|value| value.as_str())
                .unwrap_or(DEFAULT_RESOURCE_BINDING_NAME);
            ctx.set_mysql_bindings_named(
                binding_key,
                MysqlBindings {
                    host: host.to_string(),
                    port,
                    database: database.to_string(),
                    username: username.to_string(),
                    password: password.to_string(),
                    connection_url: connection_url.to_string(),
                },
            );
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
        let replica_set_host_ports = meta
            .get("host_ports")
            .and_then(|value| value.as_array())
            .map(|ports| {
                ports
                    .iter()
                    .filter_map(|entry| entry.as_u64().map(|port| port as u16))
                    .collect::<Vec<u16>>()
            })
            .unwrap_or_default();
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
                replica_set_host_ports,
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
            let binding_key = meta
                .get("binding_name")
                .and_then(|value| value.as_str())
                .unwrap_or(DEFAULT_RESOURCE_BINDING_NAME);
            ctx.set_timescaledb_bindings_named(
                binding_key,
                PostgresBindings {
                    host: host.to_string(),
                    port,
                    database: database.to_string(),
                    username: username.to_string(),
                    password: password.to_string(),
                    connection_url: connection_url.to_string(),
                    replica_port: None,
                },
            );
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
/// If any handle metadata carries Redis, Valkey, or Redis Sentinel engine fields, set
/// [`Context::set_redis_bindings`] (and [`Context::set_redis_sentinel_bindings`] for Sentinel).
fn apply_resource_handles_to_redis_bindings(ctx: &mut Context, handles: &[ResourceHandle]) {
    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        let Some(engine) = meta.get("engine").and_then(|value| value.as_str()) else {
            continue;
        };
        if engine != "redis" && engine != "valkey" && engine != "redis-sentinel" {
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
        #[cfg(feature = "redis-sentinel")]
        if engine == "redis-sentinel" {
            let sentinel_hosts = meta.get("sentinel_hosts").and_then(|value| value.as_str());
            let master_name = meta.get("master_name").and_then(|value| value.as_str());
            let connection_url = meta.get("connection_url").and_then(|value| value.as_str());
            let key_prefix = meta
                .get("key_prefix")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            let replica_urls = meta
                .get("replica_connection_urls")
                .and_then(|value| value.as_array())
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.as_str().map(str::to_owned))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if let (Some(sentinel_hosts), Some(master_name), Some(connection_url)) =
                (sentinel_hosts, master_name, connection_url)
            {
                ctx.set_redis_sentinel_bindings(RedisSentinelBindings {
                    sentinel_hosts: sentinel_hosts.to_string(),
                    master_name: master_name.to_string(),
                    connection_url: connection_url.to_string(),
                    replica_connection_urls: replica_urls,
                    key_prefix,
                });
            }
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
        let connection_url = meta.get("connection_url").and_then(|v| v.as_str());
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
            let connection_url = connection_url.unwrap_or(amqp_uri);
            ctx.set_rabbit_bindings(RabbitBindings::with_connection_url(
                amqp_uri,
                connection_url,
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

#[cfg(all(feature = "fault-injection", feature = "mongodb-cluster"))]
fn prepare_mongo_replica_set_inter_node_defer(ctx: &mut Context) {
    use crate::resources::{BuiltinResource, RegisteredResource};
    let defer = ctx.resources().iter().any(|registered| {
        matches!(
            registered,
            RegisteredResource::Builtin(BuiltinResource::Toxiproxy(_))
        )
    }) && ctx.resources().iter().any(|registered| {
        matches!(
            registered,
            RegisteredResource::Builtin(BuiltinResource::MongoReplicaSet(_))
        )
    });
    for registered in ctx.resources_mut() {
        if let RegisteredResource::Builtin(BuiltinResource::MongoReplicaSet(mongo)) = registered {
            mongo.set_defer_repl_set_init(defer);
        }
    }
}

#[cfg(feature = "mongodb-cluster")]
async fn register_mongo_replica_set_cluster_control(ctx: &mut Context, handles: &[ResourceHandle]) {
    use std::sync::Arc;

    use crate::cluster::mongo_replica_set::cluster_control::MongoReplicaSetClusterControl;
    use crate::cluster::mongo_replica_set::resource::mongo_replica_set_suite_snapshot;
    use crate::cluster::mongo_replica_set::MongoReplicaSetMarker;
    use crate::cluster::ClusterHandle;

    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        if meta.get("mongodb_cluster").and_then(|value| value.as_str()) != Some("replica_set") {
            continue;
        }
        let Some(snapshot) = mongo_replica_set_suite_snapshot().await else {
            continue;
        };
        let arc = Arc::new(MongoReplicaSetClusterControl::new(snapshot.node_count));
        ctx.set_mongo_replica_set_cluster(Some(Arc::clone(&arc)));
        ctx.register_cluster_handle::<MongoReplicaSetMarker>(ClusterHandle::new(
            "mongo",
            Box::new(arc),
            None,
        ));
        return;
    }
}

#[cfg(feature = "kafka-cluster")]
async fn register_kafka_cluster_control(ctx: &mut Context, handles: &[ResourceHandle]) {
    use std::sync::Arc;

    use crate::cluster::kafka_cluster::cluster_control::KafkaClusterClusterControl;
    use crate::cluster::kafka_cluster::resource::kafka_cluster_suite_snapshot;
    use crate::cluster::kafka_cluster::KafkaClusterMarker;
    use crate::cluster::ClusterHandle;

    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        if meta.get("kafka_cluster").and_then(|value| value.as_str()) != Some("kraft") {
            continue;
        }
        let Some(snapshot) = kafka_cluster_suite_snapshot().await else {
            continue;
        };
        let arc = Arc::new(KafkaClusterClusterControl::new(
            snapshot.node_count,
            snapshot.bootstrap_servers.clone(),
        ));
        ctx.set_kafka_cluster_control(Some(Arc::clone(&arc)));
        ctx.register_cluster_handle::<KafkaClusterMarker>(ClusterHandle::new(
            "kafka",
            Box::new(arc),
            None,
        ));
        return;
    }
}

#[cfg(feature = "redis-sentinel")]
async fn register_redis_sentinel_cluster_control(ctx: &mut Context, handles: &[ResourceHandle]) {
    use std::sync::Arc;

    use crate::cluster::redis_sentinel::cluster_control::RedisSentinelClusterControl;
    use crate::cluster::redis_sentinel::resource::redis_sentinel_suite_snapshot;
    use crate::cluster::redis_sentinel::RedisSentinelMarker;
    use crate::cluster::ClusterHandle;

    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        if meta
            .get("redis_sentinel_cluster")
            .and_then(|value| value.as_str())
            != Some("suite")
        {
            continue;
        }
        let Some(snapshot) = redis_sentinel_suite_snapshot().await else {
            continue;
        };
        let arc = Arc::new(RedisSentinelClusterControl::new(snapshot.node_count));
        ctx.set_redis_sentinel_cluster(Some(Arc::clone(&arc)));
        ctx.register_cluster_handle::<RedisSentinelMarker>(ClusterHandle::new(
            "redis-sentinel",
            Box::new(arc),
            None,
        ));
        return;
    }
}

#[cfg(feature = "postgres-cluster")]
async fn register_postgres_replica_set_cluster_control(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
    use std::sync::Arc;

    use crate::cluster::postgres_replica_set::cluster_control::PostgresReplicaSetClusterControl;
    use crate::cluster::postgres_replica_set::PostgresReplicaSetMarker;
    use crate::cluster::ClusterHandle;

    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        let Some(control) = PostgresReplicaSetClusterControl::from_handle_metadata(meta) else {
            continue;
        };
        let arc = Arc::new(control);
        ctx.set_postgres_replica_set_cluster(Some(Arc::clone(&arc)));
        ctx.register_cluster_handle::<PostgresReplicaSetMarker>(ClusterHandle::new(
            "postgres",
            Box::new(arc),
            None,
        ));
        return;
    }
}

#[cfg(feature = "rabbitmq-cluster")]
async fn register_rabbit_cluster_control(ctx: &mut Context, handles: &[ResourceHandle]) {
    use std::sync::Arc;

    use crate::cluster::rabbit_cluster::cluster_control::RabbitClusterClusterControl;
    use crate::cluster::rabbit_cluster::resource::rabbit_cluster_suite_snapshot;
    use crate::cluster::rabbit_cluster::RabbitClusterMarker;
    use crate::cluster::ClusterHandle;

    for handle in handles {
        let Some(meta) = &handle.metadata else {
            continue;
        };
        if meta
            .get("rabbitmq_cluster")
            .and_then(|value| value.as_str())
            != Some("suite")
        {
            continue;
        }
        let Some(snapshot) = rabbit_cluster_suite_snapshot().await else {
            continue;
        };
        let arc = Arc::new(RabbitClusterClusterControl::new(snapshot));
        ctx.set_rabbit_cluster_control(Some(Arc::clone(&arc)));
        ctx.register_cluster_handle::<RabbitClusterMarker>(ClusterHandle::new(
            "rabbitmq",
            Box::new(arc),
            None,
        ));
        return;
    }
}

/// Runs a scenario through all lifecycle phases.
///
/// This function executes the scenario in order:
/// - Provision resources (async)
/// - Check readiness (async)
/// - Seed data (async)
/// - Run scenario logic
/// - Record the scenario body's outcome on the timeline
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
/// Phases always run in this order: provision → readiness → seed → run → assert → teardown
/// (summarized in the list at the start of this documentation).
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
/// ```rust,no_run
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
#[allow(unused_variables)] // `ctx` unused when no resource features are enabled alongside mock services
fn mock_service_resources_from_context(ctx: &Context) -> Arc<MockServiceResources> {
    #[allow(unused_mut)]
    // assignments are feature-gated; with mock services alone the struct stays default
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
async fn reset_mock_services_at_run_start(ctx: &mut Context) {
    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    {
        let servers = std::mem::take(&mut ctx.grpc_mock_service_servers);
        for (_name, mut server) in servers {
            if let Some(sender) = server.shutdown_tx.take() {
                let _ = sender.send(());
            }
            let _ = server.join.await;
        }
    }
    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    {
        shutdown_tcp_mock_service_servers_silent(ctx).await;
    }
    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    {
        shutdown_udp_mock_service_servers_silent(ctx).await;
    }
    let servers = std::mem::take(&mut ctx.http_mock_service_servers);
    for (_name, mut server) in servers {
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
    }
    ctx.mock_service_handles.clear();
    ctx.mock_service_servers_phase_complete = false;
}

#[cfg(feature = "mock-services")]
async fn tcp_probe_local_bind_address(bind_address: &str) -> std::io::Result<()> {
    let socket_address: std::net::SocketAddr = bind_address.parse().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid HTTP mock service bind address",
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
            "mock service tcp probe timed out",
        )),
    }
}

#[cfg(feature = "mock-services")]
fn udp_probe_local_bind_address(bind_address: &str) -> std::io::Result<()> {
    use std::net::SocketAddr;
    // Parse only: the listener is already bound in `spawn_udp_mock_service_server`. Sending a
    // probe datagram would be handled by the mock and recorded as a spurious call.
    let _: SocketAddr = bind_address.parse().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid UDP mock service bind address",
        )
    })?;
    Ok(())
}

#[cfg(feature = "mock-services")]
async fn mock_service_bind_address_ready(
    client_url: &str,
    bind_address: &str,
) -> std::io::Result<()> {
    if client_url.starts_with("udp://") {
        udp_probe_local_bind_address(bind_address)
    } else {
        tcp_probe_local_bind_address(bind_address).await
    }
}

#[cfg(feature = "mock-services")]
fn recorded_mock_service_call_count(
    handles: &HashMap<String, Arc<MockServiceHandle>>,
    mock_service_name: &str,
) -> usize {
    handles
        .get(mock_service_name)
        .map(|handle| handle.calls().len())
        .unwrap_or(0)
}

#[cfg(all(feature = "mock-services", feature = "grpc"))]
async fn shutdown_grpc_mock_service_servers_with_log(ctx: &mut Context, event_log: &mut EventLog) {
    let servers = std::mem::take(&mut ctx.grpc_mock_service_servers);
    for (name, mut server) in servers {
        let recorded_count =
            recorded_mock_service_call_count(&ctx.mock_service_handles, name.as_str());
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
        event_log.append(Event::info(
            "mock-service",
            format!("mock service '{name}' stopped; {recorded_count} calls recorded"),
        ));
    }
}

#[cfg(all(feature = "mock-services", feature = "grpc"))]
async fn shutdown_grpc_mock_service_servers_silent(ctx: &mut Context) {
    let servers = std::mem::take(&mut ctx.grpc_mock_service_servers);
    for (_name, mut server) in servers {
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
    }
}

#[cfg(feature = "mock-services")]
async fn shutdown_http_mock_service_servers_with_log(ctx: &mut Context, event_log: &mut EventLog) {
    let servers = std::mem::take(&mut ctx.http_mock_service_servers);
    for (name, mut server) in servers {
        let recorded_count =
            recorded_mock_service_call_count(&ctx.mock_service_handles, name.as_str());
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
        event_log.append(Event::info(
            "mock-service",
            format!("mock service '{name}' stopped; {recorded_count} calls recorded"),
        ));
    }
}

#[cfg(feature = "mock-services")]
async fn shutdown_http_mock_service_servers_silent(ctx: &mut Context) {
    let servers = std::mem::take(&mut ctx.http_mock_service_servers);
    for (_name, mut server) in servers {
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
    }
}

#[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
async fn shutdown_tcp_mock_service_servers_with_log(ctx: &mut Context, event_log: &mut EventLog) {
    let servers = std::mem::take(&mut ctx.tcp_mock_service_servers);
    for (name, mut server) in servers {
        let recorded_count =
            recorded_mock_service_call_count(&ctx.mock_service_handles, name.as_str());
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
        event_log.append(Event::info(
            "mock-service",
            format!("mock service '{name}' stopped; {recorded_count} calls recorded"),
        ));
    }
}

#[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
async fn shutdown_tcp_mock_service_servers_silent(ctx: &mut Context) {
    let servers = std::mem::take(&mut ctx.tcp_mock_service_servers);
    for (_name, mut server) in servers {
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
    }
}

#[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
async fn shutdown_udp_mock_service_servers_with_log(ctx: &mut Context, event_log: &mut EventLog) {
    let servers = std::mem::take(&mut ctx.udp_mock_service_servers);
    for (name, mut server) in servers {
        let recorded_count =
            recorded_mock_service_call_count(&ctx.mock_service_handles, name.as_str());
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
        event_log.append(Event::info(
            "mock-service",
            format!("mock service '{name}' stopped; {recorded_count} calls recorded"),
        ));
    }
}

#[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
async fn shutdown_udp_mock_service_servers_silent(ctx: &mut Context) {
    let servers = std::mem::take(&mut ctx.udp_mock_service_servers);
    for (_name, mut server) in servers {
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
    }
}

#[cfg(feature = "mock-services")]
async fn start_and_readiness_mock_services(
    ctx: &mut Context,
    event_log: &mut EventLog,
) -> Result<(), ShakedownError> {
    let pending_http = std::mem::take(&mut ctx.pending_http_mock_services);
    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    let pending_grpc = std::mem::take(&mut ctx.pending_grpc_mock_services);
    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    let pending_tcp = std::mem::take(&mut ctx.pending_tcp_mock_services);
    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    let pending_udp = std::mem::take(&mut ctx.pending_udp_mock_services);

    let nothing_to_start = pending_http.is_empty()
        && {
            #[cfg(all(feature = "mock-services", feature = "grpc"))]
            {
                pending_grpc.is_empty()
            }
            #[cfg(not(all(feature = "mock-services", feature = "grpc")))]
            {
                true
            }
        }
        && {
            #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
            {
                pending_tcp.is_empty()
            }
            #[cfg(not(all(feature = "mock-services", feature = "tcp-mock-services")))]
            {
                true
            }
        }
        && {
            #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
            {
                pending_udp.is_empty()
            }
            #[cfg(not(all(feature = "mock-services", feature = "udp-mock-services")))]
            {
                true
            }
        };

    if nothing_to_start {
        ctx.mock_service_servers_phase_complete = true;
        return Ok(());
    }

    let resources = mock_service_resources_from_context(ctx);
    let scenario_id_string = ctx.scenario_id().to_string();

    let mut started_http_mock_servers: Vec<(String, HttpMockServiceServer)> = Vec::new();
    for (mock_service_name, mock_service_impl) in pending_http {
        match spawn_http_mock_service_server(
            mock_service_name.clone(),
            mock_service_impl,
            scenario_id_string.clone(),
            Arc::clone(&resources),
            None,
            ctx.mock_service_bind_ip,
        )
        .await
        {
            Ok((handle, server)) => {
                let bind_display = handle.bind_address.clone();
                ctx.mock_service_handles
                    .insert(mock_service_name.clone(), handle);
                started_http_mock_servers.push((mock_service_name.clone(), server));
                event_log.append(Event::info(
                    "mock-service",
                    format!("mock service '{mock_service_name}' started on {bind_display}"),
                ));
            }
            Err(error) => {
                for (_rollback_name, mut server) in started_http_mock_servers {
                    if let Some(sender) = server.shutdown_tx.take() {
                        let _ = sender.send(());
                    }
                    let _ = server.join.await;
                }
                ctx.mock_service_handles.clear();
                return Err(InfrastructureError::Provision {
                    resource: format!("mock-service:{mock_service_name}"),
                    source: Box::new(error),
                }
                .into());
            }
        }
    }
    ctx.http_mock_service_servers = started_http_mock_servers;

    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    {
        let mut started_grpc_mock_servers: Vec<(String, GrpcMockServiceServer)> = Vec::new();
        for (mock_service_name, mock_service_impl) in pending_grpc {
            match spawn_grpc_mock_service_server(
                mock_service_name.clone(),
                mock_service_impl,
                scenario_id_string.clone(),
                Arc::clone(&resources),
                None,
                ctx.mock_service_bind_ip,
            )
            .await
            {
                Ok((handle, server)) => {
                    let bind_display = handle.bind_address.clone();
                    ctx.mock_service_handles
                        .insert(mock_service_name.clone(), handle);
                    started_grpc_mock_servers.push((mock_service_name.clone(), server));
                    event_log.append(Event::info(
                        "mock-service",
                        format!("mock service '{mock_service_name}' started on {bind_display}"),
                    ));
                }
                Err(error) => {
                    shutdown_http_mock_service_servers_silent(ctx).await;
                    for (_rollback_name, mut server) in started_grpc_mock_servers {
                        if let Some(sender) = server.shutdown_tx.take() {
                            let _ = sender.send(());
                        }
                        let _ = server.join.await;
                    }
                    ctx.mock_service_handles.clear();
                    return Err(InfrastructureError::Provision {
                        resource: format!("mock-service:{mock_service_name}"),
                        source: Box::new(error),
                    }
                    .into());
                }
            }
        }
        ctx.grpc_mock_service_servers = started_grpc_mock_servers;
    }

    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    {
        let mut started_tcp_mock_servers: Vec<(String, TcpMockServiceServer)> = Vec::new();
        for (mock_service_name, registration) in pending_tcp {
            match spawn_tcp_mock_service_server(
                mock_service_name.clone(),
                Arc::clone(&registration.connection),
                registration.tls.clone(),
                scenario_id_string.clone(),
                Arc::clone(&resources),
                None,
                ctx.mock_service_bind_ip,
            )
            .await
            {
                Ok((handle, server)) => {
                    let bind_display = handle.bind_address.clone();
                    ctx.mock_service_handles
                        .insert(mock_service_name.clone(), handle);
                    started_tcp_mock_servers.push((mock_service_name.clone(), server));
                    event_log.append(Event::info(
                        "mock-service",
                        format!("mock service '{mock_service_name}' started on {bind_display}"),
                    ));
                }
                Err(error) => {
                    shutdown_http_mock_service_servers_silent(ctx).await;
                    #[cfg(all(feature = "mock-services", feature = "grpc"))]
                    shutdown_grpc_mock_service_servers_silent(ctx).await;
                    for (_rollback_name, mut server) in started_tcp_mock_servers {
                        if let Some(sender) = server.shutdown_tx.take() {
                            let _ = sender.send(());
                        }
                        let _ = server.join.await;
                    }
                    ctx.mock_service_handles.clear();
                    return Err(InfrastructureError::Provision {
                        resource: format!("mock-service:{mock_service_name}"),
                        source: Box::new(error),
                    }
                    .into());
                }
            }
        }
        ctx.tcp_mock_service_servers = started_tcp_mock_servers;
    }

    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    {
        let mut started_udp_mock_servers: Vec<(String, UdpMockServiceServer)> = Vec::new();
        for (mock_service_name, registration) in pending_udp {
            match spawn_udp_mock_service_server(
                mock_service_name.clone(),
                Arc::clone(&registration.handler),
                scenario_id_string.clone(),
                Arc::clone(&resources),
                None,
                ctx.mock_service_bind_ip,
            )
            .await
            {
                Ok((handle, server)) => {
                    let bind_display = handle.bind_address.clone();
                    ctx.mock_service_handles
                        .insert(mock_service_name.clone(), handle);
                    started_udp_mock_servers.push((mock_service_name.clone(), server));
                    event_log.append(Event::info(
                        "mock-service",
                        format!("mock service '{mock_service_name}' started on {bind_display}"),
                    ));
                }
                Err(error) => {
                    shutdown_http_mock_service_servers_silent(ctx).await;
                    #[cfg(all(feature = "mock-services", feature = "grpc"))]
                    shutdown_grpc_mock_service_servers_silent(ctx).await;
                    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
                    shutdown_tcp_mock_service_servers_silent(ctx).await;
                    for (_rollback_name, mut server) in started_udp_mock_servers {
                        if let Some(sender) = server.shutdown_tx.take() {
                            let _ = sender.send(());
                        }
                        let _ = server.join.await;
                    }
                    ctx.mock_service_handles.clear();
                    return Err(InfrastructureError::Provision {
                        resource: format!("mock-service:{mock_service_name}"),
                        source: Box::new(error),
                    }
                    .into());
                }
            }
        }
        ctx.udp_mock_service_servers = started_udp_mock_servers;
    }

    let readiness_targets: Vec<(String, String, String)> = ctx
        .mock_service_handles
        .iter()
        .map(|(mock_service_name, handle)| {
            (
                mock_service_name.clone(),
                handle.bind_address.clone(),
                handle.client_url.clone(),
            )
        })
        .collect();
    for (mock_service_name, bind_address, client_url) in readiness_targets {
        if let Err(source) =
            mock_service_bind_address_ready(client_url.as_str(), bind_address.as_str()).await
        {
            shutdown_http_mock_service_servers_silent(ctx).await;
            #[cfg(all(feature = "mock-services", feature = "grpc"))]
            shutdown_grpc_mock_service_servers_silent(ctx).await;
            #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
            shutdown_tcp_mock_service_servers_silent(ctx).await;
            #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
            shutdown_udp_mock_service_servers_silent(ctx).await;
            ctx.mock_service_handles.clear();
            return Err(InfrastructureError::Readiness {
                resource: format!("mock-service:{mock_service_name}"),
                source: Box::new(source),
            }
            .into());
        }
    }
    ctx.mock_service_servers_phase_complete = true;
    Ok(())
}

/// Writes the scenario body's result into the event log during the assert phase.
///
/// User assertions stay in the scenario body. This is the harness copy of that result so the
/// timeline shows success, or the error the body returned, before teardown. Call it only after
/// the body has run. Infrastructure failures that return earlier are not assert-phase outcomes.
fn record_assert_phase_outcome(event_log: &mut EventLog, body_result: &Result<(), ShakedownError>) {
    match body_result {
        Ok(()) => {
            event_log.append_raw(
                EventLevel::Info,
                "lifecycle",
                "Assert phase outcome: success",
            );
        }
        Err(error) => {
            event_log.append_raw(
                EventLevel::Error,
                "lifecycle",
                format!("Assert phase outcome: {error}"),
            );
        }
    }
}

/// Final value produced by a [`run_scenario`] async body: `()` for legacy scenarios, or `Result<(), ShakedownError>` to fail without panicking (e.g. performance assertions).
///
/// Return `()` on success, or [`Result::Err`] with [`ShakedownError`] to fail the scenario without
/// panicking and to integrate with the failure timeline (for example `ShakedownError::PerformanceAssertionFailed`
/// when the `load` feature is enabled).
pub trait ScenarioRunOutcome: Send + 'static {
    /// Converts the scenario body's output into a harness result.
    fn into_shakedown_result(self) -> Result<(), ShakedownError>;
}

impl ScenarioRunOutcome for () {
    fn into_shakedown_result(self) -> Result<(), ShakedownError> {
        Ok(())
    }
}

impl ScenarioRunOutcome for Result<(), ShakedownError> {
    fn into_shakedown_result(self) -> Result<(), ShakedownError> {
        self
    }
}

/// Run the full scenario lifecycle: provision → readiness → seed → scenario → assert → teardown.
///
/// This is the primary entry point for executing a shakedown scenario. It drives all registered
/// resources through the lifecycle phases, then runs `scenario_fn` with the prepared `ctx`.
/// After the body returns, the assert phase records that result on `event_log` — success, or
/// the error the body returned — and then teardown runs. A body error still renders a failure
/// timeline after teardown. Infrastructure failures before the body return as infrastructure
/// errors and do not record an assert-phase outcome.
///
/// # Panics
///
/// Does not panic; propagates errors as `Err(ShakedownError)`.
pub async fn run_scenario<F, O>(
    ctx: &mut Context,
    scenario_id: impl Into<String>,
    scenario_fn: F,
    event_log: &mut EventLog,
) -> Result<(), ShakedownError>
where
    F: for<'s> FnOnce(
        &'s mut Context,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = O> + Send + 's>>,
    O: ScenarioRunOutcome,
{
    // Update scenario ID
    ctx.set_scenario_id(scenario_id);

    #[cfg(feature = "mock-services")]
    reset_mock_services_at_run_start(ctx).await;

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

    #[cfg(all(feature = "fault-injection", feature = "mongodb-cluster"))]
    prepare_mongo_replica_set_inter_node_defer(ctx);

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
                #[cfg(feature = "mongodb-cluster")]
                register_mongo_replica_set_cluster_control(ctx, &handles).await;
                #[cfg(feature = "kafka-cluster")]
                register_kafka_cluster_control(ctx, &handles).await;
                #[cfg(feature = "redis-sentinel")]
                register_redis_sentinel_cluster_control(ctx, &handles).await;
                #[cfg(feature = "postgres-cluster")]
                register_postgres_replica_set_cluster_control(ctx, &handles).await;
                #[cfg(feature = "rabbitmq-cluster")]
                register_rabbit_cluster_control(ctx, &handles).await;
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

    #[cfg(all(feature = "fault-injection", feature = "mongodb-cluster"))]
    {
        if let Err(error) =
            crate::cluster::mongo_replica_set::inter_node::finalize_mongo_replica_set_inter_node_mesh(ctx)
                .await
        {
            event_log.append_raw(
                EventLevel::Error,
                "lifecycle",
                format!("Provision failed (MongoDB inter-node mesh): {error}"),
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
    if let Err(error) = start_and_readiness_mock_services(ctx, event_log).await {
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
    let (scenario_application_event_sender, scenario_application_event_receiver) =
        std::sync::mpsc::channel::<crate::event::Event>();
    ctx.set_scenario_application_event_sender(Some(scenario_application_event_sender));
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
    #[cfg(feature = "mongodb-cluster")]
    let cluster_event_receiver = {
        use crate::cluster::mongo_replica_set::MongoReplicaSetMarker;
        use crate::cluster::CLUSTER_EVENT_CHANNEL_CAPACITY;
        use crate::event::Event;
        if ctx.cluster_handle::<MongoReplicaSetMarker>().is_some() {
            let (sender, receiver) =
                std::sync::mpsc::sync_channel::<Event>(CLUSTER_EVENT_CHANNEL_CAPACITY);
            if let Some(handle) = ctx.cluster_handle_mut::<MongoReplicaSetMarker>() {
                handle.set_event_sender(sender.clone());
            }
            if let Some(control) = ctx.mongo_replica_set_cluster.as_ref() {
                control.set_event_sender(sender);
            }
            Some(receiver)
        } else {
            None
        }
    };
    #[cfg(feature = "kafka-cluster")]
    let kafka_cluster_event_receiver = {
        use crate::cluster::kafka_cluster::KafkaClusterMarker;
        use crate::cluster::CLUSTER_EVENT_CHANNEL_CAPACITY;
        use crate::event::Event;
        if ctx.cluster_handle::<KafkaClusterMarker>().is_some() {
            let (sender, receiver) =
                std::sync::mpsc::sync_channel::<Event>(CLUSTER_EVENT_CHANNEL_CAPACITY);
            if let Some(handle) = ctx.cluster_handle_mut::<KafkaClusterMarker>() {
                handle.set_event_sender(sender.clone());
            }
            if let Some(control) = ctx.kafka_cluster_control.as_ref() {
                control.set_event_sender(sender);
            }
            Some(receiver)
        } else {
            None
        }
    };
    #[cfg(feature = "redis-sentinel")]
    let redis_sentinel_cluster_event_receiver = {
        use crate::cluster::redis_sentinel::RedisSentinelMarker;
        use crate::cluster::CLUSTER_EVENT_CHANNEL_CAPACITY;
        use crate::event::Event;
        if ctx.cluster_handle::<RedisSentinelMarker>().is_some() {
            let (sender, receiver) =
                std::sync::mpsc::sync_channel::<Event>(CLUSTER_EVENT_CHANNEL_CAPACITY);
            if let Some(handle) = ctx.cluster_handle_mut::<RedisSentinelMarker>() {
                handle.set_event_sender(sender.clone());
            }
            if let Some(control) = ctx.redis_sentinel_cluster.as_ref() {
                control.set_event_sender(sender);
            }
            Some(receiver)
        } else {
            None
        }
    };
    #[cfg(feature = "postgres-cluster")]
    let postgres_replica_set_cluster_event_receiver = {
        use crate::cluster::postgres_replica_set::PostgresReplicaSetMarker;
        use crate::cluster::CLUSTER_EVENT_CHANNEL_CAPACITY;
        use crate::event::Event;
        if ctx.cluster_handle::<PostgresReplicaSetMarker>().is_some() {
            let (sender, receiver) =
                std::sync::mpsc::sync_channel::<Event>(CLUSTER_EVENT_CHANNEL_CAPACITY);
            if let Some(handle) = ctx.cluster_handle_mut::<PostgresReplicaSetMarker>() {
                handle.set_event_sender(sender.clone());
            }
            if let Some(control) = ctx.postgres_replica_set_cluster.as_ref() {
                control.set_event_sender(sender);
            }
            Some(receiver)
        } else {
            None
        }
    };
    #[cfg(feature = "rabbitmq-cluster")]
    let rabbit_cluster_event_receiver = {
        use crate::cluster::rabbit_cluster::RabbitClusterMarker;
        use crate::cluster::CLUSTER_EVENT_CHANNEL_CAPACITY;
        use crate::event::Event;
        if ctx.cluster_handle::<RabbitClusterMarker>().is_some() {
            let (sender, receiver) =
                std::sync::mpsc::sync_channel::<Event>(CLUSTER_EVENT_CHANNEL_CAPACITY);
            if let Some(handle) = ctx.cluster_handle_mut::<RabbitClusterMarker>() {
                handle.set_event_sender(sender.clone());
            }
            if let Some(control) = ctx.rabbit_cluster_control.as_ref() {
                control.set_event_sender(sender);
            }
            Some(receiver)
        } else {
            None
        }
    };
    #[cfg(feature = "load")]
    let load_event_receiver = {
        use crate::event::Event;
        let (sender, receiver) = std::sync::mpsc::channel::<Event>();
        ctx.set_load_event_sender(Some(sender));
        receiver
    };

    // Execute the scenario function. If the body panics, we still merge load/cluster/fault events
    // into `event_log` before re-panicking so failure timelines correlate load and topology rows.
    #[cfg(any(
        feature = "load",
        feature = "mongodb-cluster",
        feature = "kafka-cluster",
        feature = "redis-sentinel",
        feature = "postgres-cluster",
        feature = "rabbitmq-cluster",
        feature = "fault-injection"
    ))]
    let scenario_result = {
        use futures_util::future::FutureExt;
        std::panic::AssertUnwindSafe(scenario_fn(ctx))
            .catch_unwind()
            .await
    };
    #[cfg(not(any(
        feature = "load",
        feature = "mongodb-cluster",
        feature = "kafka-cluster",
        feature = "redis-sentinel",
        feature = "postgres-cluster",
        feature = "rabbitmq-cluster",
        feature = "fault-injection"
    )))]
    let scenario_outcome = scenario_fn(ctx).await;

    ctx.set_scenario_application_event_sender(None);
    for event in scenario_application_event_receiver.try_iter() {
        event_log.append(event);
    }

    #[cfg(feature = "load")]
    {
        ctx.set_load_event_sender(None);
        for event in load_event_receiver.try_iter() {
            event_log.append(event);
        }
    }

    #[cfg(feature = "mongodb-cluster")]
    if let Some(receiver) = cluster_event_receiver {
        for event in receiver.try_iter() {
            event_log.append(event);
        }
    }

    #[cfg(feature = "kafka-cluster")]
    if let Some(receiver) = kafka_cluster_event_receiver {
        for event in receiver.try_iter() {
            event_log.append(event);
        }
    }

    #[cfg(feature = "redis-sentinel")]
    if let Some(receiver) = redis_sentinel_cluster_event_receiver {
        for event in receiver.try_iter() {
            event_log.append(event);
        }
    }

    #[cfg(feature = "postgres-cluster")]
    if let Some(receiver) = postgres_replica_set_cluster_event_receiver {
        for event in receiver.try_iter() {
            event_log.append(event);
        }
    }

    #[cfg(feature = "rabbitmq-cluster")]
    if let Some(receiver) = rabbit_cluster_event_receiver {
        for event in receiver.try_iter() {
            event_log.append(event);
        }
    }

    // Drain fault events into the caller-owned log after the scenario body and before assert/teardown,
    // so timelines and tests that read `event_log` after `run_scenario` see a complete run-phase slice.
    #[cfg(feature = "fault-injection")]
    for event in fault_event_receiver.try_iter() {
        event_log.append(event);
    }

    #[cfg(any(
        feature = "load",
        feature = "mongodb-cluster",
        feature = "kafka-cluster",
        feature = "redis-sentinel",
        feature = "postgres-cluster",
        feature = "rabbitmq-cluster",
        feature = "fault-injection"
    ))]
    let run_result = match scenario_result {
        Ok(outcome) => outcome.into_shakedown_result(),
        Err(panic_payload) => std::panic::resume_unwind(panic_payload),
    };
    #[cfg(not(any(
        feature = "load",
        feature = "mongodb-cluster",
        feature = "kafka-cluster",
        feature = "redis-sentinel",
        feature = "postgres-cluster",
        feature = "rabbitmq-cluster",
        feature = "fault-injection"
    )))]
    let run_result = scenario_outcome.into_shakedown_result();

    // Outcome checks stay in the scenario body. Record that result before teardown so the
    // timeline shows success or the scenario error. Infrastructure failures returned before
    // the body never reach this phase.
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
    record_assert_phase_outcome(event_log, &run_result);

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
    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    shutdown_grpc_mock_service_servers_with_log(ctx, event_log).await;
    #[cfg(feature = "mock-services")]
    shutdown_http_mock_service_servers_with_log(ctx, event_log).await;
    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    shutdown_tcp_mock_service_servers_with_log(ctx, event_log).await;
    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    shutdown_udp_mock_service_servers_with_log(ctx, event_log).await;
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

    if let Err(ref error) = run_result {
        crate::timeline::emit_failure_timeline_for_shakedown_error(
            event_log,
            scenario_id.as_str(),
            error,
        );
    }

    run_result
}
