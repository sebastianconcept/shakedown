//! Apply provision handles onto [`super::Context`] engine bindings.
//!
//! This module is useful when a new engine must expose connection fields after provision.
//! This module copies connection fields from resource handles into named bindings on the
//! context.
//! This module does not provision containers or start mock servers.

#[cfg(any(
    feature = "postgres",
    feature = "mariadb",
    feature = "mysql",
    feature = "sqlite",
    feature = "duckdb",
    feature = "mongodb",
    feature = "nats",
    feature = "neo4j",
    feature = "elasticsearch",
    feature = "opensearch",
    feature = "qdrant",
    feature = "weaviate",
    feature = "influxdb",
    feature = "timescaledb",
    feature = "rabbitmq",
    any(feature = "redis", feature = "valkey"),
))]
use super::environment::DEFAULT_RESOURCE_BINDING_NAME;
use crate::error::ResourceHandle;
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
#[cfg(feature = "sqlite")]
use crate::resources::SqliteBindings;
#[cfg(feature = "weaviate")]
use crate::resources::WeaviateBindings;

use super::context::Context;

#[cfg(feature = "postgres")]
/// If any handle metadata carries Postgres engine fields, set [`Context::set_postgres_bindings`].
pub(crate) fn apply_resource_handles_to_postgres_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_mariadb_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_sqlite_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_duckdb_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_mysql_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_kafka_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_mongo_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_nats_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_neo4j_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_elasticsearch_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_opensearch_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_weaviate_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_influxdb_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_timescaledb_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_qdrant_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_redis_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) fn apply_resource_handles_to_rabbit_bindings(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
