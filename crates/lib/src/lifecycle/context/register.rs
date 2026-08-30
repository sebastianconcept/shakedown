//! Resource registration and typed `*_resource_mut` accessors on [`super::Context`].
//!
//! This module is useful when a scenario must register a builtin or extension resource before provision.
//! This module does not provision the resource.

use crate::resources::{BuiltinResource, RegisteredResource, Resource};

use super::Context;

impl Context {
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
}
