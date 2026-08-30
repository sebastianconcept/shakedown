//! Attach suite cluster control handles after provision.
//!
//! This module is useful when a new cluster resource must publish topology control on
//! [`super::Context`].
//! This module registers topology control on the context when handle metadata names a cluster.
//! This module does not start or stop cluster containers.

use crate::error::ResourceHandle;

use super::context::Context;

#[cfg(feature = "mongodb-cluster")]
pub(crate) async fn register_mongo_replica_set_cluster_control(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) async fn register_kafka_cluster_control(ctx: &mut Context, handles: &[ResourceHandle]) {
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
pub(crate) async fn register_redis_sentinel_cluster_control(
    ctx: &mut Context,
    handles: &[ResourceHandle],
) {
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
pub(crate) async fn register_postgres_replica_set_cluster_control(
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
pub(crate) async fn register_rabbit_cluster_control(ctx: &mut Context, handles: &[ResourceHandle]) {
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
