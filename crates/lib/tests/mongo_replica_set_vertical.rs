//! MongoDB replica set (3× `mongod` on a suite bridge network) — provision + `replSet` readiness.
//!
//! Built with `--features mongodb-cluster`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features mongodb-cluster --test mongo_replica_set_vertical -- --ignored
//! ```

use std::time::Duration;

use mongodb::bson::doc;

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, MongoReplicaSetMarker,
    MongoReplicaSetResource, RegisteredResource, CLUSTER_EVENT_SOURCE,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features mongodb-cluster --test mongo_replica_set_vertical -- --ignored"]
async fn mongo_replica_set_provisions_three_nodes_and_elects_primary() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::MongoReplicaSet(
        MongoReplicaSetResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "mongo_rs_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx
                    .mongo_bindings()
                    .expect("mongo bindings after provision");
                assert!(
                    bindings.connection_url.contains("directConnection=true"),
                    "expected directConnection=true (host-routable URI) in {}",
                    bindings.connection_url
                );
                assert!(
                    bindings.connection_url.contains("127.0.0.1"),
                    "expected loopback host in {}",
                    bindings.connection_url
                );

                let client = mongodb::Client::with_uri_str(bindings.connection_url.as_str())
                    .await
                    .expect("connect");
                let admin = client.database("admin");
                let status = admin
                    .run_command(doc! { "replSetGetStatus": 1 })
                    .await
                    .expect("replSetGetStatus");
                let members = status.get_array("members").expect("members array");
                assert_eq!(members.len(), 3, "expected 3 members");

                let mut primary = 0usize;
                let mut secondary = 0usize;
                for member in members {
                    let doc = member.as_document().expect("member doc");
                    let state = doc.get_str("stateStr").unwrap_or("").to_ascii_uppercase();
                    match state.as_str() {
                        "PRIMARY" => primary += 1,
                        "SECONDARY" => secondary += 1,
                        _ => {}
                    }
                }
                assert_eq!(primary, 1, "expected exactly one PRIMARY");
                assert_eq!(secondary, 2, "expected two SECONDARY");
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario");

    assert!(
        matches!(
            ctx.resources().first(),
            Some(RegisteredResource::Builtin(
                BuiltinResource::MongoReplicaSet(_)
            ))
        ),
        "expected MongoReplicaSet builtin"
    );
}

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features mongodb-cluster --test mongo_replica_set_vertical -- --ignored"]
async fn mongo_replica_set_step_down_elects_different_primary() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::MongoReplicaSet(
        MongoReplicaSetResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "mongo_rs_step_down_ops",
        |ctx: &mut Context| {
            Box::pin(async move {
                let mongo = ctx
                    .mongo_cluster()
                    .expect("mongo cluster handle after provision");
                let before = mongo
                    .current_primary()
                    .await
                    .expect("current primary before step-down");
                mongo
                    .step_down_primary()
                    .await
                    .expect("replSetStepDown");
                let after = mongo
                    .await_new_primary(Duration::from_secs(60))
                    .await
                    .expect("new primary election");
                assert_ne!(
                    before, after,
                    "expected a different node to become primary after step-down (before={before}, after={after})"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario");

    let cluster_events: Vec<_> = event_log
        .events()
        .filter(|event_record| event_record.source == CLUSTER_EVENT_SOURCE)
        .collect();
    assert!(
        cluster_events
            .iter()
            .any(|event_record| event_record.message.contains("step_down_primary")),
        "expected cluster event for step_down_primary in {:?}",
        cluster_events
    );
}

/// Models suite-shared behavior: one scenario stops a member, the next “scenario” restores the
/// cluster via [`shakedown::ClusterHandle::reset_topology`] (single `run_scenario` so the harness
/// does not tear down the suite between steps).
#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features mongodb-cluster --test mongo_replica_set_vertical -- --ignored"]
async fn mongo_replica_set_reset_topology_restores_quorum_after_stop() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::MongoReplicaSet(
        MongoReplicaSetResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "mongo_rs_reset_topology_suite",
        |ctx: &mut Context| {
            Box::pin(async move {
                let cluster = ctx
                    .cluster_handle::<MongoReplicaSetMarker>()
                    .expect("cluster handle after provision");

                cluster.stop_node(1).await.expect("stop node 1");
                assert!(
                    cluster.healthy_node_count().await.expect("count") < 3,
                    "expected a stopped node to reduce healthy count"
                );

                cluster
                    .reset_topology()
                    .await
                    .expect("reset_topology after stop");

                assert_eq!(
                    cluster
                        .healthy_node_count()
                        .await
                        .expect("count after reset"),
                    3,
                    "expected 3/3 containers running after reset_topology"
                );

                let mongo = ctx.mongo_cluster().expect("mongo cluster ops");
                let primary = mongo.current_primary().await.expect("primary after reset");
                assert!(primary < 3, "expected primary index in 0..3, got {primary}");
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario");

    let reset_events: Vec<_> = event_log
        .events()
        .filter(|event_record| {
            event_record.source == CLUSTER_EVENT_SOURCE
                && event_record.message.contains("reset_topology")
        })
        .collect();
    assert!(
        !reset_events.is_empty(),
        "expected cluster event log lines for reset_topology, got {:?}",
        event_log.events().collect::<Vec<_>>()
    );
}
