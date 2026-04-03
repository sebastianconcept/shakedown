//! Inter-node Toxiproxy mesh for a MongoDB replica set: `N×(N−1)` proxies, `disconnect_node` /
//! `reconnect_node` / `reset_topology` without stopping `mongod` containers.
//!
//! ```text
//! cargo test -p shakedown --features mongodb-cluster,fault-injection --test mongo_replica_set_inter_node_toxiproxy -- --ignored
//! ```

use std::time::Duration;

use mongodb::bson::doc;

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, MongoReplicaSetMarker,
    MongoReplicaSetResource, RegisteredResource, ToxiproxyResource, CLUSTER_EVENT_SOURCE,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features mongodb-cluster,fault-injection --test mongo_replica_set_inter_node_toxiproxy -- --ignored"]
async fn mongo_replica_set_inter_node_mesh_partition_rejoin() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Toxiproxy(
        ToxiproxyResource::new().with_image_tag_from_env(),
    ));
    ctx.register_builtin(BuiltinResource::MongoReplicaSet(
        MongoReplicaSetResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "mongo_rs_inter_node_mesh",
        |ctx: &mut Context| {
            Box::pin(async move {
                let bindings = ctx
                    .mongo_bindings()
                    .expect("mongo bindings after provision + mesh finalize");
                assert!(
                    bindings.connection_url.contains("directConnection=true"),
                    "expected directConnection URI {}",
                    bindings.connection_url
                );

                let client = mongodb::Client::with_uri_str(bindings.connection_url.as_str())
                    .await
                    .expect("connect");
                let admin = client.database("admin");
                let status_before = admin
                    .run_command(doc! { "replSetGetStatus": 1 })
                    .await
                    .expect("replSetGetStatus");
                let members_before = status_before.get_array("members").expect("members");
                assert_eq!(members_before.len(), 3);

                let mut victim_index: Option<usize> = None;
                for member in members_before {
                    let doc = member.as_document().expect("member doc");
                    if doc.get_str("stateStr").unwrap_or("") != "SECONDARY" {
                        continue;
                    }
                    let id = doc.get_i32("_id").expect("member _id");
                    victim_index = Some(id as usize);
                    break;
                }
                let victim_index =
                    victim_index.expect("expected at least one SECONDARY for partition target");

                // Toxiproxy disables all directed proxies involving `victim_index`.
                ctx.cluster_handle::<MongoReplicaSetMarker>()
                    .expect("cluster handle")
                    .disconnect_node(victim_index)
                    .await
                    .expect("disconnect_node");

                tokio::time::sleep(Duration::from_secs(4)).await;

                ctx.cluster_handle::<MongoReplicaSetMarker>()
                    .expect("cluster handle")
                    .reconnect_node(victim_index)
                    .await
                    .expect("reconnect_node");

                ctx.cluster_handle::<MongoReplicaSetMarker>()
                    .expect("cluster handle")
                    .reset_topology()
                    .await
                    .expect("reset_topology");

                let status_after = admin
                    .run_command(doc! { "replSetGetStatus": 1 })
                    .await
                    .expect("replSetGetStatus after reset");
                let members_after = status_after.get_array("members").expect("members");
                assert_eq!(members_after.len(), 3);
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario");

    let cluster_events: Vec<_> = event_log
        .events()
        .filter(|event| event.source == CLUSTER_EVENT_SOURCE)
        .collect();
    assert!(
        !cluster_events.is_empty(),
        "expected cluster events in log for topology ops"
    );

    assert!(
        matches!(
            ctx.resources().get(1),
            Some(RegisteredResource::Builtin(
                BuiltinResource::MongoReplicaSet(_)
            ))
        ),
        "expected MongoReplicaSet builtin"
    );
}
