//! MongoDB replica set (3× `mongod` on a suite bridge network) — provision + `replSet` readiness.
//!
//! Built with `--features mongodb-cluster`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features mongodb-cluster --test mongo_replica_set_vertical -- --ignored
//! ```

use mongodb::bson::doc;

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, MongoReplicaSetResource, RegisteredResource,
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
