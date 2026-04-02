//! Capstone: MongoDB replica set + open-model load + primary step-down (US-019).
//!
//! Requires Docker. Run with:
//!
//! ```text
//! cargo test -p shakedown --features mongodb-cluster,load --test mongo_cluster_load_disruption -- --ignored
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use serde_json::json;

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, LoadPhase, LoadProfile,
    MongoReplicaSetResource, Rate, CLUSTER_EVENT_SOURCE,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features mongodb-cluster,load --test mongo_cluster_load_disruption -- --ignored"]
async fn mongo_replica_set_load_step_down_meets_window_and_recovery_assertions() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::MongoReplicaSet(
        MongoReplicaSetResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "mongo_rs_capstone_load_disruption",
        |ctx: &mut Context| {
            Box::pin(async move {
                ctx.mongo()
                    .insert_document("us019_seed", json!({ "seed": true }))
                    .await
                    .expect("seed insert");

                let bindings = ctx.mongo_bindings().expect("mongo bindings after provision");
                let database = bindings.database.clone();
                // Surface primary step-down / election to the load closure: default driver retries
                // would hide transient errors and prevent `await_recovery` + `recovery_time` from
                // observing the disruption.
                let mut client_options =
                    ClientOptions::parse(bindings.connection_url.as_str()).await.expect("parse mongo uri");
                client_options.retry_writes = Some(false);
                client_options.retry_reads = Some(false);
                let client_cell = Arc::new(tokio::sync::RwLock::new(
                    mongodb::Client::with_options(client_options).expect("mongo client with options"),
                ));

                let profile = LoadProfile::open_model()
                    .phase(
                        LoadPhase::ramp(
                            Duration::from_secs(10),
                            Rate::per_second(1.0),
                            Rate::per_second(100.0),
                        )
                        .named("ramp_up"),
                    )
                    .phase(
                        LoadPhase::constant(Duration::from_secs(30), Rate::per_second(100.0))
                            .named("baseline"),
                    )
                    .phase(
                        LoadPhase::constant(Duration::from_secs(60), Rate::per_second(100.0))
                            .named("disruption"),
                    )
                    .phase(
                        LoadPhase::ramp(
                            Duration::from_secs(10),
                            Rate::per_second(100.0),
                            Rate::per_second(1.0),
                        )
                        .named("ramp_down"),
                    )
                    .workers(50)
                    .max_workers(100)
                    .build()
                    .expect("load profile");

                let collection_name = "us019_writes";
                let counter = Arc::new(AtomicU64::new(0));

                let load_handle = ctx
                    .start_load("mongo_rs_writes", profile, {
                        let client_cell = client_cell.clone();
                        let database = database.clone();
                        let collection_name = collection_name.to_string();
                        let counter = counter.clone();
                        move || {
                            let client_cell = client_cell.clone();
                            let database = database.clone();
                            let collection_name = collection_name.clone();
                            let counter = counter.clone();
                            async move {
                                let client = client_cell.read().await.clone();
                                let index = counter.fetch_add(1, Ordering::Relaxed);
                                let collection = client
                                    .database(database.as_str())
                                    .collection::<mongodb::bson::Document>(collection_name.as_str());
                                collection
                                    .insert_one(doc! { "i": index as i64 })
                                    .await
                                    .map_err(|e| e.to_string())?;
                                Ok::<(), String>(())
                            }
                        }
                    })
                    .await
                    .expect("start_load");

                load_handle
                    .await_phase("disruption")
                    .await
                    .expect("await disruption phase");

                let disruption_window_start = std::time::Instant::now();
                let disruption_window_end = disruption_window_start + Duration::from_secs(60);

                let mongo = ctx.mongo_cluster().expect("mongo cluster handle");
                mongo
                    .step_down_primary()
                    .await
                    .expect("step_down_primary");
                let new_primary_index = mongo
                    .await_new_primary(Duration::from_secs(30))
                    .await
                    .expect("await_new_primary");

                let bindings = ctx.mongo_bindings().expect("mongo bindings");
                let host_ports = bindings.replica_set_host_ports.as_slice();
                assert_eq!(
                    host_ports.len(),
                    3,
                    "expected replica_set_host_ports from provision metadata"
                );
                let port = host_ports[new_primary_index];
                let uri = format!(
                    "mongodb://{}:{}/{}?directConnection=true&w=1",
                    bindings.host, port, bindings.database
                );
                let mut refreshed_options = ClientOptions::parse(uri.as_str()).await.expect("parse");
                refreshed_options.retry_writes = Some(false);
                refreshed_options.retry_reads = Some(false);
                *client_cell.write().await =
                    mongodb::Client::with_options(refreshed_options).expect("refreshed client");

                load_handle
                    .await_recovery(Duration::from_secs(10))
                    .await
                    .expect("await_recovery after election");

                let metrics = load_handle.stop().await.expect("load stop");

                let err_during = metrics.error_rate_during(&(disruption_window_start..disruption_window_end));
                assert!(
                    err_during < 0.50,
                    "expected disruption-window error rate below 50%, got {err_during} (failures={}, completed={})",
                    metrics.failed_operations(),
                    metrics.successful_operations() + metrics.failed_operations()
                );

                let recovery = metrics.recovery_time().expect(
                    "expected recovery_time after transient write failures during election",
                );
                assert!(
                    recovery < Duration::from_secs(5),
                    "expected recovery within 5s, got {recovery:?}"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario");

    let correlated: Vec<_> = event_log
        .events()
        .filter(|record| record.source == CLUSTER_EVENT_SOURCE || record.source == "load")
        .collect();

    assert!(
        correlated.iter().all(|record| record.timestamp > 0),
        "expected positive timestamps on cluster/load timeline rows"
    );
    assert!(
        correlated
            .iter()
            .any(|record| record.source == CLUSTER_EVENT_SOURCE),
        "expected cluster topology events"
    );
    assert!(
        correlated.iter().any(|record| record.source == "load"),
        "expected load driver events"
    );
}
