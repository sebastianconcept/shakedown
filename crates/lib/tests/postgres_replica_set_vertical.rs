//! PostgreSQL primary + streaming replica per scenario (`postgres-cluster`).
//!
//! Built only with `--features postgres-cluster`. The test is **`#[ignore]`** so
//! `cargo test --workspace` skips Docker by default.
//!
//! ```text
//! cargo test -p shakedown --features postgres-cluster postgres_replica_set_vertical -- --ignored
//! ```

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, PostgresReplicaSetMarker,
    PostgresReplicaSetResource, RegisteredResource,
};
use tokio::time::{sleep, Duration};
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features postgres-cluster postgres_replica_set_vertical -- --ignored"]
async fn postgres_replica_set_streams_writes_to_replica() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::PostgresReplicaSet(
        PostgresReplicaSetResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "pg_rs_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx
                    .postgres_bindings()
                    .expect("postgres bindings after provision");
                assert_eq!(bindings.host, "127.0.0.1");
                assert!(bindings.port > 0);
                assert!(
                    bindings.database.starts_with("shakedown_"),
                    "expected scenario database prefix"
                );
                assert!(!bindings.connection_url.is_empty());

                let (primary_client, primary_conn) = tokio_postgres::connect(
                    bindings.connection_url.as_str(),
                    NoTls,
                )
                .await
                .expect("connect to primary");
                tokio::spawn(async move {
                    let _ = primary_conn.await;
                });

                primary_client
                    .execute(
                        "CREATE TABLE IF NOT EXISTS rs_proof (id SERIAL PRIMARY KEY, v TEXT NOT NULL)",
                        &[],
                    )
                    .await
                    .expect("create table on primary");
                primary_client
                    .execute(
                        "INSERT INTO rs_proof (v) VALUES ($1)",
                        &[&"from-primary"],
                    )
                    .await
                    .expect("insert on primary");

                let replica_port = bindings
                    .replica_port
                    .expect("replica_port on postgres-cluster bindings");

                let replica_url = format!(
                    "postgresql://{}:{}@127.0.0.1:{}/{}",
                    bindings.username, bindings.password, replica_port, bindings.database
                );

                let mut replicated = false;
                for _ in 0..50 {
                    let (replica_client, replica_conn) =
                        tokio_postgres::connect(replica_url.as_str(), NoTls)
                            .await
                            .expect("connect to replica");
                    tokio::spawn(async move {
                        let _ = replica_conn.await;
                    });
                    match replica_client
                        .query_opt(
                            "SELECT v FROM rs_proof WHERE v = $1",
                            &[&"from-primary"],
                        )
                        .await
                    {
                        Ok(Some(_)) => {
                            replicated = true;
                            break;
                        }
                        Ok(None) => {}
                        Err(ref error)
                            if error.code() == Some(&SqlState::UNDEFINED_TABLE) => {}
                        Err(error) => panic!("select on replica: {error}"),
                    }
                    sleep(Duration::from_millis(200)).await;
                }
                assert!(
                    replicated,
                    "expected row replicated to replica within timeout"
                );

                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::PostgresReplicaSet(_)))
                    ),
                    "expected PostgresReplicaSet builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario succeeds");
}

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features postgres-cluster postgres_replica_set_vertical -- --ignored"]
async fn postgres_replica_set_promote_replica_allows_writes_on_new_primary() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::PostgresReplicaSet(
        PostgresReplicaSetResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "pg_rs_promote_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx
                    .postgres_bindings()
                    .expect("postgres bindings after provision");
                let cluster = ctx
                    .postgres_replica_set_cluster()
                    .expect("postgres replica set cluster");

                assert_eq!(
                    cluster.current_primary().await.expect("initial primary"),
                    0
                );

                let (primary_client, primary_conn) = tokio_postgres::connect(
                    bindings.connection_url.as_str(),
                    NoTls,
                )
                .await
                .expect("connect to initial primary");
                tokio::spawn(async move {
                    let _ = primary_conn.await;
                });
                primary_client
                    .execute(
                        "CREATE TABLE IF NOT EXISTS promote_proof (id SERIAL PRIMARY KEY, v TEXT NOT NULL)",
                        &[],
                    )
                    .await
                    .expect("create table on primary");

                cluster
                    .promote_replica(1)
                    .await
                    .expect("promote replica node 1");

                // `pg_promote` does not shut down the former primary; both can report
                // `pg_is_in_recovery() = false` until the old primary is stopped (split-brain window).
                let cluster_handle = ctx
                    .cluster_handle::<PostgresReplicaSetMarker>()
                    .expect("registered cluster handle");
                cluster_handle
                    .stop_node(0)
                    .await
                    .expect("stop former primary container");

                sleep(Duration::from_secs(2)).await;

                assert_eq!(
                    cluster.current_primary().await.expect("current primary"),
                    1
                );

                let replica_port = bindings
                    .replica_port
                    .expect("replica_port on postgres-cluster bindings");
                let new_primary_url = format!(
                    "postgresql://{}:{}@{}:{}/{}",
                    bindings.username,
                    bindings.password,
                    bindings.host,
                    replica_port,
                    bindings.database
                );

                let (new_primary_client, new_primary_conn) =
                    tokio_postgres::connect(new_primary_url.as_str(), NoTls)
                        .await
                        .expect("connect to new primary");
                tokio::spawn(async move {
                    let _ = new_primary_conn.await;
                });
                new_primary_client
                    .execute(
                        "INSERT INTO promote_proof (v) VALUES ($1)",
                        &[&"after-promotion"],
                    )
                    .await
                    .expect("insert on promoted primary");
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario succeeds");

    let has_promote_event = event_log
        .events()
        .any(|event| event.source == "cluster" && event.message.contains("promote_replica"));
    assert!(
        has_promote_event,
        "expected cluster event for promote_replica"
    );
}
