//! PostgreSQL primary + streaming replica per scenario (`postgres-cluster`).
//!
//! Built only with `--features postgres-cluster`. The test is **`#[ignore]`** so
//! `cargo test --workspace` skips Docker by default.
//!
//! ```text
//! cargo test -p shakedown --features postgres-cluster postgres_replica_set_vertical -- --ignored
//! ```

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, PostgresReplicaSetResource,
    RegisteredResource,
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
