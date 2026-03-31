//! TimescaleDB suite container + per-scenario database + Context bindings.
//!
//! Built with `--features timescaledb`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features timescaledb --test timescaledb_vertical -- --ignored
//! ```

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, RegisteredResource, TimescaledbResource,
};
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features timescaledb --test timescaledb_vertical -- --ignored"]
async fn timescaledb_provisions_and_sets_bindings() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Timescaledb(
        TimescaledbResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "ts_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx
                    .timescaledb_bindings()
                    .expect("timescaledb bindings after provision");
                assert_eq!(bindings.host, "127.0.0.1");
                assert!(bindings.port > 0);
                assert!(
                    bindings.database.starts_with("shakedown_"),
                    "expected scenario database prefix"
                );
                assert!(!bindings.connection_url.is_empty());
                let (client, connection) =
                    tokio_postgres::connect(bindings.connection_url.as_str(), NoTls)
                        .await
                        .expect("connect to scenario timescaledb database");
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                let one: i32 = client
                    .query_one("SELECT 1", &[])
                    .await
                    .expect("select 1")
                    .get(0);
                assert_eq!(one, 1);
                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::Timescaledb(_)))
                    ),
                    "expected Timescaledb builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
