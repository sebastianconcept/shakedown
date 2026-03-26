//! Postgres suite container + per-scenario database + Context bindings (optional).
//!
//! This target is built only with `--features postgres`. The test is **`#[ignore]`** so
//! `cargo test --workspace` skips Docker by default.
//!
//! With Docker available:
//!
//! ```text
//! cargo test -p shakedown --features postgres postgres_vertical -- --ignored
//! ```

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, PostgresResource, RegisteredResource,
};
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features postgres postgres_vertical -- --ignored"]
async fn postgres_provisions_and_sets_bindings() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Postgres(
        PostgresResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "pg_vertical_proof",
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
                let (postgres_client, postgres_connection) = tokio_postgres::connect(
                    bindings.connection_url.as_str(),
                    NoTls,
                )
                .await
                .expect("connect to scenario postgres database");
                tokio::spawn(async move {
                    let _ = postgres_connection.await;
                });
                postgres_client
                    .execute(
                        "CREATE TABLE shakedown_items (id SERIAL PRIMARY KEY, value_text TEXT NOT NULL)",
                        &[],
                    )
                    .await
                    .expect("create table");
                postgres_client
                    .execute(
                        "INSERT INTO shakedown_items (value_text) VALUES ($1)",
                        &[&"hello-postgres"],
                    )
                    .await
                    .expect("insert row");
                let selected_value: String = postgres_client
                    .query_one(
                        "SELECT value_text FROM shakedown_items WHERE id = 1",
                        &[],
                    )
                    .await
                    .expect("read inserted row")
                    .get(0);
                assert_eq!(selected_value.as_str(), "hello-postgres");
                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::Postgres(_)))
                    ),
                    "expected Postgres builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
