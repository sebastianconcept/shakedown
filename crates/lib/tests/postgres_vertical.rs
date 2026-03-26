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
