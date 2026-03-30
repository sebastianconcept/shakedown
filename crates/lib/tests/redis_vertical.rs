//! Redis suite container + per-scenario key prefix + Context bindings (optional).
//!
//! Built with `--features redis`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features redis --test redis_vertical -- --ignored
//! ```

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, RedisResource, RegisteredResource,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features redis --test redis_vertical -- --ignored"]
async fn redis_provisions_and_sets_bindings() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Redis(
        RedisResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "redis_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx
                    .redis_bindings()
                    .expect("redis bindings after provision");
                assert_eq!(bindings.host, "127.0.0.1");
                assert!(bindings.port > 0);
                assert!(
                    bindings.key_prefix.starts_with("shakedown:"),
                    "expected scenario key prefix"
                );
                assert!(
                    bindings.connection_url.starts_with("redis://"),
                    "expected redis URL"
                );
                ctx.redis()
                    .set("vertical_k", "vertical_v")
                    .await
                    .expect("set via helpers");
                ctx.redis()
                    .assert_key_equals("vertical_k", "vertical_v")
                    .await
                    .expect("assert value");
                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::Redis(_)))
                    ),
                    "expected Redis builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
