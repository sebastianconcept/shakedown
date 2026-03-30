//! Valkey suite container + shared Redis bindings/helpers (optional).
//!
//! Built with `--features valkey`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features valkey --test valkey_vertical -- --ignored
//! ```

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, RegisteredResource, ValkeyResource,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features valkey --test valkey_vertical -- --ignored"]
async fn valkey_provisions_and_sets_bindings() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Valkey(
        ValkeyResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "valkey_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx
                    .redis_bindings()
                    .expect("redis bindings after Valkey provision");
                assert_eq!(bindings.host, "127.0.0.1");
                assert!(bindings.port > 0);
                ctx.redis()
                    .set("valkey_k", "valkey_v")
                    .await
                    .expect("set via helpers");
                ctx.redis()
                    .assert_key_equals("valkey_k", "valkey_v")
                    .await
                    .expect("assert value");
                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::Valkey(_)))
                    ),
                    "expected Valkey builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
