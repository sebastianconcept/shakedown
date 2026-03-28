//! Toxiproxy suite container + control API readiness (US-002).
//!
//! This target is built only with `--features fault-injection`. The test is **`#[ignore]`** so
//! `cargo test --workspace` skips Docker by default.
//!
//! With Docker available:
//!
//! ```text
//! cargo test -p shakedown --features fault-injection --test toxiproxy_vertical -- --ignored
//! ```

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, RegisteredResource, ToxiproxyResource,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features fault-injection --test toxiproxy_vertical -- --ignored"]
async fn toxiproxy_provisions_version_ready_and_teardown_clears_proxies() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Toxiproxy(
        ToxiproxyResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "toxiproxy_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async move {
                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::Toxiproxy(_)))
                    ),
                    "expected Toxiproxy builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
