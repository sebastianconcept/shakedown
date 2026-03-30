//! NATS suite broker + per-scenario subject prefix + Context bindings (optional).
//!
//! Built with `--features nats`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features nats --test nats_vertical -- --ignored
//! ```

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, NatsResource, RegisteredResource,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features nats --test nats_vertical -- --ignored"]
async fn nats_provisions_and_sets_bindings() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Nats(
        NatsResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "nats_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx.nats_bindings().expect("nats bindings after provision");
                assert_eq!(bindings.host, "127.0.0.1");
                assert!(bindings.port > 0);
                assert!(
                    bindings.subject_prefix.starts_with("shakedown."),
                    "expected scenario subject prefix"
                );
                assert!(
                    bindings.subject_prefix.ends_with('.'),
                    "expected trailing dot on prefix"
                );
                assert!(
                    bindings.connection_url.starts_with("nats://"),
                    "expected nats URL"
                );
                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::Nats(_)))
                    ),
                    "expected Nats builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
