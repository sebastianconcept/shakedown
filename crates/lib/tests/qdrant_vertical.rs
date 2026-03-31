//! Qdrant suite container + per-scenario collection prefix + Context bindings (optional).
//!
//! Built with `--features qdrant`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features qdrant --test qdrant_vertical -- --ignored
//! ```

use qdrant_client::qdrant::Distance;
use qdrant_client::Payload;

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, QdrantResource, RegisteredResource,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features qdrant --test qdrant_vertical -- --ignored"]
async fn qdrant_provisions_and_sets_bindings() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Qdrant(
        QdrantResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "qdrant_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx
                    .qdrant_bindings()
                    .expect("qdrant bindings after provision");
                assert_eq!(bindings.host, "127.0.0.1");
                assert!(bindings.rest_port > 0);
                assert!(bindings.grpc_port > 0);
                assert!(bindings.rest_url.starts_with("http://127.0.0.1:"));
                assert!(bindings.grpc_url.starts_with("http://127.0.0.1:"));
                assert!(
                    bindings.collection_prefix.starts_with("shakedown_"),
                    "expected scenario collection prefix"
                );
                ctx.qdrant()
                    .create_collection("vertical_col", 4, Distance::Cosine)
                    .await
                    .expect("create_collection");
                ctx.qdrant()
                    .upsert_points(
                        "vertical_col",
                        vec![qdrant_client::qdrant::PointStruct::new(
                            1,
                            vec![0.0_f32, 0.0_f32, 0.0_f32, 1.0_f32],
                            Payload::default(),
                        )],
                    )
                    .await
                    .expect("upsert");
                ctx.qdrant()
                    .assert_point_exists("vertical_col", 1)
                    .await
                    .expect("assert point");
                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::Qdrant(_)))
                    ),
                    "expected Qdrant builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
