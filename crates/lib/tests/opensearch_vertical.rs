//! OpenSearch suite container + per-scenario index prefix + Context bindings (optional).
//!
//! Built with `--features opensearch`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features opensearch --test opensearch_vertical -- --ignored
//! ```

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, OpenSearchResource, RegisteredResource,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features opensearch --test opensearch_vertical -- --ignored"]
async fn opensearch_provisions_and_sets_bindings() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::OpenSearch(
        OpenSearchResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "opensearch_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx
                    .opensearch_bindings()
                    .expect("opensearch bindings after provision");
                assert_eq!(bindings.host, "127.0.0.1");
                assert!(bindings.port > 0);
                assert!(bindings.url.starts_with("http://127.0.0.1:"));
                assert!(
                    bindings.index_prefix.starts_with("shakedown_"),
                    "expected scenario index prefix"
                );
                ctx.opensearch()
                    .index_document(
                        "vertical_idx",
                        "doc1",
                        serde_json::json!({ "title": "vertical", "n": 42 }),
                    )
                    .await
                    .expect("index via helpers");
                ctx.opensearch()
                    .assert_document_indexed("vertical_idx", "doc1")
                    .await
                    .expect("assert indexed");
                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::OpenSearch(_)))
                    ),
                    "expected OpenSearch builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
