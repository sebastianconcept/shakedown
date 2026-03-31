//! Weaviate suite container + per-scenario class prefix + Context bindings (optional).
//!
//! Built with `--features weaviate`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features weaviate --test weaviate_vertical -- --ignored
//! ```

use serde_json::json;

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, RegisteredResource, WeaviateResource,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features weaviate --test weaviate_vertical -- --ignored"]
async fn weaviate_provisions_and_sets_bindings() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Weaviate(
        WeaviateResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "weaviate_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx
                    .weaviate_bindings()
                    .expect("weaviate bindings after provision");
                assert_eq!(bindings.host, "127.0.0.1");
                assert!(bindings.port > 0);
                assert!(bindings.rest_url.starts_with("http://127.0.0.1:"));
                assert!(
                    bindings.class_prefix.starts_with("shakedown_"),
                    "expected scenario class prefix"
                );
                ctx.weaviate()
                    .create_class(json!({
                        "class": "VerticalThing",
                        "vectorizer": "none",
                        "properties": [
                            { "name": "title", "dataType": ["text"] }
                        ]
                    }))
                    .await
                    .expect("create_class");
                let object_id = ctx
                    .weaviate()
                    .create_object("VerticalThing", json!({ "title": "proof" }))
                    .await
                    .expect("create_object");
                ctx.weaviate()
                    .assert_object_exists(object_id.as_str())
                    .await
                    .expect("assert_object_exists");
                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::Weaviate(_)))
                    ),
                    "expected Weaviate builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
