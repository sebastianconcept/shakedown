//! MongoDB suite container + per-scenario database + Context bindings (optional).
//!
//! Built with `--features mongodb`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features mongodb --test mongo_vertical -- --ignored
//! ```

use mongodb::bson::doc;

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, MongoResource, RegisteredResource,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features mongodb --test mongo_vertical -- --ignored"]
async fn mongo_provisions_and_sets_bindings() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Mongo(
        MongoResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "mongo_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx
                    .mongo_bindings()
                    .expect("mongo bindings after provision");
                assert_eq!(bindings.host, "127.0.0.1");
                assert!(bindings.port > 0);
                assert!(
                    bindings.database.starts_with("shakedown_"),
                    "expected scenario database prefix"
                );
                assert!(
                    bindings.connection_url.contains("authSource=admin"),
                    "expected authSource in URI"
                );
                ctx.mongo()
                    .expect("mongodb helpers")
                    .insert_document(
                        "vertical_coll",
                        serde_json::json!({ "vertical_field": "vertical_value" }),
                    )
                    .await
                    .expect("insert via helpers");
                ctx.mongo()
                    .expect("mongodb helpers")
                    .assert_document_exists(
                        "vertical_coll",
                        doc! { "vertical_field": "vertical_value" },
                    )
                    .await
                    .expect("assert document");
                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::Mongo(_)))
                    ),
                    "expected Mongo builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
