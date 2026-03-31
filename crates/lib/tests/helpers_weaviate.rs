//! Weaviate typed helpers: schema + object round-trip (Docker).
//!
//! Built with `--features weaviate`. **`#[ignore]`** unless `--ignored`.
//!
//! ```text
//! cargo test -p shakedown --features weaviate --test helpers_weaviate -- --ignored
//! ```

use serde_json::json;

#[shakedown::test(resources = [Weaviate])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features weaviate --test helpers_weaviate -- --ignored"]
async fn weaviate_helpers_class_object(ctx: &mut shakedown::Context) {
    ctx.weaviate()
        .create_class(json!({
            "class": "HArticle",
            "vectorizer": "none",
            "properties": [{ "name": "title", "dataType": ["text"] }]
        }))
        .await
        .expect("create_class");
    let object_id = ctx
        .weaviate()
        .create_object("HArticle", json!({ "title": "hello-weaviate" }))
        .await
        .expect("create_object");
    ctx.weaviate()
        .assert_object_exists(object_id.as_str())
        .await
        .expect("assert_object_exists");
    ctx.weaviate()
        .assert_class_count("HArticle", 1)
        .await
        .expect("assert_class_count");
}
