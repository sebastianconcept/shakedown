//! MongoDB typed helpers: insert + assert (Docker).
//!
//! Built with `--features mongodb`. **`#[ignore]`** unless `--ignored`.
//!
//! ```text
//! cargo test -p shakedown --features mongodb --test helpers_mongo -- --ignored
//! ```

use mongodb::bson::doc;

#[shakedown::test(resources = [Mongo])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features mongodb --test helpers_mongo -- --ignored"]
async fn mongo_helpers_insert_then_assert(ctx: &mut shakedown::Context) {
    ctx.mongo()
        .insert_document("items", serde_json::json!({ "name": "alpha", "n": 1 }))
        .await
        .expect("insert_document");
    ctx.mongo()
        .assert_document_exists("items", doc! { "name": "alpha" })
        .await
        .expect("assert_document_exists");
    let found = ctx
        .mongo()
        .find_documents("items", doc! { "name": "alpha" })
        .await
        .expect("find_documents");
    assert_eq!(found.len(), 1);
    ctx.mongo()
        .assert_document_count("items", doc! {}, 1)
        .await
        .expect("assert_document_count");
}
