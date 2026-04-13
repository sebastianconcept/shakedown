//! Elasticsearch typed helpers: index, refresh, search (Docker).
//!
//! Built with `--features elasticsearch`. **`#[ignore]`** unless `--ignored`.
//!
//! ```text
//! cargo test -p shakedown --features elasticsearch --test helpers_elasticsearch -- --ignored
//! ```

#[shakedown::test(resources = [Elasticsearch])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features elasticsearch --test helpers_elasticsearch -- --ignored"]
async fn elasticsearch_helpers_index_search(ctx: &mut shakedown::Context) {
    ctx.elasticsearch()
        .expect("elasticsearch helpers")
        .index_document(
            "docs",
            "a1",
            serde_json::json!({ "title": "alpha", "k": "v" }),
        )
        .await
        .expect("index_document");
    let query = serde_json::json!({
        "query": { "match": { "title": "alpha" } }
    });
    ctx.elasticsearch()
        .expect("elasticsearch helpers")
        .assert_search_hits("docs", query, 1)
        .await
        .expect("assert_search_hits");
}
