//! OpenSearch typed helpers: index, refresh, search (Docker).
//!
//! Built with `--features opensearch`. **`#[ignore]`** unless `--ignored`.
//!
//! ```text
//! cargo test -p shakedown --features opensearch --test helpers_opensearch -- --ignored
//! ```

#[shakedown::test(resources = [OpenSearch])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features opensearch --test helpers_opensearch -- --ignored"]
async fn opensearch_helpers_index_search(ctx: &mut shakedown::Context) {
    ctx.opensearch()
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
    ctx.opensearch()
        .assert_search_hits("docs", query, 1)
        .await
        .expect("assert_search_hits");
}
