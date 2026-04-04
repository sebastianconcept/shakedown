//! Appendix C: Elasticsearch — index, search, and document assertions
//!
//! **Resources:** Elasticsearch (Docker)
//! **Assertions:** `index_document`, `search`, `assert_document_indexed`, `assert_search_hits`
//! **Teaching intent:** Treat search indices like any other scenario-scoped resource — index order
//!   documents under a logical index name and assert retrieval after refresh.
//!
//! Run: `cargo test -p shakedown-examples --features elasticsearch --test appendix_elasticsearch -- --ignored`

use serde_json::json;

#[shakedown::test(resources = [Elasticsearch])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features elasticsearch --test appendix_elasticsearch -- --ignored"]
async fn elasticsearch_order_catalog_index_and_search(ctx: &mut shakedown::Context) {
    let logical_index = "orders";
    let document_id = "order-es-1";

    ctx.elasticsearch()
        .index_document(
            logical_index,
            document_id,
            json!({
                "order_id": document_id,
                "customer_id": "customer-es-1",
                "status": "awaiting_fulfillment",
            }),
        )
        .await
        .expect("index order document");

    ctx.elasticsearch()
        .assert_document_indexed(logical_index, document_id)
        .await
        .expect("GET should find the indexed order");

    let match_query = json!({
        "query": { "match": { "customer_id": "customer-es-1" } }
    });
    let search_result = ctx
        .elasticsearch()
        .search(logical_index, match_query.clone())
        .await
        .expect("search by customer_id");

    assert!(
        search_result.total_hits >= 1,
        "expected at least one hit, got {}",
        search_result.total_hits
    );

    ctx.elasticsearch()
        .assert_search_hits(logical_index, match_query, 1)
        .await
        .expect("assert_search_hits after refresh");
}
