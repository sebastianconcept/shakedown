//! Appendix D: OpenSearch — index, search, and document assertions (OpenSearch types)
//!
//! **Resources:** OpenSearch (Docker)
//! **Assertions:** `index_document`, `search`, `assert_document_indexed`, `assert_search_hits`
//! **Teaching intent:** Same workflow as Elasticsearch, using the OpenSearch-backed helpers so
//!   teams on AWS OpenSearch Service can copy this card verbatim.
//!
//! Run: `cargo test -p shakedown-examples --features opensearch --test appendix_opensearch -- --ignored`

use serde_json::json;

#[shakedown::test(resources = [OpenSearch])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features opensearch --test appendix_opensearch -- --ignored"]
async fn opensearch_order_catalog_index_and_search(ctx: &mut shakedown::Context) {
    let logical_index = "orders";
    let document_id = "order-os-1";

    ctx.opensearch()
        .index_document(
            logical_index,
            document_id,
            json!({
                "order_id": document_id,
                "customer_id": "customer-os-1",
                "status": "awaiting_fulfillment",
            }),
        )
        .await
        .expect("index order document");

    ctx.opensearch()
        .assert_document_indexed(logical_index, document_id)
        .await
        .expect("GET should find the indexed order");

    let match_query = json!({
        "query": { "match": { "customer_id": "customer-os-1" } }
    });
    let search_result = ctx
        .opensearch()
        .search(logical_index, match_query.clone())
        .await
        .expect("search by customer_id");

    assert!(
        search_result.total_hits >= 1,
        "expected at least one hit, got {}",
        search_result.total_hits
    );

    ctx.opensearch()
        .assert_search_hits(logical_index, match_query, 1)
        .await
        .expect("assert_search_hits after refresh");
}
