//! Chapter 6: MongoDB documents for fulfillments
//!
//! **Resources:** MongoDB (Docker)
//! **Assertions:** `insert_document`, `find_documents`, `assert_document_exists`, `assert_document_count`
//! **Concepts:** JSON documents aligned with the fulfillment service, BSON filters via `doc!`
//! **Teaching intent:** Persist and query fulfillment records the same way the companion service will,
//!   without standing up that service yet.
//!
//! Run with Docker: `cargo test -p shakedown-examples --features mongodb --test ch06_documents_mongo -- --ignored`

use mongodb::bson::doc;
use serde_json::json;

#[shakedown::test(resources = [Mongo])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features mongodb --test ch06_documents_mongo -- --ignored"]
async fn mongodb_fulfillment_documents_insert_find_and_assert(ctx: &mut shakedown::Context) {
    let fulfillment_doc = json!({
        "_id": "fulfillment-ch06-1",
        "order_id": "order-ch06-1",
        "status": "fulfilled",
        "lines": [
            { "sku": "widget-1", "quantity": 2 }
        ]
    });

    ctx.mongo()
        .insert_document("fulfillments", fulfillment_doc.clone())
        .await
        .expect("insert fulfillment document");

    let filter = doc! { "order_id": "order-ch06-1" };
    let found = ctx
        .mongo()
        .find_documents("fulfillments", filter.clone())
        .await
        .expect("find_documents by order_id");

    assert_eq!(found.len(), 1);
    assert_eq!(
        found[0].get_str("status").expect("status field"),
        "fulfilled"
    );

    ctx.mongo()
        .assert_document_exists("fulfillments", filter.clone())
        .await
        .expect("at least one fulfillment for the order");

    ctx.mongo()
        .assert_document_count("fulfillments", filter, 1)
        .await
        .expect("no duplicate fulfillment rows for the same order");
}
