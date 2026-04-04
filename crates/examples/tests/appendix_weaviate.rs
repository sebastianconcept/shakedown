//! Appendix F: Weaviate — schema, objects, and class-level counts
//!
//! **Resources:** Weaviate (Docker)
//! **Assertions:** `create_class`, `create_object`, `assert_object_exists`, `assert_class_count`
//! **Teaching intent:** Register a minimal `OrderSnapshot` class and prove objects round-trip through
//!   the REST API with scenario-prefixed class names.
//!
//! Run: `cargo test -p shakedown-examples --features weaviate --test appendix_weaviate -- --ignored`

use serde_json::json;

#[shakedown::test(resources = [Weaviate])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features weaviate --test appendix_weaviate -- --ignored"]
async fn weaviate_order_snapshot_class_and_object(ctx: &mut shakedown::Context) {
    ctx.weaviate()
        .create_class(json!({
            "class": "OrderSnapshot",
            "vectorizer": "none",
            "properties": [
                { "name": "order_id", "dataType": ["text"] },
                { "name": "status", "dataType": ["text"] }
            ]
        }))
        .await
        .expect("create OrderSnapshot class");

    let object_id = ctx
        .weaviate()
        .create_object(
            "OrderSnapshot",
            json!({
                "order_id": "order-weaviate-1",
                "status": "paid"
            }),
        )
        .await
        .expect("insert order snapshot object");

    ctx.weaviate()
        .assert_object_exists(object_id.as_str())
        .await
        .expect("GET object by id");

    ctx.weaviate()
        .assert_class_count("OrderSnapshot", 1)
        .await
        .expect("single object in class");
}
