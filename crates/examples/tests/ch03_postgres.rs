//! Chapter 3: Postgres schema seeding and row assertions
//!
//! **Resources:** Postgres (Docker)
//! **Assertions:** `execute`, `query_one`, `assert_row_exists`, `assert_row_count`
//! **Concepts:** shared DDL from `shakedown_examples`, parameterized queries
//! **Teaching intent:** Treat Postgres like production — seed schema, insert domain rows, query and
//!   assert with harness helpers.
//!
//! Run with Docker: `cargo test -p shakedown-examples --features postgres --test ch03_postgres -- --ignored`

use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::test(resources = [Postgres])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features postgres --test ch03_postgres -- --ignored"]
async fn postgres_seed_query_and_assert_orders(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("apply orders DDL from shared fixture");

    let order_body = sample_order("customer-ch03");
    let order_id = "order-ch03-1";
    let customer_id = order_body["customer_id"]
        .as_str()
        .expect("customer_id in sample order payload");
    let items_json = serde_json::to_string(&order_body["items"]).expect("items json");
    let status = "created";

    ctx.postgres()
        .execute(
            "INSERT INTO orders (id, customer_id, items, status) VALUES ($1, $2, $3, $4)",
            &[&order_id, &customer_id, &items_json, &status],
        )
        .await
        .expect("insert order row");

    let row = ctx
        .postgres()
        .query_one(
            "SELECT customer_id, status FROM orders WHERE id = $1",
            &[&order_id],
        )
        .await
        .expect("query_one returns the inserted order");

    let customer_id: String = row.get(0);
    let status: String = row.get(1);
    assert_eq!(customer_id, "customer-ch03");
    assert_eq!(status, "created");

    ctx.postgres()
        .assert_row_exists(
            "SELECT 1 FROM orders WHERE id = $1 AND customer_id = $2",
            &[&order_id, &"customer-ch03"],
        )
        .await
        .expect("assert_row_exists for placed order");

    ctx.postgres()
        .assert_row_count(
            "SELECT * FROM orders WHERE customer_id = $1",
            &[&"customer-ch03"],
            1,
        )
        .await
        .expect("exactly one order for this customer");
}
