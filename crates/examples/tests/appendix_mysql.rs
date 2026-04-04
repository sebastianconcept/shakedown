//! Appendix K: Oracle MySQL — SQL execute and row assertions
//!
//! **Resources:** MySQL (Docker)
//! **Assertions:** `execute`, `assert_row_exists`, `assert_row_count`
//! **Teaching intent:** Same relational card as MariaDB, routed through Oracle MySQL images when
//!   production parity demands it.
//!
//! Run: `cargo test -p shakedown-examples --features mysql --test appendix_mysql -- --ignored`

#[shakedown::test(resources = [Mysql])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features mysql --test appendix_mysql -- --ignored"]
async fn mysql_order_row_execute_and_assert(ctx: &mut shakedown::Context) {
    ctx.mysql()
        .execute(
            "CREATE TABLE orders (\
                id VARCHAR(64) PRIMARY KEY, \
                customer_id VARCHAR(255) NOT NULL, \
                items TEXT NOT NULL, \
                status VARCHAR(64) NOT NULL DEFAULT 'created'\
            )",
            (),
        )
        .await
        .expect("create orders table");

    let order_id = "order-mysql-1";
    let customer_id = "customer-mysql-1";
    let items_json = r#"[{"product_id":"sku-7","quantity":1}]"#;

    ctx.mysql()
        .execute(
            "INSERT INTO orders (id, customer_id, items, status) VALUES (?, ?, ?, ?)",
            (order_id, customer_id, items_json, "created"),
        )
        .await
        .expect("insert order");

    ctx.mysql()
        .assert_row_exists(
            "SELECT 1 FROM orders WHERE id = ? AND customer_id = ?",
            (order_id, customer_id),
        )
        .await
        .expect("order row exists");

    ctx.mysql()
        .assert_row_count("SELECT COUNT(*) FROM orders WHERE id = ?", (order_id,), 1)
        .await
        .expect("exactly one matching order");
}
