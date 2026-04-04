//! Appendix J: MariaDB — SQL execute and row assertions
//!
//! **Resources:** MariaDB (Docker)
//! **Assertions:** `execute`, `assert_row_exists`, `assert_row_count`
//! **Teaching intent:** Mirror the relational patterns from Postgres chapters against MariaDB using
//!   the same order-centric language.
//!
//! Run: `cargo test -p shakedown-examples --features mariadb --test appendix_mariadb -- --ignored`

#[shakedown::test(resources = [MariaDB])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features mariadb --test appendix_mariadb -- --ignored"]
async fn mariadb_order_row_execute_and_assert(ctx: &mut shakedown::Context) {
    ctx.mariadb()
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

    let order_id = "order-mariadb-1";
    let customer_id = "customer-mariadb-1";
    let items_json = r#"[{"product_id":"sku-1","quantity":2}]"#;

    ctx.mariadb()
        .execute(
            "INSERT INTO orders (id, customer_id, items, status) VALUES (?, ?, ?, ?)",
            (order_id, customer_id, items_json, "created"),
        )
        .await
        .expect("insert order");

    ctx.mariadb()
        .assert_row_exists(
            "SELECT 1 FROM orders WHERE id = ? AND customer_id = ?",
            (order_id, customer_id),
        )
        .await
        .expect("order row exists");

    ctx.mariadb()
        .assert_row_count("SELECT COUNT(*) FROM orders WHERE id = ?", (order_id,), 1)
        .await
        .expect("exactly one matching order");
}
