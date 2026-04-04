//! Chapter 1: Your first shakedown scenario
//!
//! **Resources:** SQLite (no Docker)
//! **Assertions:** `assert_row_exists`, `assert_row_count`
//! **Concepts:** manual `run_scenario` lifecycle, `Context::new()`, `EventLog`,
//!   provision → readiness → seed → run → assert → teardown
//!
//! This is the only chapter that uses `run_scenario` directly. Later chapters use
//! `#[shakedown::test]`, which automates the same flow.

use shakedown::{run_scenario, BuiltinResource, Context, EventLog, SqliteResource};
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL_SQLITE};

#[tokio::test]
async fn first_scenario_sqlite_orders() {
    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Sqlite(SqliteResource::new()));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "ch01_first_order",
        |scenario_context| {
            Box::pin(async move {
                scenario_context
                    .sqlite()
                    .execute(ORDERS_SCHEMA_SQL_SQLITE, ())
                    .await
                    .expect("create orders table");

                let order = sample_order("customer-1");
                let customer_id_owned = order["customer_id"]
                    .as_str()
                    .expect("customer_id")
                    .to_string();
                let items_json_owned = serde_json::to_string(&order["items"]).expect("items json");

                scenario_context
                    .sqlite()
                    .execute(
                        "INSERT INTO orders (id, customer_id, items, status) VALUES (?1, ?2, ?3, ?4)",
                        (
                            "ord-1".to_string(),
                            customer_id_owned.clone(),
                            items_json_owned,
                            "created".to_string(),
                        ),
                    )
                    .await
                    .expect("insert order");

                scenario_context
                    .sqlite()
                    .assert_row_exists(
                        "SELECT 1 FROM orders WHERE id = ?1 AND customer_id = ?2",
                        ("ord-1".to_string(), customer_id_owned),
                    )
                    .await
                    .expect("order row should exist");

                scenario_context
                    .sqlite()
                    .assert_row_count("SELECT * FROM orders", (), 1)
                    .await
                    .expect("exactly one order");
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario should pass");
}
