//! Appendix I: DuckDB — in-process analytics SQL and DuckDB-backed outbox storage
//!
//! **Resources:** DuckDB (no Docker)
//! **Assertions:** `execute`, `assert_row_exists`, `assert_row_count`, `outbox().setup`,
//!   `outbox().assert_pending`, `assert_sent`, `assert_failed`, `assert_retry`, `with_payload::<T>()`
//! **Teaching intent:** Run analytical SQL beside SQLite-style scenarios and store canonical outbox
//!   rows in DuckDB when you want zero-container publication workflows.
//!
//! Run (no Docker): `cargo test -p shakedown-examples --features duckdb,outbox --test appendix_duckdb`

#[shakedown::test(resources = [DuckDb])]
async fn duckdb_order_metrics_analytics(ctx: &mut shakedown::Context) {
    ctx.duckdb()
        .execute(
            "CREATE TABLE IF NOT EXISTS order_metrics (\
                id VARCHAR PRIMARY KEY DEFAULT (uuid()), \
                customer_id VARCHAR NOT NULL, \
                total DOUBLE NOT NULL, \
                created_at TIMESTAMP DEFAULT current_timestamp\
            )",
            (),
        )
        .await
        .expect("create order_metrics table");

    ctx.duckdb()
        .execute(
            "INSERT INTO order_metrics (customer_id, total) VALUES ('customer-duck-1', 99.50)",
            (),
        )
        .await
        .expect("insert high-value basket");

    ctx.duckdb()
        .execute(
            "INSERT INTO order_metrics (customer_id, total) VALUES ('customer-duck-1', 42.00)",
            (),
        )
        .await
        .expect("insert second observation");

    ctx.duckdb()
        .assert_row_exists(
            "SELECT 1 FROM order_metrics WHERE customer_id = 'customer-duck-1' AND total = 99.50",
            (),
        )
        .await
        .expect("row exists for first total");

    ctx.duckdb()
        .assert_row_count(
            "SELECT * FROM order_metrics WHERE customer_id = 'customer-duck-1'",
            (),
            2,
        )
        .await
        .expect("two analytics rows for the customer");
}

#[shakedown::test(resources = [DuckDb])]
async fn duckdb_outbox_storage_pending_row_with_payload_predicate(ctx: &mut shakedown::Context) {
    let outbox = ctx.outbox();
    outbox
        .setup()
        .await
        .expect("setup creates outbox_event in DuckDB");

    ctx.duckdb()
        .execute(
            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts) \
             VALUES ('order', 'order-duck-outbox-1', 'OrderPlaced', \
             '{\"aggregate_id\":\"order-duck-outbox-1\",\"total\":55.0}', 'pending', 0)",
            (),
        )
        .await
        .expect("insert synthetic outbox row");

    outbox
        .assert_pending("order-duck-outbox-1", "OrderPlaced")
        .await
        .expect("row is pending")
        .with_payload::<serde_json::Value>(|payload| {
            payload["total"]
                .as_f64()
                .map(|value| (value - 55.0).abs() < f64::EPSILON)
                == Some(true)
        })
        .await
        .expect("payload predicate matches DuckDB JSON");
}

#[shakedown::test(resources = [DuckDb])]
async fn duckdb_outbox_sent_failed_and_retry_rows(ctx: &mut shakedown::Context) {
    let outbox = ctx.outbox();
    outbox
        .setup()
        .await
        .expect("setup creates outbox_event in DuckDB");

    ctx.duckdb()
        .execute(
            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts) \
             VALUES ('order', 'order-duck-sent-1', 'OrderShipped', '{}', 'sent', 0)",
            (),
        )
        .await
        .expect("insert sent row");

    outbox
        .assert_sent("order-duck-sent-1", "OrderShipped")
        .await
        .expect("row is sent");

    ctx.duckdb()
        .execute(
            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts) \
             VALUES ('order', 'order-duck-fail-1', 'CaptureFailed', '{}', 'failed', 0)",
            (),
        )
        .await
        .expect("insert failed row");

    outbox
        .assert_failed("order-duck-fail-1", "CaptureFailed")
        .await
        .expect("row is failed");

    ctx.duckdb()
        .execute(
            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts) \
             VALUES ('order', 'order-duck-retry-1', 'OrderRetried', '{}', 'pending', 4)",
            (),
        )
        .await
        .expect("insert high-attempt pending row");

    outbox
        .assert_retry("order-duck-retry-1", "OrderRetried", 3)
        .await
        .expect("retry attempts meet minimum");
}
