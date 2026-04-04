//! Appendix H: TimescaleDB — hypertables and SQL assertions on Postgres-compatible APIs
//!
//! **Resources:** TimescaleDB (Docker)
//! **Assertions:** `execute`, `assert_row_exists`, `assert_hypertable_exists`
//! **Teaching intent:** Create a hypertable for order throughput samples and assert both catalog
//!   metadata and row-level facts.
//!
//! Run: `cargo test -p shakedown-examples --features timescaledb --test appendix_timescaledb -- --ignored`

#[shakedown::test(resources = [Timescaledb])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features timescaledb --test appendix_timescaledb -- --ignored"]
async fn timescaledb_order_throughput_hypertable(ctx: &mut shakedown::Context) {
    ctx.timescaledb()
        .execute(
            "CREATE TABLE order_throughput (ts TIMESTAMPTZ NOT NULL, orders_per_min DOUBLE PRECISION NOT NULL)",
            &[],
        )
        .await
        .expect("create base table");

    ctx.timescaledb()
        .execute("SELECT create_hypertable('order_throughput', 'ts')", &[])
        .await
        .expect("promote to hypertable");

    ctx.timescaledb()
        .assert_hypertable_exists("order_throughput")
        .await
        .expect("hypertable registered in timescaledb_information");

    ctx.timescaledb()
        .execute(
            "INSERT INTO order_throughput (ts, orders_per_min) VALUES (NOW(), $1)",
            &[&128.0_f64],
        )
        .await
        .expect("insert throughput sample");

    ctx.timescaledb()
        .assert_row_exists(
            "SELECT 1 FROM order_throughput WHERE orders_per_min = $1",
            &[&128.0_f64],
        )
        .await
        .expect("row exists");

    ctx.timescaledb()
        .assert_row_count(
            "SELECT COUNT(*) FROM order_throughput WHERE orders_per_min = $1",
            &[&128.0_f64],
            1,
        )
        .await
        .expect("exactly one sample row");
}
