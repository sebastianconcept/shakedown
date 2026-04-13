//! TimescaleDB helpers: hypertable creation and `assert_hypertable_exists` (Docker).
//!
//! ```text
//! cargo test -p shakedown --features timescaledb --test helpers_timescaledb -- --ignored
//! ```

#[shakedown::test(resources = [Timescaledb])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features timescaledb --test helpers_timescaledb -- --ignored"]
async fn timescaledb_helpers_hypertable_round_trip(ctx: &mut shakedown::Context) {
    ctx.timescaledb()
        .expect("timescaledb helpers")
        .execute(
            "CREATE TABLE metrics (ts TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NOT NULL)",
            &[],
        )
        .await
        .expect("create table");
    ctx.timescaledb()
        .expect("timescaledb helpers")
        .execute("SELECT create_hypertable('metrics', 'ts')", &[])
        .await
        .expect("create_hypertable");
    ctx.timescaledb()
        .expect("timescaledb helpers")
        .assert_hypertable_exists("metrics")
        .await
        .expect("hypertable should exist");
    ctx.timescaledb()
        .expect("timescaledb helpers")
        .execute(
            "INSERT INTO metrics (ts, value) VALUES (NOW(), $1)",
            &[&42.5f64],
        )
        .await
        .expect("insert");
    ctx.timescaledb()
        .expect("timescaledb helpers")
        .assert_row_exists("SELECT 1 FROM metrics WHERE value = $1", &[&42.5f64])
        .await
        .expect("row exists");
}
