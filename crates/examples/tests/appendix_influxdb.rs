//! Appendix G: InfluxDB 2.x — line protocol writes, Flux queries, point assertions
//!
//! **Resources:** InfluxDB (Docker)
//! **Assertions:** `write_point`, `query`, `assert_point_exists`
//! **Teaching intent:** Emit order-level time-series samples and verify them with Flux the same way
//!   SRE dashboards would query the bucket.
//!
//! Run: `cargo test -p shakedown-examples --features influxdb --test appendix_influxdb -- --ignored`

use shakedown::InfluxFieldValue;

#[shakedown::test(resources = [InfluxDb])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features influxdb --test appendix_influxdb -- --ignored"]
async fn influxdb_order_metric_write_query_and_assert(ctx: &mut shakedown::Context) {
    let bindings = ctx
        .influxdb_bindings()
        .expect("InfluxDB bindings after provision");

    ctx.influxdb()
        .write_point(
            "order_metrics",
            &[("order_id", "order-influx-1"), ("region", "eu-west")],
            &[
                ("revenue", InfluxFieldValue::Float(199.95)),
                ("line_items", InfluxFieldValue::Integer(3)),
            ],
            None,
        )
        .await
        .expect("write order metric point");

    ctx.influxdb()
        .assert_point_exists(
            "order_metrics",
            &[("order_id", "order-influx-1"), ("region", "eu-west")],
        )
        .await
        .expect("Flux should find the written point");

    let flux = format!(
        "from(bucket: \"{}\") |> range(start: -1h) \
         |> filter(fn: (r) => r._measurement == \"order_metrics\" and r.order_id == \"order-influx-1\")",
        bindings.bucket
    );
    let csv_body = ctx
        .influxdb()
        .query(flux.as_str())
        .await
        .expect("Flux query");
    assert!(
        csv_body.contains("order_metrics"),
        "annotated CSV should reference the measurement: {csv_body}"
    );
}
