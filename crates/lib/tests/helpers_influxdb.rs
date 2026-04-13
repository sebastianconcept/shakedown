//! InfluxDB 2.x typed helpers: write + Flux query + assert (Docker).
//!
//! Built with `--features influxdb`. **`#[ignore]`** unless `--ignored`.
//!
//! ```text
//! cargo test -p shakedown --features influxdb --test helpers_influxdb -- --ignored
//! ```

use shakedown::InfluxFieldValue;

#[shakedown::test(resources = [InfluxDb])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features influxdb --test helpers_influxdb -- --ignored"]
async fn influxdb_helpers_write_query_assert(ctx: &mut shakedown::Context) {
    let bindings = ctx.influxdb_bindings().expect("bindings");
    ctx.influxdb()
        .expect("influxdb helpers")
        .write_point(
            "helpers_metric",
            &[("region", "us-east")],
            &[("value", InfluxFieldValue::Integer(7))],
            None,
        )
        .await
        .expect("write_point");

    ctx.influxdb()
        .expect("influxdb helpers")
        .assert_point_exists("helpers_metric", &[("region", "us-east")])
        .await
        .expect("assert_point_exists");

    let flux = format!(
        "from(bucket: \"{}\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \"helpers_metric\")",
        bindings.bucket
    );
    let body = ctx
        .influxdb()
        .expect("influxdb helpers")
        .query(flux.as_str())
        .await
        .expect("query");
    assert!(
        body.contains("helpers_metric"),
        "csv should name measurement: {body}"
    );
}
