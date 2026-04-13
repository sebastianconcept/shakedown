//! InfluxDB 2.x suite container + per-scenario bucket + Context bindings (optional).
//!
//! Built with `--features influxdb`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features influxdb --test influxdb_vertical -- --ignored
//! ```

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, InfluxDbResource, InfluxFieldValue,
    RegisteredResource,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features influxdb --test influxdb_vertical -- --ignored"]
async fn influxdb_provisions_and_sets_bindings() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::InfluxDb(
        InfluxDbResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "influxdb_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async {
                let bindings = ctx
                    .influxdb_bindings()
                    .expect("influxdb bindings after provision");
                assert_eq!(bindings.host, "127.0.0.1");
                assert!(bindings.port > 0);
                assert!(bindings.url.starts_with("http://127.0.0.1:"));
                assert_eq!(bindings.organization.as_str(), "shakedown");
                assert!(!bindings.bucket.is_empty());
                assert!(!bindings.token.is_empty());

                ctx.influxdb().expect("influxdb helpers")
                    .write_point(
                        "vertical_mem",
                        &[("host", "proof-host")],
                        &[("used_mb", InfluxFieldValue::Float(42.0))],
                        None,
                    )
                    .await
                    .expect("write_point");

                ctx.influxdb().expect("influxdb helpers")
                    .assert_point_exists("vertical_mem", &[("host", "proof-host")])
                    .await
                    .expect("assert_point_exists");

                let flux = format!(
                    "from(bucket: \"{}\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \"vertical_mem\") |> limit(n:1)",
                    bindings.bucket
                );
                let csv = ctx.influxdb().expect("influxdb helpers").query(flux.as_str()).await.expect("query");
                assert!(
                    csv.contains("vertical_mem"),
                    "expected measurement in Flux CSV: {csv}"
                );

                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::InfluxDb(_)))
                    ),
                    "expected InfluxDb builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}
