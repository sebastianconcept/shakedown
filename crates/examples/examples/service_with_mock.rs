//! Same adoption flow as `tests/service_with_mock.rs` (mock + SUT + env mappings + Kafka
//! produce/consume + Postgres), expanded **without** the attribute macro so you can
//! `cargo run --example service_with_mock` or type-check with:
//!
//! ```text
//! cargo check -p shakedown-examples --example service_with_mock --features kafka,postgres,mock-services,services
//! ```
//!
//! Prefer [`#[shakedown::test]`](shakedown::test) in real crates — it registers resources and
//! applies auto-`#[ignore]` for Docker-backed suites.

use shakedown::{BuiltinResource, Context, EventLog, KafkaResource, PostgresResource};

#[tokio::main]
async fn main() -> shakedown::Result<()> {
    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Postgres(PostgresResource::new()));
    context.register_builtin(BuiltinResource::Kafka(KafkaResource::new()));

    shakedown_examples::register_adoption_example_prelude(&mut context)?;

    let mut event_log = EventLog::new();
    shakedown::run_scenario(
        &mut context,
        "service_with_mock",
        |scenario_context| {
            Box::pin(async move {
                shakedown_examples::adoption_example_body(scenario_context).await;
            })
        },
        &mut event_log,
    )
    .await
}
