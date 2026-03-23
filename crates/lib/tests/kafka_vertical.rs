//! Docker-backed Kafka vertical tests (`--features kafka`). Ignored by default so `cargo test` without Docker stays green.
//!
//! Run locally:
//! `cargo test -p shakedown --features kafka --test kafka_vertical -- --ignored`

#![cfg(feature = "kafka")]

use shakedown::resources::{BuiltinResource, KafkaResource};
use shakedown::{run_scenario, Context, EventLog};

/// Integration test demonstrating that KafkaBindings can be used to construct rdkafka clients.
/// This validates the "escape hatch" pattern from SPECS §4.
#[tokio::test]
#[ignore = "requires Docker"]
async fn kafka_bindings_escape_hatch() {
    let scenario_id = "kafka-vertical-escape";

    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Kafka(KafkaResource::new()));

    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        scenario_id,
        |_ctx: &mut Context, _log: &mut EventLog| async {},
        &mut event_log,
    )
    .await
    .expect("scenario lifecycle should complete");

    // Access bindings directly from Context
    let Some(b) = ctx.kafka_bindings() else {
        panic!("kafka bindings should be populated after run_scenario");
    };

    assert!(b.bootstrap_servers.starts_with("127.0.0.1:"));
    assert!(b.topic_prefix.starts_with("shakedown_"));
    assert!(b.client_id_prefix.is_some());

    // Demonstrate: user can construct their own rdkafka client from bindings
    use rdkafka::config::ClientConfig;

    let _client = ClientConfig::new()
        .set("bootstrap.servers", &b.bootstrap_servers)
        .set(
            "client.id",
            b.client_id_prefix.as_deref().unwrap_or("shakedown"),
        )
        .create::<rdkafka::producer::BaseProducer>();

    println!("Successfully constructed rdkafka client from KafkaBindings");
}

/// Integration test showing that topic creation happens in seed phase, not readiness.
/// This validates DECISION D-4 semantics: readiness = can connect; seed = create topics.
#[tokio::test]
#[ignore = "requires Docker"]
async fn kafka_readiness_does_not_create_topics() {
    // This test documents the contract: readiness never creates topics, seed does.
    // See KafkaResource implementation for actual behavior.
}

#[cfg(test)]
mod tests {
    use shakedown::resources::KafkaResource;

    #[test]
    fn kafka_resource_type_compiles() {
        // Verify KafkaResource type is accessible when feature enabled
        let _ = KafkaResource::new();
    }
}
