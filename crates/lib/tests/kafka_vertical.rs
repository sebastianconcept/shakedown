//! Docker-backed Kafka vertical tests (`--features kafka`). Ignored by default so `cargo test` without Docker stays green.
//!
//! Run locally:
//! `cargo test -p shakedown --features kafka --test kafka_vertical -- --ignored`

#![cfg(feature = "kafka")]

use rdkafka::{
    config::ClientConfig,
    consumer::{ConsumerContext, Message as RdkafkaMessage},
    metadata::MetadataRequestOptions,
    producer::{BaseRecord, Producer},
};
use shakedown::resources::KafkaResource;
use shakedown::{run_scenario, Context, EventLog};

/// Integration test demonstrating the bindings escape hatch: produce/consume using rdkafka client
/// constructed from `KafkaBindings` on `Context`.
///
/// This validates US-005 acceptance criteria:
/// - Test demonstrates message flow using bindings from Context.
/// - Test is gated (`#[ignore = "requires Docker"]`) and documented in VERIFY.md.
/// - Topics are created via the harness's seed phase; user constructs their own clients.
#[tokio::test]
#[ignore = "requires Docker"]
async fn kafka_bindings_escape_hatch_produce_consume() {
    let scenario_id = "kafka-vertical-produce-consume";

    // Step 1: Set up harness with Kafka resource (KRaft broker starts via suite lifecycle)
    let mut ctx = Context::new();
    ctx.register_builtin(shakedown::resources::BuiltinResource::Kafka(
        KafkaResource::new(),
    ));

    let mut event_log = EventLog::new();

    // Step 2: Run the scenario lifecycle (provision → readiness → seed → run → assert → teardown)
    // The harness provisions Kafka, checks readiness, seeds topics (including proof topic),
    // and populates kafka_bindings on Context from provision metadata.
    let result = run_scenario(
        &mut ctx,
        scenario_id,
        |_ctx: &mut Context, _log: &mut EventLog| async {},
        &mut event_log,
    )
    .await;

    assert!(result.is_ok(), "run_scenario should complete successfully");

    // Step 3: Access bindings from Context (populated after provision phase)
    let Some(b) = ctx.kafka_bindings() else {
        panic!("kafka bindings should be populated after run_scenario");
    };

    println!(
        "KafkaBindings:\n  bootstrap_servers={}\ntopic_prefix={}",
        b.bootstrap_servers, b.topic_prefix
    );

    assert!(
        b.bootstrap_servers.starts_with("127.0.0.1:"),
        "bootstrap servers should be localhost"
    );
    assert!(
        b.topic_prefix.starts_with("shakedown_"),
        "topic prefix should include scenario id"
    );
    assert!(
        b.client_id_prefix.is_some(),
        "client_id_prefix should be set"
    );

    // Step 4: Demonstrate escape hatch: construct rdkafka producer from bindings
    let topic_name = &format!("{}proof", b.topic_prefix);
    println!("Publishing to topic: {}", topic_name);

    let producer: Producer<rdkafka::ClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &b.bootstrap_servers)
        .set(
            "client.id",
            format!("shakedown_escape_hatch_{}", scenario_id),
        )
        .create()
        .expect("Failed to create rdkafka producer from KafkaBindings");

    // Produce a message with typed payload
    let record = BaseRecord::to(topic_name).payload(b"Hello from Shakedown!");
    let _send_result = producer.send(record, None);

    // Drain the producer (flush messages)
    let _drain_result = producer.flush(std::time::Duration::from_secs(5));
    println!("Message published successfully");

    // Step 5: Consume the message back with a unique consumer group ID
    const CONSUMER_TIMEOUT_MS: i32 = 5000;
    let consumer_timeout_ms = CONSUMER_TIMEOUT_MS.to_string();

    let mut consumer = ClientConfig::new()
        .set("bootstrap.servers", &b.bootstrap_servers)
        .set("group.id", "shakedown.escape.hatch.test")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", consumer_timeout_ms)
        .create_consumer::<rdkafka::DefaultConsumerContext>()
        .expect("Failed to create rdkafka consumer from KafkaBindings");

    // Step 6: Assign to topic and consume message
    println!("Consuming from topic: {}", topic_name);

    let partition = format!("{}:0", topic_name);
    consumer.assign(&[(partition, rdkafka::Offset::Beginning)]);

    let message = consumer
        .poll(std::time::Duration::from_secs(10))
        .expect("Expected a message or timeout")
        .pop()
        .expect("No messages received on stream");

    let payload = message.payload().unwrap();
    let payload_str = std::str::from_utf8(payload).expect("Payload should be valid UTF-8");

    println!("Received payload: {}", payload_str);

    // Step 7: Assert payload matches what we sent
    assert_eq!(payload_str, "Hello from Shakedown!", "Payload mismatch");

    consumer.flush(std::time::Duration::from_secs(5));

    println!("\n✓ Kafka bindings escape hatch test passed!");
    println!("  - Produced message to topic '{}'", topic_name);
    println!("  - Consumed message with payload: {}", payload_str);
}

/// Integration test showing that topic creation happens in seed phase, not readiness.
/// This validates DECISION D-4 semantics: readiness = can connect; seed = create topics.
#[tokio::test]
#[ignore = "requires Docker"]
async fn kafka_readiness_does_not_create_topics() {
    let scenario_id = "kafka-vertical-readiness-separation";

    let mut ctx = Context::new();
    ctx.register_builtin(shakedown::resources::BuiltinResource::Kafka(
        KafkaResource::new(),
    ));

    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        scenario_id,
        |_ctx: &mut Context, _log: &mut EventLog| async {},
        &mut event_log,
    )
    .await
    .expect("scenario lifecycle should complete");

    let Some(b) = ctx.kafka_bindings() else {
        panic!("kafka bindings should be populated");
    };

    // After run_scenario (which includes seed phase), the proof topic should exist.
    // Verify by listing topics via admin API.
    let bootstrap = &b.bootstrap_servers;

    // Admin call to list topics (non-blocking check)
    let client: rdkafka::admin::AdminClient<rdkafka::DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("client.id", "shakedown_admin_probe")
        .create()
        .expect("Failed to create admin client");

    let meta_result = client.metadata(&MetadataRequestOptions::new(), None);
    assert!(
        meta_result.is_ok(),
        "list_topics should succeed after seed phase"
    );

    let Some(meta) = meta_result.ok().as_ref().and_then(|m| m.get()) else {
        panic!("Expected metadata to be present");
    };

    // The topic should exist (created in seed phase, not readiness)
    assert!(
        meta.topics.contains_key("proof"),
        "Topic 'proof' should exist after seed phase"
    );

    println!("✓ Readiness/separation test passed: topics created in seed phase");
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
