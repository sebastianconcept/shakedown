use crate::resources::KafkaBindings;

#[test]
fn kafka_bindings_topic_namespace() {
    let bindings = KafkaBindings::new("localhost:9092", "shakedown_test");
    assert_eq!(bindings.bootstrap_servers, "localhost:9092");
    assert_eq!(bindings.topic_prefix, "shakedown_test");
    assert!(bindings.client_id_prefix.is_some());
    assert_eq!(bindings.topic_namespace("proof"), "shakedown_testproof");
}

#[test]
fn kafka_bindings_consumer_group_namespace_uses_same_prefix_as_topic_namespace() {
    let bindings = KafkaBindings::new("localhost:9092", "shakedown_abc_");
    // Consumer group and topic namespacing use the same prefix so they share the same
    // scenario boundary, preventing cross-scenario message stealing in parallel runs.
    assert_eq!(
        bindings.consumer_group_namespace("orders-consumer"),
        "shakedown_abc_orders-consumer"
    );
    assert_eq!(
        bindings.consumer_group_namespace("orders-consumer"),
        format!("{}{}", bindings.topic_prefix, "orders-consumer")
    );
}
