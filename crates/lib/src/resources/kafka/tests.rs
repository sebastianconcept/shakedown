use crate::resources::KafkaBindings;

#[test]
fn client_config_has_bootstrap_servers() {
    let bindings = KafkaBindings::new("localhost:19092", "shakedown_test_");
    let configuration = bindings.client_config();
    assert_eq!(
        configuration.get("bootstrap.servers"),
        Some("localhost:19092")
    );
    assert_eq!(configuration.get("message.timeout.ms"), Some("5000"));
}

#[test]
fn future_producer_returns_err_when_client_config_is_invalid() {
    // Empty bootstrap does not fail `create()` in current librdkafka (connection is lazy).
    // Invalid numeric properties are rejected when the native config is built.
    let bindings = KafkaBindings::new("127.0.0.1:9092", "p_");
    let mut configuration = bindings.client_config();
    configuration.set("message.timeout.ms", "not_a_number");
    assert!(KafkaBindings::future_producer_from_client_config(configuration).is_err());
    assert!(bindings.future_producer().is_ok());
}

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
