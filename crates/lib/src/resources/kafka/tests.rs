use crate::resources::KafkaBindings;

#[test]
fn kafka_bindings_topic_namespace() {
    let bindings = KafkaBindings::new("localhost:9092", "shakedown_test");
    assert_eq!(bindings.bootstrap_servers, "localhost:9092");
    assert_eq!(bindings.topic_prefix, "shakedown_test");
    assert!(bindings.client_id_prefix.is_some());
    assert_eq!(bindings.topic_namespace("proof"), "shakedown_testproof");
}
