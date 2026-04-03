use crate::resources::RabbitBindings;

#[test]
fn rabbit_bindings_namespaced_name() {
    let bindings = RabbitBindings::new(
        "amqp://guest:guest@127.0.0.1:5672",
        "127.0.0.1",
        5672,
        "guest",
        "guest",
        "/",
        "shakedown_test_",
    );
    assert_eq!(bindings.amqp_uri, "amqp://guest:guest@127.0.0.1:5672");
    assert_eq!(bindings.connection_url, bindings.amqp_uri);
    assert_eq!(bindings.port, 5672);
    assert_eq!(bindings.vhost, "/");
    assert_eq!(bindings.namespaced_name("proof"), "shakedown_test_proof");
}
