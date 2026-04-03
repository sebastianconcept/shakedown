use crate::resources::sqlite_named_memory_uri;

#[test]
fn sqlite_named_memory_uri_is_unique_per_scenario() {
    let a = sqlite_named_memory_uri("test_a");
    let b = sqlite_named_memory_uri("test_b");
    assert_ne!(a, b);
    assert!(a.starts_with("file:shakedown_"));
    assert!(a.contains("mode=memory"));
    assert!(a.contains("cache=shared"));
}
