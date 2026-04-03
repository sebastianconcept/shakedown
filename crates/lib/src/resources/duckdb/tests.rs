use crate::resources::duckdb_named_memory_database_path;

#[test]
fn duckdb_named_memory_database_path_is_unique_per_scenario() {
    let a = duckdb_named_memory_database_path("test_a");
    let b = duckdb_named_memory_database_path("test_b");
    assert_ne!(a, b);
    assert!(a.ends_with(".duckdb"));
}
