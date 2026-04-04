use super::build_resource_registration;
use syn::parse_str;

#[test]
fn accepts_relational_builtins() {
    // Given a list of relational builtin resource names
    for resource_name in [
        "Postgres",
        "MariaDB",
        "Mysql",
        "Sqlite",
        "DuckDb",
        "Timescaledb",
    ] {
        // When each name is parsed as a resource spec
        let resource_ident = parse_str(resource_name).expect("resource identifier should parse");
        // Then registration code builds without error
        assert!(
            build_resource_registration(&resource_ident).is_ok(),
            "{resource_name} should be accepted by resources parser",
        );
    }
}

#[test]
fn relational_builtins_emit_feature_gated_expansion() {
    // Given relational builtins and their expected Cargo feature gates
    for (resource_name, expected_feature) in [
        ("Postgres", "postgres"),
        ("MariaDB", "mariadb"),
        ("Mysql", "mysql"),
        ("Sqlite", "sqlite"),
        ("DuckDb", "duckdb"),
        ("Timescaledb", "timescaledb"),
    ] {
        // When registration tokens are built for that builtin
        let resource_ident = parse_str(resource_name).expect("resource identifier should parse");
        let expansion = build_resource_registration(&resource_ident)
            .expect("resource registration should build")
            .to_string();
        // Then the expansion contains the matching `#[cfg(feature = "...")]`
        assert!(
            expansion.contains(&format!("feature = \"{expected_feature}\"")),
            "{resource_name} expansion should be cfg-gated by {expected_feature}",
        );
    }
}
