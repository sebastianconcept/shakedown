use super::{
    build_resource_entry, build_resource_registration, parse_resource_entry_list, ResourceEntry,
    ResourceSpec,
};
use quote::quote;
use syn::bracketed;
use syn::parse::{Parse, ParseStream};
use syn::parse_str;

struct BracketedResourceEntries(pub Vec<ResourceEntry>);

impl Parse for BracketedResourceEntries {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let inner;
        bracketed!(inner in input);
        parse_resource_entry_list(&inner).map(BracketedResourceEntries)
    }
}

#[test]
fn mariadb_named_resource_spec_builds() {
    let resource_ident: ResourceSpec =
        parse_str(r#"MariaDB(name = "primary")"#).expect("parse MariaDB with name");
    assert!(
        build_resource_registration(&resource_ident).is_ok(),
        "MariaDB(name = \"primary\") should expand",
    );
    let expansion = build_resource_registration(&resource_ident)
        .expect("build")
        .to_string();
    assert!(
        expansion.contains("with_logical_binding_name") && expansion.contains("MariaDBResource"),
        "expansion should chain with_logical_binding_name on MariaDBResource: {expansion}",
    );
}

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
fn relational_builtins_emit_unconditional_registration() {
    // Given relational builtins and their expected resource types
    for (resource_name, expected_type) in [
        ("Postgres", "PostgresResource"),
        ("MariaDB", "MariaDBResource"),
        ("Mysql", "MysqlResource"),
        ("Sqlite", "SqliteResource"),
        ("DuckDb", "DuckDbResource"),
        ("Timescaledb", "TimescaledbResource"),
    ] {
        // When registration tokens are built for that builtin
        let resource_ident = parse_str(resource_name).expect("resource identifier should parse");
        let expansion = build_resource_registration(&resource_ident)
            .expect("resource registration should build")
            .to_string();
        // Then the expansion contains an unconditional registration call — no cfg(feature) guard
        assert!(
            expansion.contains(expected_type),
            "{resource_name} expansion should reference {expected_type}",
        );
        assert!(
            !expansion.contains("cfg (feature"),
            "{resource_name} expansion must not contain cfg(feature) — feature resolution must happen in shakedown, not the calling crate",
        );
        assert!(
            !expansion.contains("compile_error"),
            "{resource_name} expansion must not contain compile_error!",
        );
    }
}

#[test]
fn all_builtins_emit_unconditional_registration() {
    // Given every supported builtin resource name and its expected resource type
    for (resource_name, expected_type) in [
        ("Fake", "FakeResource"),
        ("Kafka", "KafkaResource"),
        ("KafkaCluster", "KafkaClusterResource"),
        ("Rabbit", "RabbitResource"),
        ("RabbitCluster", "RabbitClusterResource"),
        ("Postgres", "PostgresResource"),
        ("PostgresReplicaSet", "PostgresReplicaSetResource"),
        ("MariaDB", "MariaDBResource"),
        ("Mysql", "MysqlResource"),
        ("Sqlite", "SqliteResource"),
        ("DuckDb", "DuckDbResource"),
        ("Redis", "RedisResource"),
        ("RedisSentinel", "RedisSentinelResource"),
        ("Valkey", "ValkeyResource"),
        ("Mongo", "MongoResource"),
        ("MongoReplicaSet", "MongoReplicaSetResource"),
        ("Nats", "NatsResource"),
        ("Neo4j", "Neo4jResource"),
        ("Elasticsearch", "ElasticsearchResource"),
        ("OpenSearch", "OpenSearchResource"),
        ("Qdrant", "QdrantResource"),
        ("Weaviate", "WeaviateResource"),
        ("InfluxDb", "InfluxDbResource"),
        ("Timescaledb", "TimescaledbResource"),
        ("Toxiproxy", "ToxiproxyResource"),
    ] {
        // When registration tokens are built for that builtin
        let resource_ident = parse_str(resource_name).expect("resource identifier should parse");
        let expansion = build_resource_registration(&resource_ident)
            .expect("resource registration should build")
            .to_string();
        // Then the expansion references the expected type without any cfg(feature) guard or compile_error!
        assert!(
            expansion.contains(expected_type),
            "{resource_name} expansion should reference {expected_type}",
        );
        assert!(
            !expansion.contains("cfg (feature"),
            "{resource_name} expansion must not contain cfg(feature) — feature resolution must happen in shakedown, not the calling crate",
        );
        assert!(
            !expansion.contains("compile_error"),
            "{resource_name} expansion must not contain compile_error!",
        );
    }
}

#[test]
fn custom_bare_ident_emits_register_extension() {
    let parsed: BracketedResourceEntries =
        syn::parse2(quote! { [VaultDev] }).expect("parse bracketed resource list");
    assert_eq!(parsed.0.len(), 1);
    let expansion = build_resource_entry(&parsed.0[0])
        .expect("build")
        .to_string();
    assert!(
        expansion.contains("register_extension"),
        "expected register_extension, got: {expansion}",
    );
}

#[test]
fn custom_path_call_expression_emits_register_extension() {
    let parsed: BracketedResourceEntries =
        syn::parse2(quote! { [crate::testing::VaultDevResource::new()] })
            .expect("parse bracketed resource list");
    assert_eq!(parsed.0.len(), 1);
    let expansion = build_resource_entry(&parsed.0[0])
        .expect("build")
        .to_string();
    assert!(
        expansion.contains("register_extension") && expansion.contains("VaultDevResource"),
        "expected register_extension with expression, got: {expansion}",
    );
}

#[test]
fn builtins_mixed_with_custom_expression() {
    let parsed: BracketedResourceEntries =
        syn::parse2(quote! { [Fake, crate::foo::Bar::new()] }).expect("parse");
    assert!(matches!(parsed.0[0], ResourceEntry::Builtin(_)));
    assert!(matches!(parsed.0[1], ResourceEntry::Custom(_)));
}
