//! Builtin resource specs and registration token generation for scenario test macros.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{bracketed, parenthesized, Expr, Ident, Token};

/// A single resource specification that can include configuration.
///
/// Either just `MongoReplicaSet` (bare identifier) or `MongoReplicaSet(node_count = 3)` (with config).
pub(crate) struct ResourceSpec {
    /// The resource type identifier (e.g., `MongoReplicaSet`).
    pub(crate) ident: Ident,
    /// Optional configuration key-value pairs (e.g., `node_count = 3`).
    pub(crate) config: Option<Vec<(Ident, Expr)>>,
}

impl Parse for ResourceSpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ident = input.parse::<Ident>()?;

        // Check for optional parenthesized configuration
        let config = if input.peek(syn::token::Paren) {
            let content;
            parenthesized!(content in input);

            let mut pairs = Vec::new();
            while !content.is_empty() {
                let key: Ident = content.parse()?;
                content.parse::<Token![=]>()?;
                let value: Expr = content.parse()?;
                pairs.push((key, value));

                if !content.is_empty() {
                    content.parse::<Token![,]>()?;
                }
            }

            Some(pairs)
        } else {
            None
        };

        Ok(ResourceSpec { ident, config })
    }
}

/// Comma-separated resource specs inside `[ ... ]` (e.g. `resources = [Fake, Kafka]` or `resources = [MongoReplicaSet(node_count = 3)]`).
pub(crate) fn parse_resource_spec_list_bracketed(
    input: ParseStream,
) -> syn::Result<Vec<ResourceSpec>> {
    let inner;
    bracketed!(inner in input);
    parse_resource_spec_list(&inner)
}

/// Parse a comma-separated list of resource specs (no surrounding brackets).
pub(crate) fn parse_resource_spec_list(input: ParseStream) -> syn::Result<Vec<ResourceSpec>> {
    let mut specs = Vec::new();
    while !input.is_empty() {
        specs.push(input.parse::<ResourceSpec>()?);
        if !input.is_empty() {
            input.parse::<Token![,]>()?;
        }
    }
    Ok(specs)
}

/// Builds registration code for a single resource spec (bare identifier or with config).
#[allow(clippy::needless_return)]
pub(crate) fn build_resource_registration(spec: &ResourceSpec) -> syn::Result<TokenStream> {
    let name = spec.ident.to_string();

    // Handle known resource types with configuration support
    match name.as_str() {
        "Fake" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Fake resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Fake(
                    ::shakedown::FakeResource::default(),
                ));
            });
        }
        "Kafka" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Kafka resource does not support configuration (use KafkaCluster instead)",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "kafka")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Kafka(
                        ::shakedown::KafkaResource::new(),
                    ));
                }
                #[cfg(not(feature = "kafka"))]
                {
                    ::core::compile_error!(
                        "Kafka in #[shakedown::test(resources = [...])] requires `kafka` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Postgres" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Postgres resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "postgres")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Postgres(
                        ::shakedown::PostgresResource::new(),
                    ));
                }
                #[cfg(not(feature = "postgres"))]
                {
                    ::core::compile_error!(
                        "Postgres in #[shakedown::test(resources = [...])] requires `postgres` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "PostgresReplicaSet" => {
            // Handle configuration for PostgresReplicaSet
            let mut builder_tokens = quote! {
                ::shakedown::PostgresReplicaSetResource::new()
            };

            if let Some(config) = &spec.config {
                for (key, value) in config {
                    match key.to_string().as_str() {
                        "image_tag" => {
                            builder_tokens = quote! { #builder_tokens.with_image_tag(#value) };
                        }
                        "version" => {
                            builder_tokens = quote! { #builder_tokens.with_image_tag(#value) };
                        }
                        _ => {
                            return Err(syn::Error::new(
                                key.span(),
                                format!("unknown PostgresReplicaSet configuration key `{}`; valid keys are `image_tag` or `version`", key)
                            ));
                        }
                    }
                }
            }

            // Always add the env tag
            builder_tokens = quote! { #builder_tokens.with_image_tag_from_env() };

            return Ok(quote! {
                #[cfg(feature = "postgres-cluster")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::PostgresReplicaSet(
                        #builder_tokens,
                    ));
                }
                #[cfg(not(feature = "postgres-cluster"))]
                {
                    ::core::compile_error!(
                        "PostgresReplicaSet in #[shakedown::test(resources = [...])] requires `postgres-cluster` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "MariaDB" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "MariaDB resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "mariadb")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::MariaDB(
                        ::shakedown::MariaDBResource::new(),
                    ));
                }
                #[cfg(not(feature = "mariadb"))]
                {
                    ::core::compile_error!(
                        "MariaDB in #[shakedown::test(resources = [...])] requires `mariadb` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Mysql" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Mysql resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "mysql")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Mysql(
                        ::shakedown::MysqlResource::new(),
                    ));
                }
                #[cfg(not(feature = "mysql"))]
                {
                    ::core::compile_error!(
                        "Mysql in #[shakedown::test(resources = [...])] requires `mysql` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Sqlite" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Sqlite resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "sqlite")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Sqlite(
                        ::shakedown::SqliteResource::new(),
                    ));
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    ::core::compile_error!(
                        "Sqlite in #[shakedown::test(resources = [...])] requires `sqlite` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "DuckDb" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "DuckDb resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "duckdb")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::DuckDb(
                        ::shakedown::DuckDbResource::new(),
                    ));
                }
                #[cfg(not(feature = "duckdb"))]
                {
                    ::core::compile_error!(
                        "DuckDb in #[shakedown::test(resources = [...])] requires `duckdb` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Rabbit" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Rabbit resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "rabbitmq")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Rabbit(
                        ::shakedown::RabbitResource::new(),
                    ));
                }
                #[cfg(not(feature = "rabbitmq"))]
                {
                    ::core::compile_error!(
                        "Rabbit in #[shakedown::test(resources = [...])] requires `rabbitmq` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Redis" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Redis resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "redis")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Redis(
                        ::shakedown::RedisResource::new(),
                    ));
                }
                #[cfg(not(feature = "redis"))]
                {
                    ::core::compile_error!(
                        "Redis in #[shakedown::test(resources = [...])] requires `redis` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "RedisSentinel" => {
            // Handle configuration for RedisSentinel
            let mut builder_tokens = quote! {
                ::shakedown::RedisSentinelResource::new()
            };

            if let Some(config) = &spec.config {
                for (key, value) in config {
                    match key.to_string().as_str() {
                        "image_tag" => {
                            builder_tokens = quote! { #builder_tokens.with_image_tag(#value) };
                        }
                        "version" => {
                            builder_tokens = quote! { #builder_tokens.with_image_tag(#value) };
                        }
                        "master_name" => {
                            builder_tokens = quote! { #builder_tokens.with_master_name(#value) };
                        }
                        _ => {
                            return Err(syn::Error::new(
                                key.span(),
                                format!("unknown RedisSentinel configuration key `{}`; valid keys are `image_tag`, `version`, or `master_name`", key)
                            ));
                        }
                    }
                }
            }

            // Always add the env tag
            builder_tokens = quote! { #builder_tokens.with_image_tag_from_env() };

            return Ok(quote! {
                #[cfg(feature = "redis-sentinel")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::RedisSentinel(
                        #builder_tokens,
                    ));
                }
                #[cfg(not(feature = "redis-sentinel"))]
                {
                    ::core::compile_error!(
                        "RedisSentinel in #[shakedown::test(resources = [...])] requires `redis-sentinel` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Valkey" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Valkey resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "valkey")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Valkey(
                        ::shakedown::ValkeyResource::new(),
                    ));
                }
                #[cfg(not(feature = "valkey"))]
                {
                    ::core::compile_error!(
                        "Valkey in #[shakedown::test(resources = [...])] requires `valkey` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Mongo" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Mongo resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "mongodb")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Mongo(
                        ::shakedown::MongoResource::new(),
                    ));
                }
                #[cfg(not(feature = "mongodb"))]
                {
                    ::core::compile_error!(
                        "Mongo in #[shakedown::test(resources = [...])] requires `mongodb` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "MongoReplicaSet" => {
            // Handle configuration for MongoReplicaSet
            let mut builder_tokens = quote! {
                ::shakedown::MongoReplicaSetResource::new()
            };

            if let Some(config) = &spec.config {
                for (key, value) in config {
                    match key.to_string().as_str() {
                        "node_count" => {
                            builder_tokens = quote! { #builder_tokens.with_node_count(#value) };
                        }
                        "image_tag" => {
                            builder_tokens = quote! { #builder_tokens.with_image_tag(#value) };
                        }
                        "version" => {
                            builder_tokens = quote! { #builder_tokens.with_image_tag(#value) };
                        }
                        _ => {
                            return Err(syn::Error::new(
                                key.span(),
                                format!("unknown MongoReplicaSet configuration key `{}`; valid keys are `node_count`, `image_tag`, or `version`", key)
                            ));
                        }
                    }
                }
            }

            // Always add the env tag
            builder_tokens = quote! { #builder_tokens.with_image_tag_from_env() };

            return Ok(quote! {
                #[cfg(feature = "mongodb-cluster")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::MongoReplicaSet(
                        #builder_tokens,
                    ));
                }
                #[cfg(not(feature = "mongodb-cluster"))]
                {
                    ::core::compile_error!(
                        "MongoReplicaSet in #[shakedown::test(resources = [...])] requires `mongodb-cluster` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "KafkaCluster" => {
            // Handle configuration for KafkaCluster
            let mut builder_tokens = quote! {
                ::shakedown::KafkaClusterResource::new()
            };

            if let Some(config) = &spec.config {
                for (key, value) in config {
                    match key.to_string().as_str() {
                        "node_count" => {
                            builder_tokens = quote! { #builder_tokens.with_node_count(#value) };
                        }
                        "broker_count" => {
                            builder_tokens = quote! { #builder_tokens.with_node_count(#value) };
                        }
                        "image_tag" => {
                            builder_tokens = quote! { #builder_tokens.with_image_tag(#value) };
                        }
                        "version" => {
                            builder_tokens = quote! { #builder_tokens.with_image_tag(#value) };
                        }
                        "jvm_image" => {
                            builder_tokens = quote! { #builder_tokens.with_jvm_image() };
                        }
                        _ => {
                            return Err(syn::Error::new(
                                key.span(),
                                format!("unknown KafkaCluster configuration key `{}`; valid keys are `node_count`, `broker_count`, `image_tag`, `version`, or `jvm_image`", key)
                            ));
                        }
                    }
                }
            }

            // Always add the env tag
            builder_tokens = quote! { #builder_tokens.with_image_tag_from_env() };

            return Ok(quote! {
                #[cfg(feature = "kafka-cluster")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::KafkaCluster(
                        #builder_tokens,
                    ));
                }
                #[cfg(not(feature = "kafka-cluster"))]
                {
                    ::core::compile_error!(
                        "KafkaCluster in #[shakedown::test(resources = [...])] requires `kafka-cluster` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "RabbitCluster" => {
            // Handle configuration for RabbitCluster
            let mut builder_tokens = quote! {
                ::shakedown::RabbitClusterResource::new()
            };

            if let Some(config) = &spec.config {
                for (key, value) in config {
                    match key.to_string().as_str() {
                        "node_count" => {
                            builder_tokens = quote! { #builder_tokens.with_node_count(#value) };
                        }
                        "image_tag" => {
                            builder_tokens = quote! { #builder_tokens.with_image_tag(#value) };
                        }
                        "version" => {
                            builder_tokens = quote! { #builder_tokens.with_image_tag(#value) };
                        }
                        _ => {
                            return Err(syn::Error::new(
                                key.span(),
                                format!("unknown RabbitCluster configuration key `{}`; valid keys are `node_count`, `image_tag`, or `version`", key)
                            ));
                        }
                    }
                }
            }

            // Always add the env tag
            builder_tokens = quote! { #builder_tokens.with_image_tag_from_env() };

            return Ok(quote! {
                #[cfg(feature = "rabbitmq-cluster")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::RabbitCluster(
                        #builder_tokens,
                    ));
                }
                #[cfg(not(feature = "rabbitmq-cluster"))]
                {
                    ::core::compile_error!(
                        "RabbitCluster in #[shakedown::test(resources = [...])] requires `rabbitmq-cluster` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Nats" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Nats resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "nats")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Nats(
                        ::shakedown::NatsResource::new(),
                    ));
                }
                #[cfg(not(feature = "nats"))]
                {
                    ::core::compile_error!(
                        "Nats in #[shakedown::test(resources = [...])] requires `nats` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Neo4j" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Neo4j resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "neo4j")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Neo4j(
                        ::shakedown::Neo4jResource::new(),
                    ));
                }
                #[cfg(not(feature = "neo4j"))]
                {
                    ::core::compile_error!(
                        "Neo4j in #[shakedown::test(resources = [...])] requires `neo4j` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Elasticsearch" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Elasticsearch resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "elasticsearch")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Elasticsearch(
                        ::shakedown::ElasticsearchResource::new(),
                    ));
                }
                #[cfg(not(feature = "elasticsearch"))]
                {
                    ::core::compile_error!(
                        "Elasticsearch in #[shakedown::test(resources = [...])] requires `elasticsearch` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "OpenSearch" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "OpenSearch resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "opensearch")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::OpenSearch(
                        ::shakedown::OpenSearchResource::new(),
                    ));
                }
                #[cfg(not(feature = "opensearch"))]
                {
                    ::core::compile_error!(
                        "OpenSearch in #[shakedown::test(resources = [...])] requires `opensearch` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Qdrant" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Qdrant resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "qdrant")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Qdrant(
                        ::shakedown::QdrantResource::new(),
                    ));
                }
                #[cfg(not(feature = "qdrant"))]
                {
                    ::core::compile_error!(
                        "Qdrant in #[shakedown::test(resources = [...])] requires `qdrant` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Weaviate" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Weaviate resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "weaviate")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Weaviate(
                        ::shakedown::WeaviateResource::new(),
                    ));
                }
                #[cfg(not(feature = "weaviate"))]
                {
                    ::core::compile_error!(
                        "Weaviate in #[shakedown::test(resources = [...])] requires `weaviate` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "InfluxDb" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "InfluxDb resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "influxdb")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::InfluxDb(
                        ::shakedown::InfluxDbResource::new(),
                    ));
                }
                #[cfg(not(feature = "influxdb"))]
                {
                    ::core::compile_error!(
                        "InfluxDb in #[shakedown::test(resources = [...])] requires `influxdb` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Timescaledb" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Timescaledb resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "timescaledb")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Timescaledb(
                        ::shakedown::TimescaledbResource::new(),
                    ));
                }
                #[cfg(not(feature = "timescaledb"))]
                {
                    ::core::compile_error!(
                        "Timescaledb in #[shakedown::test(resources = [...])] requires `timescaledb` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        "Toxiproxy" => {
            if spec.config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Toxiproxy resource does not support configuration",
                ));
            }
            return Ok(quote! {
                #[cfg(feature = "fault-injection")]
                {
                    ctx.register_builtin(::shakedown::BuiltinResource::Toxiproxy(
                        ::shakedown::ToxiproxyResource::new(),
                    ));
                }
                #[cfg(not(feature = "fault-injection"))]
                {
                    ::core::compile_error!(
                        "Toxiproxy in #[shakedown::test(resources = [...])] requires `fault-injection` feature on the `shakedown` dependency"
                    );
                }
            });
        }
        _ => {
            return Err(syn::Error::new(
                spec.ident.span(),
                format!(
                    "unknown resource type `{name}`; valid builtins are `Fake`, `Kafka`, `KafkaCluster`, `Rabbit`, `RabbitCluster`, `Postgres`, `PostgresReplicaSet`, `MariaDB`, `Mysql`, `Sqlite`, `DuckDb`, `Redis`, `RedisSentinel`, `Valkey`, `Mongo`, `MongoReplicaSet`, `Nats`, `Neo4j`, `Elasticsearch`, `OpenSearch`, `Qdrant`, `Weaviate`, `InfluxDb`, `Timescaledb`, and `Toxiproxy`"
                ),
            ));
        }
    }
}

#[cfg(test)]
mod tests;
