//! Builtin resource specs and registration token generation for scenario test macros.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{bracketed, parenthesized, Expr, Ident, Token};

fn path_string_literal(expr: &Expr, key: &str) -> syn::Result<syn::LitStr> {
    match expr {
        Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(lit),
            ..
        }) => Ok(lit.clone()),
        _ => Err(syn::Error::new(
            expr.span(),
            format!("`{key}` must be a string literal path (e.g. `\"./migrations\"`)"),
        )),
    }
}

type BindingNamePartition = (Option<syn::LitStr>, Option<Vec<(Ident, Expr)>>);

/// Pulls `name = "..."` out of resource parentheses so it is not forwarded to engine-specific keys.
fn partition_binding_name_config(spec: &ResourceSpec) -> syn::Result<BindingNamePartition> {
    let Some(config) = &spec.config else {
        return Ok((None, None));
    };
    let mut name_lit: Option<syn::LitStr> = None;
    let mut rest: Vec<(Ident, Expr)> = Vec::new();
    for (key, value) in config {
        if key == "name" {
            if name_lit.is_some() {
                return Err(syn::Error::new(
                    key.span(),
                    "duplicate `name` in resource configuration",
                ));
            }
            let lit = match value {
                Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(lit),
                    ..
                }) => lit.clone(),
                _ => {
                    return Err(syn::Error::new(
                        value.span(),
                        "`name` must be a string literal, e.g. `name = \"primary\"`",
                    ));
                }
            };
            name_lit = Some(lit);
        } else {
            rest.push((key.clone(), value.clone()));
        }
    }
    let rest_opt = if rest.is_empty() { None } else { Some(rest) };
    Ok((name_lit, rest_opt))
}

fn reject_binding_name_if_unsupported(
    spec: &ResourceSpec,
    binding_name_lit: &Option<syn::LitStr>,
) -> syn::Result<()> {
    if binding_name_lit.is_some() {
        return Err(syn::Error::new(
            spec.ident.span(),
            "`name` is only supported for MariaDB, Postgres, Mysql, and Timescaledb",
        ));
    }
    Ok(())
}

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
    let resource_type_name = spec.ident.to_string();
    let (binding_name_lit, rest_config) = partition_binding_name_config(spec)?;

    // Handle known resource types with configuration support
    match resource_type_name.as_str() {
        "Fake" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
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
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Kafka resource does not support configuration (use KafkaCluster instead)",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Kafka(
                    ::shakedown::KafkaResource::new(),
                ));
            });
        }
        "Postgres" => {
            let mut builder = quote! { ::shakedown::PostgresResource::new() };
            if let Some(ref lit) = binding_name_lit {
                builder = quote! { #builder.with_logical_binding_name(#lit) };
            }
            if let Some(config) = &rest_config {
                for (key, value) in config {
                    match key.to_string().as_str() {
                        "migrations" => {
                            let lit = path_string_literal(value, "migrations")?;
                            builder = quote! {
                                #builder.with_migrations_path(
                                    ::std::path::Path::new(::std::concat!(
                                        ::std::env!("CARGO_MANIFEST_DIR"),
                                        "/",
                                        #lit
                                    ))
                                    .to_path_buf()
                                )
                            };
                        }
                        "seed" => {
                            let lit = path_string_literal(value, "seed")?;
                            builder = quote! {
                                #builder.with_seed_path(
                                    ::std::path::Path::new(::std::concat!(
                                        ::std::env!("CARGO_MANIFEST_DIR"),
                                        "/",
                                        #lit
                                    ))
                                    .to_path_buf()
                                )
                            };
                        }
                        _ => {
                            return Err(syn::Error::new(
                                key.span(),
                                "unknown Postgres configuration key; valid keys are `migrations` or `seed`",
                            ));
                        }
                    }
                }
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Postgres(#builder));
            });
        }
        "PostgresReplicaSet" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            // Handle configuration for PostgresReplicaSet
            let mut builder_tokens = quote! {
                ::shakedown::PostgresReplicaSetResource::new()
            };

            if let Some(config) = &rest_config {
                for (key, value) in config {
                    match key.to_string().as_str() {
                        "image_tag" => {
                            builder_tokens = quote! { #builder_tokens.with_image_tag(#value) };
                        }
                        "version" => {
                            builder_tokens = quote! { #builder_tokens.with_image_tag(#value) };
                        }
                        "migrations" => {
                            let lit = path_string_literal(value, "migrations")?;
                            builder_tokens = quote! {
                                #builder_tokens.with_migrations_path(
                                    ::std::path::Path::new(::std::concat!(
                                        ::std::env!("CARGO_MANIFEST_DIR"),
                                        "/",
                                        #lit
                                    ))
                                    .to_path_buf()
                                )
                            };
                        }
                        "seed" => {
                            let lit = path_string_literal(value, "seed")?;
                            builder_tokens = quote! {
                                #builder_tokens.with_seed_path(
                                    ::std::path::Path::new(::std::concat!(
                                        ::std::env!("CARGO_MANIFEST_DIR"),
                                        "/",
                                        #lit
                                    ))
                                    .to_path_buf()
                                )
                            };
                        }
                        _ => {
                            return Err(syn::Error::new(
                                key.span(),
                                format!("unknown PostgresReplicaSet configuration key `{}`; valid keys are `image_tag`, `version`, `migrations`, or `seed`", key)
                            ));
                        }
                    }
                }
            }

            // Always add the env tag
            builder_tokens = quote! { #builder_tokens.with_image_tag_from_env() };

            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::PostgresReplicaSet(
                    #builder_tokens,
                ));
            });
        }
        "MariaDB" => {
            let mut builder = quote! { ::shakedown::MariaDBResource::new() };
            if let Some(ref lit) = binding_name_lit {
                builder = quote! { #builder.with_logical_binding_name(#lit) };
            }
            if let Some(config) = &rest_config {
                for (key, value) in config {
                    match key.to_string().as_str() {
                        "migrations" => {
                            let lit = path_string_literal(value, "migrations")?;
                            builder = quote! {
                                #builder.with_migrations_path(
                                    ::std::path::Path::new(::std::concat!(
                                        ::std::env!("CARGO_MANIFEST_DIR"),
                                        "/",
                                        #lit
                                    ))
                                    .to_path_buf()
                                )
                            };
                        }
                        "seed" => {
                            let lit = path_string_literal(value, "seed")?;
                            builder = quote! {
                                #builder.with_seed_path(
                                    ::std::path::Path::new(::std::concat!(
                                        ::std::env!("CARGO_MANIFEST_DIR"),
                                        "/",
                                        #lit
                                    ))
                                    .to_path_buf()
                                )
                            };
                        }
                        _ => {
                            return Err(syn::Error::new(
                                key.span(),
                                "unknown MariaDB configuration key; valid keys are `migrations` or `seed`",
                            ));
                        }
                    }
                }
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::MariaDB(#builder));
            });
        }
        "Mysql" => {
            let mut builder = quote! { ::shakedown::MysqlResource::new() };
            if let Some(ref lit) = binding_name_lit {
                builder = quote! { #builder.with_logical_binding_name(#lit) };
            }
            if let Some(config) = &rest_config {
                for (key, value) in config {
                    match key.to_string().as_str() {
                        "migrations" => {
                            let lit = path_string_literal(value, "migrations")?;
                            builder = quote! {
                                #builder.with_migrations_path(
                                    ::std::path::Path::new(::std::concat!(
                                        ::std::env!("CARGO_MANIFEST_DIR"),
                                        "/",
                                        #lit
                                    ))
                                    .to_path_buf()
                                )
                            };
                        }
                        "seed" => {
                            let lit = path_string_literal(value, "seed")?;
                            builder = quote! {
                                #builder.with_seed_path(
                                    ::std::path::Path::new(::std::concat!(
                                        ::std::env!("CARGO_MANIFEST_DIR"),
                                        "/",
                                        #lit
                                    ))
                                    .to_path_buf()
                                )
                            };
                        }
                        _ => {
                            return Err(syn::Error::new(
                                key.span(),
                                "unknown Mysql configuration key; valid keys are `migrations` or `seed`",
                            ));
                        }
                    }
                }
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Mysql(#builder));
            });
        }
        "Sqlite" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Sqlite resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Sqlite(
                    ::shakedown::SqliteResource::new(),
                ));
            });
        }
        "DuckDb" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "DuckDb resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::DuckDb(
                    ::shakedown::DuckDbResource::new(),
                ));
            });
        }
        "Rabbit" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Rabbit resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Rabbit(
                    ::shakedown::RabbitResource::new(),
                ));
            });
        }
        "Redis" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Redis resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Redis(
                    ::shakedown::RedisResource::new(),
                ));
            });
        }
        "RedisSentinel" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            // Handle configuration for RedisSentinel
            let mut builder_tokens = quote! {
                ::shakedown::RedisSentinelResource::new()
            };

            if let Some(config) = &rest_config {
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
                ctx.register_builtin(::shakedown::BuiltinResource::RedisSentinel(
                    #builder_tokens,
                ));
            });
        }
        "Valkey" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Valkey resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Valkey(
                    ::shakedown::ValkeyResource::new(),
                ));
            });
        }
        "Mongo" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Mongo resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Mongo(
                    ::shakedown::MongoResource::new(),
                ));
            });
        }
        "MongoReplicaSet" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            // Handle configuration for MongoReplicaSet
            let mut builder_tokens = quote! {
                ::shakedown::MongoReplicaSetResource::new()
            };

            if let Some(config) = &rest_config {
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
                ctx.register_builtin(::shakedown::BuiltinResource::MongoReplicaSet(
                    #builder_tokens,
                ));
            });
        }
        "KafkaCluster" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            // Handle configuration for KafkaCluster
            let mut builder_tokens = quote! {
                ::shakedown::KafkaClusterResource::new()
            };

            if let Some(config) = &rest_config {
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
                ctx.register_builtin(::shakedown::BuiltinResource::KafkaCluster(
                    #builder_tokens,
                ));
            });
        }
        "RabbitCluster" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            // Handle configuration for RabbitCluster
            let mut builder_tokens = quote! {
                ::shakedown::RabbitClusterResource::new()
            };

            if let Some(config) = &rest_config {
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
                ctx.register_builtin(::shakedown::BuiltinResource::RabbitCluster(
                    #builder_tokens,
                ));
            });
        }
        "Nats" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Nats resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Nats(
                    ::shakedown::NatsResource::new(),
                ));
            });
        }
        "Neo4j" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Neo4j resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Neo4j(
                    ::shakedown::Neo4jResource::new(),
                ));
            });
        }
        "Elasticsearch" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Elasticsearch resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Elasticsearch(
                    ::shakedown::ElasticsearchResource::new(),
                ));
            });
        }
        "OpenSearch" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "OpenSearch resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::OpenSearch(
                    ::shakedown::OpenSearchResource::new(),
                ));
            });
        }
        "Qdrant" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Qdrant resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Qdrant(
                    ::shakedown::QdrantResource::new(),
                ));
            });
        }
        "Weaviate" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Weaviate resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Weaviate(
                    ::shakedown::WeaviateResource::new(),
                ));
            });
        }
        "InfluxDb" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "InfluxDb resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::InfluxDb(
                    ::shakedown::InfluxDbResource::new(),
                ));
            });
        }
        "Timescaledb" => {
            let mut builder = quote! { ::shakedown::TimescaledbResource::new() };
            if let Some(ref lit) = binding_name_lit {
                builder = quote! { #builder.with_logical_binding_name(#lit) };
            }
            if let Some(config) = &rest_config {
                for (key, value) in config {
                    match key.to_string().as_str() {
                        "migrations" => {
                            let lit = path_string_literal(value, "migrations")?;
                            builder = quote! {
                                #builder.with_migrations_path(
                                    ::std::path::Path::new(::std::concat!(
                                        ::std::env!("CARGO_MANIFEST_DIR"),
                                        "/",
                                        #lit
                                    ))
                                    .to_path_buf()
                                )
                            };
                        }
                        "seed" => {
                            let lit = path_string_literal(value, "seed")?;
                            builder = quote! {
                                #builder.with_seed_path(
                                    ::std::path::Path::new(::std::concat!(
                                        ::std::env!("CARGO_MANIFEST_DIR"),
                                        "/",
                                        #lit
                                    ))
                                    .to_path_buf()
                                )
                            };
                        }
                        _ => {
                            return Err(syn::Error::new(
                                key.span(),
                                "unknown Timescaledb configuration key; valid keys are `migrations` or `seed`",
                            ));
                        }
                    }
                }
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Timescaledb(#builder));
            });
        }
        "Toxiproxy" => {
            reject_binding_name_if_unsupported(spec, &binding_name_lit)?;
            if rest_config.is_some() {
                return Err(syn::Error::new(
                    spec.ident.span(),
                    "Toxiproxy resource does not support configuration",
                ));
            }
            return Ok(quote! {
                ctx.register_builtin(::shakedown::BuiltinResource::Toxiproxy(
                    ::shakedown::ToxiproxyResource::new(),
                ));
            });
        }
        _ => {
            return Err(syn::Error::new(
                spec.ident.span(),
                format!(
                    "unknown resource type `{resource_type_name}`; valid builtins are `Fake`, `Kafka`, `KafkaCluster`, `Rabbit`, `RabbitCluster`, `Postgres`, `PostgresReplicaSet`, `MariaDB`, `Mysql`, `Sqlite`, `DuckDb`, `Redis`, `RedisSentinel`, `Valkey`, `Mongo`, `MongoReplicaSet`, `Nats`, `Neo4j`, `Elasticsearch`, `OpenSearch`, `Qdrant`, `Weaviate`, `InfluxDb`, `Timescaledb`, and `Toxiproxy`"
                ),
            ));
        }
    }
}

#[cfg(test)]
mod tests;
