//! Procedural macros for the `shakedown` test harness.
//!
//! **What:** attributes and helpers that expand into `#[tokio::test]` async tests wired to
//! [`shakedown::run_scenario`](https://docs.rs/shakedown/latest/shakedown/fn.run_scenario.html),
//! plus optional compile-time resource registration.
//!
//! **Why:** tests should not duplicate `Context::new`, `EventLog::new`, and scenario wiring in every
//! function.
//!
//! **Macros (public API):**
//! - [`test`] — standard scenario tests; optional `resources = [...]` registers builtins when the
//!   matching `shakedown` crate features are enabled.
//! - [`faulty_test`] — same as [`test`], but sets [`shakedown::ScenarioKind::Faulty`](https://docs.rs/shakedown/latest/shakedown/enum.ScenarioKind.html)
//!   for fault-injection / degraded-mode scenarios.
//! - [`register_resource`] — reserved pass-through (no expansion yet); future: resource self-registration.
//! - [`default_greeting`] — tiny proc-macro demo for crate smoke tests.

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{bracketed, parenthesized, Expr, FnArg, Ident, ItemFn, Token, Type};

/// True when `ty` is `&mut ...::Context` (mutable reference to a path whose last segment is `Context`).
fn is_ctx_mut_context_type(ty: &Type) -> bool {
    let Type::Reference(type_reference) = ty else {
        return false;
    };
    if type_reference.mutability.is_none() {
        return false;
    }
    let Type::Path(type_path) = type_reference.elem.as_ref() else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "Context")
}

/// Comma-separated resource specs inside `[ ... ]` (e.g. `resources = [Fake, Kafka]` or `resources = [MongoReplicaSet(node_count = 3)]`).
fn parse_resource_spec_list_bracketed(input: ParseStream) -> syn::Result<Vec<ResourceSpec>> {
    let inner;
    bracketed!(inner in input);
    parse_resource_spec_list(&inner)
}

/// Parse a comma-separated list of resource specs (no surrounding brackets).
fn parse_resource_spec_list(input: ParseStream) -> syn::Result<Vec<ResourceSpec>> {
    let mut specs = Vec::new();
    while !input.is_empty() {
        specs.push(input.parse::<ResourceSpec>()?);
        if !input.is_empty() {
            input.parse::<Token![,]>()?;
        }
    }
    Ok(specs)
}

/// `#[shakedown::test]` attribute: optional `resources = [Fake, Kafka, ...]` and `prelude = { ... }`.
struct TestAttrArgs {
    resources: Option<Vec<ResourceSpec>>,
    prelude: Option<syn::Block>,
}

/// A single resource specification that can include configuration.
///
/// Either just `MongoReplicaSet` (bare identifier) or `MongoReplicaSet(node_count = 3)` (with config).
struct ResourceSpec {
    /// The resource type identifier (e.g., `MongoReplicaSet`).
    ident: Ident,
    /// Optional configuration key-value pairs (e.g., `node_count = 3`).
    config: Option<Vec<(Ident, Expr)>>,
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

impl Parse for TestAttrArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut resources = None;
        let mut prelude = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            if key == "resources" {
                if resources.is_some() {
                    return Err(syn::Error::new(
                        key.span(),
                        "duplicate `resources` attribute argument",
                    ));
                }
                resources = Some(parse_resource_spec_list_bracketed(input)?);
            } else if key == "prelude" {
                if prelude.is_some() {
                    return Err(syn::Error::new(
                        key.span(),
                        "duplicate `prelude` attribute argument",
                    ));
                }
                prelude = Some(input.parse::<syn::Block>()?);
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    "unknown attribute argument (supported: `resources = [ ... ]`, `prelude = { ... }`)",
                ));
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(Self { resources, prelude })
    }
}

/// Parse `#[shakedown::test]` arguments.
fn parse_test_attr(attr: TokenStream) -> syn::Result<TestAttrArgs> {
    if attr.is_empty() {
        return Ok(TestAttrArgs {
            resources: None,
            prelude: None,
        });
    }
    syn::parse(attr)
}

/// Parsed `#[shakedown::faulty_test(...)]` arguments.
struct FaultyTestAttr {
    resources: Option<Vec<ResourceSpec>>,
    fault_profile: Option<String>,
}

impl Parse for FaultyTestAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut resources = None;
        let mut fault_profile = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            if key == "resources" {
                resources = Some(parse_resource_spec_list_bracketed(input)?);
            } else if key == "fault_profile" {
                let expr: Expr = input.parse()?;
                let span = expr.span();
                let value = match expr {
                    Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    }) => s.value(),
                    _ => {
                        return Err(syn::Error::new(
                            span,
                            "`fault_profile` must be a string literal, e.g. fault_profile = \"latency\"",
                        ));
                    }
                };
                if fault_profile.is_some() {
                    return Err(syn::Error::new(
                        key.span(),
                        "duplicate `fault_profile` attribute argument",
                    ));
                }
                fault_profile = Some(value);
            } else {
                let _: Expr = input.parse()?;
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(Self {
            resources,
            fault_profile,
        })
    }
}

/// Parse `#[shakedown::faulty_test]` arguments.
fn parse_faulty_test_attr(attr: TokenStream) -> syn::Result<FaultyTestAttr> {
    if attr.is_empty() {
        return Ok(FaultyTestAttr {
            resources: None,
            fault_profile: None,
        });
    }
    syn::parse(attr)
}

/// Returns `true` when the first parameter is `ctx: &mut …::Context`.
fn first_parameter_is_ctx_mut_context(func: &ItemFn) -> bool {
    let Some(first) = func.sig.inputs.first() else {
        return false;
    };
    let FnArg::Typed(pat_type) = first else {
        return false;
    };
    is_ctx_mut_context_type(pat_type.ty.as_ref())
}

/// Declarative entry point for a **normal** Shakedown scenario test.
///
/// **What:** An attribute you place on an `async fn` so the macro expands it into a `#[tokio::test]`
/// that constructs [`shakedown::Context`](https://docs.rs/shakedown/latest/shakedown/struct.Context.html),
/// an [`shakedown::EventLog`](https://docs.rs/shakedown/latest/shakedown/event/struct.EventLog.html),
/// optionally pre-registers builtin resources, then runs your body inside
/// [`shakedown::run_scenario`](https://docs.rs/shakedown/latest/shakedown/fn.run_scenario.html)
/// (full lifecycle: provision → readiness → seed → run → assert → teardown).
///
/// **Unlocks when used:**
/// - **No wiring boilerplate** — you write only the scenario body; the harness supplies `ctx` and
///   drives the lifecycle.
/// - **Scenario identity** — the Rust function name becomes the scenario id string passed to
///   `run_scenario` (for logs, isolation keys, diagnostics).
/// - **`resources = [ ... ]`** — compile-time registration of builtins (`Fake`, `Kafka`, `Postgres`,
///   …) on `ctx` before your code runs; each name requires the matching feature on the `shakedown`
///   dependency (e.g. `kafka` for `Kafka`).
/// - **`prelude = { ... }`** — statements run on `ctx` after resource registration and before
///   [`run_scenario`] (e.g. register HTTP/gRPC mock services, which must complete before the harness
///   starts mock listeners).
/// - **Integration with the rest of the harness** — same path as manual `run_scenario` callers:
///   outbox assertions, Kafka/DB bindings, Docker diagnostics when features are on, etc.
///
/// The attributed function must be `async` and take `ctx: &mut shakedown::Context` as the first parameter.
///
/// # Resource registration
///
/// - `#[shakedown::test]` — no builtins; register manually on `ctx` if needed.
/// - `#[shakedown::test(resources = [Fake])]` — registers `shakedown::FakeResource`.
/// - `#[shakedown::test(resources = [Fake, Kafka])]` — also registers Kafka when the `shakedown`
///   dependency enables the `kafka` feature.
/// - `#[shakedown::test(prelude = { ctx.register_http_mock_service("api", stub).unwrap(); })]` —
///   runs the block before the scenario lifecycle (after `resources` registration).
///
/// # Panics
///
/// Panics with the display string of `shakedown::ShakedownError` if the scenario fails.
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let test_attr = match parse_test_attr(attr) {
        Ok(args) => args,
        Err(e) => return e.to_compile_error().into(),
    };
    let resources_list = test_attr.resources;
    let prelude_block = test_attr.prelude;

    let func = match syn::parse::<ItemFn>(item.clone()) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };

    // Validate async function
    if func.sig.asyncness.is_none() {
        return syn::Error::new(
            func.sig.ident.span(),
            "#[shakedown::test] requires an async function",
        )
        .to_compile_error()
        .into();
    }

    if !first_parameter_is_ctx_mut_context(&func) {
        return syn::Error::new(
            func.sig.fn_token.span(),
            "#[shakedown::test] expected signature: async fn name(ctx: &mut Context)",
        )
        .to_compile_error()
        .into();
    }

    // Extract function name for scenario ID
    let func_name = func.sig.ident.to_string();

    // Validate resources list if present
    let resources_registration = match resources_list {
        None => quote! {}, // No auto-registration, user registers manually if needed
        Some(resource_names) => {
            // Build registration code for each resource
            let mut registrations = Vec::new();

            for resource_ident in &resource_names {
                let reg = build_resource_registration(resource_ident);
                match reg {
                    Ok(code) => registrations.push(code),
                    Err(e) => return e.into_compile_error().into(),
                }
            }

            if registrations.is_empty() {
                quote! {}
            } else {
                quote! {
                    // Auto-register builtin resources before scenario runs
                    #(#registrations)*
                }
            }
        }
    };

    let prelude_tokens = match &prelude_block {
        Some(block) => quote! { #block },
        None => quote! {},
    };

    let fn_ident = &func.sig.ident;
    let forwarded_attrs = &func.attrs;
    // `quote!` parses `#func.block` as `#func` + literal `.block`; bind the block first.
    let user_block = &func.block;
    let expanded = quote! {
        #(#forwarded_attrs)*
        #[::tokio::test]
        async fn #fn_ident() {
            let mut ctx = ::shakedown::Context::new();
            let mut event_log = ::shakedown::EventLog::new();

            // Register resources if specified in attribute
            #resources_registration

            // Optional prelude (mock registration, etc.) before lifecycle / mock listener startup
            {
                #prelude_tokens
            }

            let result = ::shakedown::run_scenario(
                &mut ctx,
                #func_name,
                |ctx: &mut ::shakedown::Context| {
                    ::std::boxed::Box::pin(async move {
                        #user_block
                    })
                },
                &mut event_log,
            ).await;

            if let Err(e) = result {
                panic!("shakedown scenario failed: {}", e);
            }
        }
    }
    .into();

    #[allow(clippy::let_and_return)]
    expanded
}

/// Declarative entry point for a **fault-oriented** Shakedown scenario test.
///
/// **What:** Same expansion as [`test`] — `#[tokio::test]`, fresh `Context` and `EventLog`, optional
/// `resources = [ ... ]`, then `run_scenario` — except the harness sets
/// [`shakedown::ScenarioKind::Faulty`](https://docs.rs/shakedown/latest/shakedown/enum.ScenarioKind.html)
/// on the context **before** the scenario runs.
///
/// **Unlocks when used:**
/// - **Everything [`test`] unlocks** (lifecycle, builtins, scenario id, integrations).
/// - **Fault-injection signalling** — code under `ScenarioKind::Faulty` can branch (now or in future
///   milestones) to simulate failures, chaos, or degraded infrastructure without changing the macro
///   shape.
/// - **`fault_profile = "..."`** — expands to `ctx.set_fault_profile(...)` before the scenario runs.
/// - **Other `name = <expr>` pairs** — reserved for future wiring; parsed and ignored (no silent
///   failure for unknown keys beyond `resources` / `fault_profile`).
///
/// Use for scenarios that explicitly opt into fault injection or degraded-mode behaviour. Resource
/// lists match [`test`].
///
/// # Panics
///
/// Panics if `shakedown::run_scenario` returns `Err`, same as [`test`].
#[proc_macro_attribute]
pub fn faulty_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let parsed = match parse_faulty_test_attr(attr) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };
    let resources_list = parsed.resources;
    let fault_profile_stmt = match &parsed.fault_profile {
        None => quote! {},
        Some(s) => {
            let lit = syn::LitStr::new(s, proc_macro2::Span::call_site());
            quote! {
                ctx.set_fault_profile(#lit);
            }
        }
    };

    let func = match syn::parse::<ItemFn>(item.clone()) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };

    // Validate async function
    if func.sig.asyncness.is_none() {
        return syn::Error::new(
            func.sig.ident.span(),
            "#[shakedown::faulty_test] requires an async function",
        )
        .to_compile_error()
        .into();
    }

    if !first_parameter_is_ctx_mut_context(&func) {
        return syn::Error::new(
            func.sig.fn_token.span(),
            "#[shakedown::faulty_test] expected signature: async fn name(ctx: &mut Context)",
        )
        .to_compile_error()
        .into();
    }

    // Extract function name for scenario ID
    let func_name = func.sig.ident.to_string();

    // Validate resources list if present (same as test macro)
    let resources_registration = match resources_list {
        None => quote! {},
        Some(resource_names) => {
            let mut registrations = Vec::new();

            for resource_ident in &resource_names {
                let reg = build_resource_registration(resource_ident);
                match reg {
                    Ok(code) => registrations.push(code),
                    Err(e) => return e.into_compile_error().into(),
                }
            }

            if registrations.is_empty() {
                quote! {}
            } else {
                quote! {
                    // Auto-register builtin resources before scenario runs
                    #(#registrations)*
                }
            }
        }
    };

    let fn_ident = &func.sig.ident;
    let forwarded_attrs = &func.attrs;
    let user_block = &func.block;
    let expanded = quote! {
        #(#forwarded_attrs)*
        #[::tokio::test]
        async fn #fn_ident() {
            let mut ctx = ::shakedown::Context::new();
            let mut event_log = ::shakedown::EventLog::new();

            // Register resources if specified in attribute
            #resources_registration

            ctx.set_scenario_kind(::shakedown::ScenarioKind::Faulty);
            #fault_profile_stmt

            let result = ::shakedown::run_scenario(
                &mut ctx,
                #func_name,
                |ctx: &mut ::shakedown::Context| {
                    ::std::boxed::Box::pin(async move {
                        #user_block
                    })
                },
                &mut event_log,
            ).await;

            if let Err(e) = result {
                panic!("shakedown scenario failed: {}", e);
            }
        }
    }
    .into();

    #[allow(clippy::let_and_return)]
    expanded
}

/// Builds registration code for a single resource spec (bare identifier or with config).
#[allow(clippy::needless_return)]
fn build_resource_registration(spec: &ResourceSpec) -> syn::Result<proc_macro2::TokenStream> {
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

/// Placeholder attribute for **resource self-registration** (not implemented yet).
///
/// **What:** A no-op attribute that expands to its input item unchanged. It exists so API shape can be
/// reserved early.
///
/// **Unlocks when used (today):** nothing — the item is emitted as-is. Use only if you want to
/// annotate types or modules in preparation for future harness integration.
///
/// **Will unlock (planned):** attaching metadata to `Resource` implementations so the harness can
/// discover and register them without listing every type in `resources = [...]` at the test site.
#[proc_macro_attribute]
pub fn register_resource(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let _input = match syn::parse2::<syn::Item>(item.clone().into()) {
        Ok(i) => i,
        Err(e) => return e.to_compile_error().into(),
    };

    quote! { #_input }.into()
}

/// Minimal **function-like** proc-macro used for crate smoke tests and examples.
///
/// **What:** Expands to the string literal `"Hello, shakedown!"` (no configurable input).
///
/// **Unlocks when used:** a quick compile-time check that the `shakedown-macros` crate is linked
/// and procedural macros resolve correctly; not intended for production scenario code.
#[proc_macro]
pub fn default_greeting(_input: TokenStream) -> TokenStream {
    use syn::LitStr;
    let _lit = LitStr::new("Hello, shakedown!", proc_macro2::Span::call_site());
    quote! { #_lit }.into()
}

#[cfg(test)]
mod tests {
    use super::build_resource_registration;
    use syn::parse_str;

    #[test]
    fn accepts_relational_builtins() {
        for resource_name in [
            "Postgres",
            "MariaDB",
            "Mysql",
            "Sqlite",
            "DuckDb",
            "Timescaledb",
        ] {
            let resource_ident =
                parse_str(resource_name).expect("resource identifier should parse");
            assert!(
                build_resource_registration(&resource_ident).is_ok(),
                "{resource_name} should be accepted by resources parser",
            );
        }
    }

    #[test]
    fn relational_builtins_emit_feature_gated_expansion() {
        for (resource_name, expected_feature) in [
            ("Postgres", "postgres"),
            ("MariaDB", "mariadb"),
            ("Mysql", "mysql"),
            ("Sqlite", "sqlite"),
            ("DuckDb", "duckdb"),
            ("Timescaledb", "timescaledb"),
        ] {
            let resource_ident =
                parse_str(resource_name).expect("resource identifier should parse");
            let expansion = build_resource_registration(&resource_ident)
                .expect("resource registration should build")
                .to_string();
            assert!(
                expansion.contains(&format!("feature = \"{expected_feature}\"")),
                "{resource_name} expansion should be cfg-gated by {expected_feature}",
            );
        }
    }
}
