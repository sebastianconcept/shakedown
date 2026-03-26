//! Procedural macros for the `shakedown` test harness.
//!
//! What: attributes that wrap async tests with `shakedown::Context::new`, optional builtin
//! registration, and `shakedown::run_scenario`.
//!
//! Why: callers should not hand-roll lifecycle wiring in every test module.
//!
//! How: depend on `shakedown`; use `#[shakedown::test]` or `#[shakedown::faulty_test]` on
//! `async fn name(ctx: &mut shakedown::Context)`; enable crate features (e.g. `kafka`) on
//! `shakedown` to unlock matching builtins in `resources = [...]`.

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{bracketed, Expr, FnArg, Ident, ItemFn, Token, Type};

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

/// Comma-separated idents inside `[ ... ]` (e.g. `resources = [Fake, Kafka]`).
fn parse_ident_list_bracketed(input: ParseStream) -> syn::Result<Vec<Ident>> {
    let inner;
    bracketed!(inner in input);
    parse_ident_list(&inner)
}

/// Parse a comma-separated list of idents (no surrounding brackets).
fn parse_ident_list(input: ParseStream) -> syn::Result<Vec<Ident>> {
    let mut idents = Vec::new();
    while !input.is_empty() {
        idents.push(input.parse::<Ident>()?);
        if !input.is_empty() {
            input.parse::<Token![,]>()?;
        }
    }
    Ok(idents)
}

/// `#[shakedown::test]` attribute: optional `resources = [Fake, Kafka, ...]` only.
struct TestAttrArgs {
    resources: Option<Vec<Ident>>,
}

impl Parse for TestAttrArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self { resources: None });
        }

        let key: Ident = input.parse()?;
        if key != "resources" {
            return Err(syn::Error::new(
                key.span(),
                "unknown attribute argument (only `resources = [ ... ]` is supported in this milestone)",
            ));
        }
        input.parse::<Token![=]>()?;
        let resources = parse_ident_list_bracketed(input)?;
        Ok(Self {
            resources: Some(resources),
        })
    }
}

/// Parse `#[shakedown::test]` arguments; returns builtin resource idents when `resources = [..]` is present.
fn parse_test_attr(attr: TokenStream) -> syn::Result<Option<Vec<Ident>>> {
    if attr.is_empty() {
        return Ok(None);
    }
    let args: TestAttrArgs = syn::parse(attr)?;
    Ok(args.resources)
}

/// `#[shakedown::faulty_test]` attribute: `resources = [...]` plus any `name = <expr>` pairs
/// reserved for `mvp-fault-injection` are accepted and ignored in this milestone.
struct FaultyTestAttrParser {
    resources: Option<Vec<Ident>>,
}

impl Parse for FaultyTestAttrParser {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut resources = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            if key == "resources" {
                resources = Some(parse_ident_list_bracketed(input)?);
            } else {
                let _ = Expr::parse(input)?;
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(Self { resources })
    }
}

/// Parse `#[shakedown::faulty_test]` arguments, extracting `resources = [..]` when present.
fn parse_faulty_test_attr(attr: TokenStream) -> syn::Result<Option<Vec<Ident>>> {
    if attr.is_empty() {
        return Ok(None);
    }
    let parsed: FaultyTestAttrParser = syn::parse(attr)?;
    Ok(parsed.resources)
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

/// Wrap an async test in the full shakedown lifecycle (provision → teardown).
///
/// The attributed function must be `async` and take `ctx: &mut shakedown::Context` as the first parameter.
///
/// # Resource registration
///
/// - `#[shakedown::test]` — no builtins; register manually on `ctx` if needed.
/// - `#[shakedown::test(resources = [Fake])]` — registers `shakedown::FakeResource`.
/// - `#[shakedown::test(resources = [Fake, Kafka])]` — also registers Kafka when the `shakedown`
///   dependency enables the `kafka` feature.
///
/// # Panics
///
/// Panics with the display string of `shakedown::ShakedownError` if the scenario fails.
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let resources_list = match parse_test_attr(attr) {
        Ok(list) => list,
        Err(e) => return e.to_compile_error().into(),
    };

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

/// Like [`test`], but sets `shakedown::ScenarioKind::Faulty` before running.
///
/// Use for scenarios that opt into fault injection or degraded-mode behaviour. Resource attributes
/// match [`test`].
///
/// # Panics
///
/// Panics if `shakedown::run_scenario` returns `Err`, same as [`test`].
#[proc_macro_attribute]
pub fn faulty_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let resources_list = match parse_faulty_test_attr(attr) {
        Ok(list) => list,
        Err(e) => return e.to_compile_error().into(),
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

/// Builds registration code for a single resource identifier.
fn build_resource_registration(ident: &Ident) -> syn::Result<proc_macro2::TokenStream> {
    let name = ident.to_string();
    match name.as_str() {
        "Fake" => Ok(quote! {
            ctx.register_builtin(::shakedown::BuiltinResource::Fake(
                ::shakedown::FakeResource::default(),
            ));
        }),
        "Kafka" => Ok(quote! {
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
        }),
        "Postgres" => Ok(quote! {
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
        }),
        "MariaDB" => Ok(quote! {
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
        }),
        "Mysql" => Ok(quote! {
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
        }),
        "Sqlite" => Ok(quote! {
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
        }),
        _ => Err(syn::Error::new(
            ident.span(),
            format!(
                "unknown resource type `{name}`; valid builtins are `Fake`, `Kafka`, `Postgres`, `MariaDB`, `Mysql`, and `Sqlite`"
            ),
        )),
    }
}

/// Placeholder attribute: currently passes the item through unchanged.
///
/// Reserved for a future milestone where resources declare themselves to the harness; safe to ignore
/// for now.
#[proc_macro_attribute]
pub fn register_resource(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let _input = match syn::parse2::<syn::Item>(item.clone().into()) {
        Ok(i) => i,
        Err(e) => return e.to_compile_error().into(),
    };

    quote! { #_input }.into()
}

/// Expands to a fixed demo string (`"Hello, shakedown!"`) for crate smoke tests.
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
        for resource_name in ["Postgres", "MariaDB", "Mysql", "Sqlite"] {
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
