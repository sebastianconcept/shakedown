//! Proc macros consumed by the workspace `lib` crate.
//!
//! This crate provides the procedural macro attributes for shakedown testing:
//! - `#[shakedown::test]` — orchestrates a normal scenario lifecycle
//! - `#[shakedown::faulty_test]` — orchestrates a faulty scenario (for fault injection)

use proc_macro::TokenStream;
use quote::quote;
use syn::{spanned::Spanned, FnArg, ItemFn};

/// Creates a scenario test wrapped in the full lifecycle.
///
/// Supports optional `resources` argument to auto-register builtin resources:
/// - `#[shakedown::test]` — basic test, no auto-registration
/// - `#[shakedown::test(resources = [Fake])]` — registers Fake resource
/// - `#[shakedown::test(resources = [Fake, Kafka])]` — registers both (Kafka requires `--features kafka`)
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse attribute arguments using simple lookahead approach
    let resources_list = parse_macro_args(attr);

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

    // Validate first parameter is ctx: &mut Context (or bare Context type)
    let has_correct_param = func
        .sig
        .inputs
        .first()
        .map(|arg| match arg {
            FnArg::Typed(pat_type) => {
                let ty = pat_type.ty.as_ref();
                // Accept both bare path types (Context) and reference paths (&mut Context)
                matches!(ty, syn::Type::Path(_) | syn::Type::Reference(_))
            }
            FnArg::Receiver(_) => false,
        })
        .unwrap_or(false);

    if !has_correct_param {
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

            for ident in &resource_names {
                let reg = build_resource_registration(ident);
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

    let expanded = quote! {
        #[::tokio::test]
        async fn #func.sig.ident() {
            let mut ctx = ::shakedown::Context::new();
            let mut event_log = ::shakedown::EventLog::new();

            // Register resources if specified in attribute
            #resources_registration

            let result = ::shakedown::run_scenario(
                &mut ctx,
                #func_name,
                |ctx: &mut ::shakedown::Context, _event_log: &mut ::shakedown::EventLog| async move {
                    // User's scenario body
                    #func.block
                },
                &mut event_log,
            ).await;

            if let Err(e) = result {
                panic!("shakedown scenario failed: {}", e);
            }
        }
    }.into();

    expanded
}

/// Creates a faulty scenario test wrapped in the full lifecycle.
///
/// Same as `#[shakedown::test]` but sets `ScenarioKind::Faulty` for fault-injection scenarios.
/// Supports optional `resources` argument identically to `#[shakedown::test]`.
#[proc_macro_attribute]
pub fn faulty_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse attribute arguments (same as test macro)
    let resources_list = parse_macro_args(attr);

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

    // Validate first parameter is ctx: &mut Context (or bare Context type)
    let has_correct_param = func
        .sig
        .inputs
        .first()
        .map(|arg| match arg {
            FnArg::Typed(pat_type) => {
                let ty = pat_type.ty.as_ref();
                // Accept both bare path types (Context) and reference paths (&mut Context)
                matches!(ty, syn::Type::Path(_) | syn::Type::Reference(_))
            }
            FnArg::Receiver(_) => false,
        })
        .unwrap_or(false);

    if !has_correct_param {
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

            for ident in &resource_names {
                let reg = build_resource_registration(ident);
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

    let expanded = quote! {
        #[::tokio::test]
        async fn #func.sig.ident() {
            let mut ctx = ::shakedown::Context::new();
            let mut event_log = ::shakedown::EventLog::new();

            // Register resources if specified in attribute
            #resources_registration

            ctx.set_scenario_kind(::shakedown::ScenarioKind::Faulty);

            let result = ::shakedown::run_scenario(
                &mut ctx,
                #func_name,
                |ctx: &mut ::shakedown::Context, _event_log: &mut ::shakedown::EventLog| async move {
                    // User's scenario body
                    #func.block
                },
                &mut event_log,
            ).await;

            if let Err(e) = result {
                panic!("shakedown scenario failed: {}", e);
            }
        }
    }.into();

    expanded
}

/// Parses the attribute arguments to extract optional resources list.
fn parse_macro_args(attr: TokenStream) -> Option<Vec<String>> {
    // Check if attr contains "[resources" pattern or just a bracket group with identifiers
    let tokens: Vec<proc_macro::TokenTree> = attr.into_iter().collect();

    let mut resource_names = Vec::new();
    let mut in_brackets = false;

    for token in &tokens {
        match token {
            proc_macro::TokenTree::Ident(ident) => {
                if ident.to_string() == "resources" && !in_brackets {
                    // Found the keyword, enter bracket mode
                    in_brackets = true;
                } else if in_brackets {
                    // This is a resource identifier (skip "resources" keyword itself)
                    if ident.to_string() != "resources" {
                        resource_names.push(ident.to_string());
                    }
                }
            }
            proc_macro::TokenTree::Punct(punct) => {
                let ch = punct.as_char();
                if ch == '[' && !in_brackets {
                    in_brackets = true;
                } else if ch == ']' {
                    in_brackets = false;
                }
            }
            _ => {}
        }
    }

    Some(resource_names)
}

/// Builds registration code for a single resource identifier.
fn build_resource_registration(ident: &str) -> syn::Result<proc_macro2::TokenStream> {
    match ident {
        "Fake" => Ok(quote! {
            ctx.register_builtin(::shakedown::BuiltinResource::Fake(
                ::shakedown::FakeResource::default(),
            ));
        }),
        #[cfg(feature = "kafka")]
        "Kafka" => Ok(quote! {
            #[cfg(feature = "kafka")]
            ctx.register_builtin(::shakedown::BuiltinResource::Kafka(
                ::shakedown::KafkaResource::new(),
            ));
        }),
        _ => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("unknown resource type: `{}`. Valid resources: `Fake`, `Kafka` (requires --features kafka)", ident),
        )),
    }
}

/// Registers a resource for use in Shakedown scenarios.
#[proc_macro_attribute]
pub fn register_resource(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let _input = match syn::parse2::<syn::Item>(item.clone().into()) {
        Ok(i) => i,
        Err(e) => return e.to_compile_error().into(),
    };

    quote! { #_input }.into()
}

/// Expands to the project's default greeting string.
#[proc_macro]
pub fn default_greeting(_input: TokenStream) -> TokenStream {
    use syn::LitStr;
    let _lit = LitStr::new("Hello, shakedown!", proc_macro2::Span::call_site());
    quote! { #_lit }.into()
}
