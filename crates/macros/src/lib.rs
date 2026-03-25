//! Proc macros consumed by the workspace `lib` crate.
//!
//! This crate provides the procedural macro attributes for shakedown testing:
//! - `#[shakedown::test]` — orchestrates a normal scenario lifecycle
//! - `#[shakedown::faulty_test]` — orchestrates a faulty scenario (for fault injection)

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{bracketed, FnArg, Ident, ItemFn, Token, Type};

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

/// `#[shakedown::test]` / `#[shakedown::faulty_test]` attribute: optional `resources = [Fake, Kafka, ...]`.
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
        let inner;
        bracketed!(inner in input);
        let mut resources = Vec::new();
        while !inner.is_empty() {
            resources.push(inner.parse::<Ident>()?);
            if !inner.is_empty() {
                inner.parse::<Token![,]>()?;
            }
        }
        Ok(Self {
            resources: Some(resources),
        })
    }
}

fn parse_test_attr(attr: TokenStream) -> syn::Result<Option<Vec<Ident>>> {
    if attr.is_empty() {
        return Ok(None);
    }
    let args: TestAttrArgs = syn::parse(attr)?;
    Ok(args.resources)
}

fn first_parameter_is_ctx_mut_context(func: &ItemFn) -> bool {
    let Some(first) = func.sig.inputs.first() else {
        return false;
    };
    let FnArg::Typed(pat_type) = first else {
        return false;
    };
    is_ctx_mut_context_type(pat_type.ty.as_ref())
}

/// Creates a scenario test wrapped in the full lifecycle.
///
/// Supports optional `resources` argument to auto-register builtin resources:
/// - `#[shakedown::test]` — basic test, no auto-registration
/// - `#[shakedown::test(resources = [Fake])]` — registers Fake resource
/// - `#[shakedown::test(resources = [Fake, Kafka])]` — registers both (Kafka requires `--features kafka`)
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
    // `quote!` parses `#func.block` as `#func` + literal `.block`; bind the block first.
    let user_block = &func.block;
    let expanded = quote! {
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

/// Creates a faulty scenario test wrapped in the full lifecycle.
///
/// Same as `#[shakedown::test]` but sets `ScenarioKind::Faulty` for fault-injection scenarios.
/// Supports optional `resources` argument identically to `#[shakedown::test]`.
#[proc_macro_attribute]
pub fn faulty_test(attr: TokenStream, item: TokenStream) -> TokenStream {
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
    let user_block = &func.block;
    let expanded = quote! {
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
        _ => Err(syn::Error::new(
            ident.span(),
            format!(
                "unknown resource type `{name}`; valid builtin resources are `Fake` and `Kafka` (Kafka requires `kafka` feature on `shakedown`)"
            ),
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
