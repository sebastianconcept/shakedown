//! Proc macros consumed by the workspace `lib` crate.

use proc_macro::TokenStream;
use quote::quote;
use syn::spanned::Spanned;

/// Creates a scenario test wrapped in the full lifecycle.
#[proc_macro_attribute]
pub fn test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = match syn::parse::<syn::ItemFn>(item.clone()) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };

    if func.sig.asyncness.is_none() {
        return syn::Error::new(
            func.sig.ident.span(),
            "#[shakedown::test] requires an async function",
        )
        .to_compile_error()
        .into();
    }

    let has_correct_param = func
        .sig
        .inputs
        .first()
        .map(|arg| match arg {
            syn::FnArg::Typed(pat_type) => matches!(pat_type.ty.as_ref(), syn::Type::Path(_)),
            syn::FnArg::Receiver(_) => false,
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

    quote! {
        #[::tokio::test]
        async fn #func.sig.ident {
            let mut ctx = ::shakedown::Context::new();
            let mut event_log = ::shakedown::EventLog::new();

            let result = ::shakedown::run_scenario(
                &mut ctx,
                #func.sig.ident.to_string(),
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
    }.into()
}

/// Creates a faulty scenario test wrapped in the full lifecycle.
#[proc_macro_attribute]
pub fn faulty_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = match syn::parse::<syn::ItemFn>(item.clone()) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };

    if func.sig.asyncness.is_none() {
        return syn::Error::new(
            func.sig.ident.span(),
            "#[shakedown::faulty_test] requires an async function",
        )
        .to_compile_error()
        .into();
    }

    let has_correct_param = func
        .sig
        .inputs
        .first()
        .map(|arg| match arg {
            syn::FnArg::Typed(pat_type) => matches!(pat_type.ty.as_ref(), syn::Type::Path(_)),
            syn::FnArg::Receiver(_) => false,
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

    quote! {
        #[::tokio::test]
        async fn #func.sig.ident {
            let mut ctx = ::shakedown::Context::new();
            let mut event_log = ::shakedown::EventLog::new();

            ctx.set_scenario_kind(::shakedown::ScenarioKind::Faulty);

            let result = ::shakedown::run_scenario(
                &mut ctx,
                #func.sig.ident.to_string(),
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
    }.into()
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
