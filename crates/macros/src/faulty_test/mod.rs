//! Implementation of the `#[shakedown::faulty_test]` attribute macro.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{Expr, Ident, ItemFn, Token};

use crate::resource_registration::{build_resource_entry, parse_resource_spec_list_bracketed};
use crate::signature::first_parameter_is_ctx_mut_context;

/// Parsed `#[shakedown::faulty_test(...)]` arguments.
pub(super) struct FaultyTestAttr {
    pub(super) resources: Option<Vec<crate::resource_registration::ResourceEntry>>,
    pub(super) fault_profile: Option<String>,
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

pub(super) fn parse_faulty_test_attr(attr: TokenStream) -> syn::Result<FaultyTestAttr> {
    if attr.is_empty() {
        return Ok(FaultyTestAttr {
            resources: None,
            fault_profile: None,
        });
    }
    syn::parse2(attr)
}

/// Expands `#[shakedown::faulty_test]` (see crate-level docs on `lib.rs`).
pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    let parsed = match parse_faulty_test_attr(attr) {
        Ok(p) => p,
        Err(e) => return e.to_compile_error(),
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

    let func = match syn::parse2::<ItemFn>(item.clone()) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    if func.sig.asyncness.is_none() {
        return syn::Error::new(
            func.sig.ident.span(),
            "#[shakedown::faulty_test] requires an async function",
        )
        .to_compile_error();
    }

    if !first_parameter_is_ctx_mut_context(&func) {
        return syn::Error::new(
            func.sig.fn_token.span(),
            "#[shakedown::faulty_test] expected signature: async fn name(ctx: &mut Context)",
        )
        .to_compile_error();
    }

    let func_name = func.sig.ident.to_string();

    let resources_registration = match resources_list {
        None => quote! {},
        Some(resource_names) => {
            let mut registrations = Vec::new();

            for resource_entry in &resource_names {
                let reg = build_resource_entry(resource_entry);
                match reg {
                    Ok(code) => registrations.push(code),
                    Err(e) => return e.into_compile_error(),
                }
            }

            if registrations.is_empty() {
                quote! {}
            } else {
                quote! {
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
    };

    #[allow(clippy::let_and_return)]
    expanded
}

#[cfg(test)]
mod tests;
