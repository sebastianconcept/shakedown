//! Implementation of the `#[shakedown::test]` attribute macro.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{Ident, ItemFn, Token};

use crate::resource_registration::{
    build_resource_registration, parse_resource_spec_list_bracketed, ResourceSpec,
};
use crate::signature::first_parameter_is_ctx_mut_context;

/// `#[shakedown::test]` attribute: optional `resources = [Fake, Kafka, ...]`, `prelude = { ... }`,
/// `migrations = "…"` (stages a sqlx migration tree path relative to the test crate's `CARGO_MANIFEST_DIR`;
/// applied in `run_scenario` after seed when `shakedown` is built with `postgres` + `sqlx`), and
/// `ignore = true | false` (overrides auto-`#[ignore]` when `resources` is non-empty).
pub(super) struct TestAttrArgs {
    pub(super) resources: Option<Vec<ResourceSpec>>,
    pub(super) prelude: Option<syn::Block>,
    pub(super) ignore: Option<bool>,
    pub(super) migrations: Option<syn::LitStr>,
}

impl Parse for TestAttrArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut resources = None;
        let mut prelude = None;
        let mut ignore = None;
        let mut migrations = None;

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
            } else if key == "migrations" {
                if migrations.is_some() {
                    return Err(syn::Error::new(
                        key.span(),
                        "duplicate `migrations` attribute argument",
                    ));
                }
                migrations = Some(input.parse::<syn::LitStr>()?);
            } else if key == "ignore" {
                if ignore.is_some() {
                    return Err(syn::Error::new(
                        key.span(),
                        "duplicate `ignore` attribute argument",
                    ));
                }
                let lit: syn::LitBool = input.parse()?;
                ignore = Some(lit.value);
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    "unknown attribute argument (supported: `resources = [ ... ]`, `prelude = { ... }`, `migrations = \"…\"`, `ignore = true | false`)",
                ));
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(Self {
            resources,
            prelude,
            ignore,
            migrations,
        })
    }
}

pub(super) fn parse_test_attr(attr: TokenStream) -> syn::Result<TestAttrArgs> {
    if attr.is_empty() {
        return Ok(TestAttrArgs {
            resources: None,
            prelude: None,
            ignore: None,
            migrations: None,
        });
    }
    syn::parse2(attr)
}

fn is_test_ignore_attr(attr: &syn::Attribute) -> bool {
    attr.path().is_ident("ignore")
}

/// Expands `#[shakedown::test]` (see crate-level docs on `lib.rs`).
pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    let test_attr = match parse_test_attr(attr) {
        Ok(args) => args,
        Err(e) => return e.to_compile_error(),
    };

    let resources_list = test_attr.resources;
    let prelude_block = test_attr.prelude;
    let migrations_lit = test_attr.migrations;

    let has_resources = resources_list
        .as_ref()
        .is_some_and(|resource_specs| !resource_specs.is_empty());
    let should_ignore = match test_attr.ignore {
        Some(explicit_ignore) => explicit_ignore,
        None => has_resources,
    };
    let strip_user_ignore = should_ignore || (has_resources && test_attr.ignore == Some(false));

    let func = match syn::parse2::<ItemFn>(item.clone()) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error(),
    };

    if func.sig.asyncness.is_none() {
        return syn::Error::new(
            func.sig.ident.span(),
            "#[shakedown::test] requires an async function",
        )
        .to_compile_error();
    }

    if !first_parameter_is_ctx_mut_context(&func) {
        return syn::Error::new(
            func.sig.fn_token.span(),
            "#[shakedown::test] expected signature: async fn name(ctx: &mut Context)",
        )
        .to_compile_error();
    }

    let func_name = func.sig.ident.to_string();

    let resources_registration = match &resources_list {
        None => quote! {},
        Some(resource_names) => {
            let mut registrations = Vec::new();

            for resource_ident in resource_names {
                let reg = build_resource_registration(resource_ident);
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

    if let Some(ref migrations_lit) = migrations_lit {
        let has_postgres = resources_list.as_ref().is_some_and(|resource_specs| {
            resource_specs
                .iter()
                .any(|resource_spec| resource_spec.ident == "Postgres")
        });
        if !has_postgres {
            return syn::Error::new(
                migrations_lit.span(),
                "`migrations` requires `resources = [Postgres, ...]` so the scenario has a Postgres database to migrate",
            )
            .to_compile_error();
        }
    }

    let migrations_tokens = match &migrations_lit {
        Some(path_lit) => quote! {
            ctx.set_pending_postgres_sqlx_migrations_path(
                ::std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/", #path_lit))
                    .to_path_buf(),
            );
        },
        None => quote! {},
    };

    let prelude_tokens = match &prelude_block {
        Some(block) => quote! { #block },
        None => quote! {},
    };

    let fn_ident = &func.sig.ident;
    let forwarded_filtered: Vec<syn::Attribute> = func
        .attrs
        .iter()
        .filter(|attr| !(is_test_ignore_attr(attr) && strip_user_ignore))
        .cloned()
        .collect();
    let ignore_attr = if should_ignore {
        quote! { #[ignore] }
    } else {
        quote! {}
    };
    let user_block = &func.block;
    let expanded = quote! {
        #(#forwarded_filtered)*
        #ignore_attr
        #[::tokio::test]
        async fn #fn_ident() {
            let mut ctx = ::shakedown::Context::new();
            let mut event_log = ::shakedown::EventLog::new();

            #resources_registration

            {
                #migrations_tokens
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
    };

    #[allow(clippy::let_and_return)]
    expanded
}

#[cfg(test)]
mod tests;
