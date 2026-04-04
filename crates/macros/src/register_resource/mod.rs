//! Implementation of the `#[shakedown::register_resource]` attribute macro.

use proc_macro2::TokenStream;
use quote::quote;

/// Pass-through expansion for `#[shakedown::register_resource]` (see crate-level docs on `lib.rs`).
pub(crate) fn expand(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let parsed_item = match syn::parse2::<syn::Item>(item) {
        Ok(i) => i,
        Err(e) => return e.to_compile_error(),
    };

    quote! { #parsed_item }
}

#[cfg(test)]
mod tests;
