//! Implementation of the `default_greeting!` proc macro.

use proc_macro2::TokenStream;
use quote::quote;
use syn::LitStr;

/// Expands `default_greeting!` to a string literal (see crate-level docs on `lib.rs`).
pub(crate) fn expand(_input: TokenStream) -> TokenStream {
    let lit = LitStr::new("Hello, shakedown!", proc_macro2::Span::call_site());
    quote! { #lit }
}

#[cfg(test)]
mod tests;
