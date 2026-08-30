//! register_resource is the pass-through expansion for `#[shakedown::register_resource]`.

use proc_macro2::TokenStream;
use quote::quote;

/// expand returns the input item unchanged, or a compile error if the item does not parse.
pub(crate) fn expand(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let parsed_item = match syn::parse2::<syn::Item>(item) {
        Ok(i) => i,
        Err(e) => return e.to_compile_error(),
    };

    quote! { #parsed_item }
}

#[cfg(test)]
mod tests;
