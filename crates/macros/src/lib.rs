//! Proc macros consumed by the workspace `lib` crate.

use proc_macro::TokenStream;
use quote::quote;

/// Expands to the project's default greeting string at compile time.
#[proc_macro]
pub fn default_greeting(_input: TokenStream) -> TokenStream {
    quote! {
        "Hello, shakedown!"
    }
    .into()
}
