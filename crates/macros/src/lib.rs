//! Proc macros consumed by the workspace `lib` crate.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Registers a resource for use in Shakedown scenarios.
///
/// This macro generates the necessary boilerplate to register a custom
/// resource with the Shakedown framework.
///
/// # Example
///
/// ```ignore
/// use shakedown::Context;
/// use shakedown_macros::register_resource;
///
/// #[register_resource]
/// struct MyDatabase {
///     host: String,
///     port: u16,
/// }
///
/// impl shakedown::Resource for MyDatabase {
///     fn provision(_ctx: &mut Context) -> Self {
///         // Implementation here
///         MyDatabase {
///             host: "localhost".to_string(),
///             port: 5432,
///         }
///     }
///
///     fn apply_default_seed(&self) {
///         // Seed data initialization
///     }
///
///     fn check_ready(&self) -> bool {
///         // Check if resource is ready
///         true
///     }
///
///     fn teardown(self) {
///         // Cleanup resources
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn register_resource(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    // For now, just pass through the item unchanged
    // The actual registration will happen at a higher level
    let output = quote! {
        #input
    };

    TokenStream::from(output)
}

/// Expands to the project's default greeting string at compile time.
#[proc_macro]
pub fn default_greeting(_input: TokenStream) -> TokenStream {
    quote! {
        "Hello, shakedown!"
    }
    .into()
}
