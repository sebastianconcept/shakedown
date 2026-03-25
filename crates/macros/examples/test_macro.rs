//! Dev-only scratch binary: prototype `#[tokio::test]` wrapping of a user scenario block.

/// Parse a sample function and print the `quote!` output for manual review.
fn main() {
    let input = r#"async fn my_test(ctx: &mut shakedown::Context) {
        assert!(true);
    }"#;

    let func: syn::ItemFn = syn::parse_str(input).unwrap();
    eprintln!("Function name: {}", func.sig.ident);

    // Create a new function with tokio::test
    let fn_name = func.sig.ident;
    let block = &func.block;

    // Try without async - we'll add it via the attribute
    let expanded = quote::quote! {
        #[::tokio::test]
        async fn #fn_name() {
            // User's original function body
            { #block }
        }
    };
    eprintln!("EXPANDED: {}", expanded);
}
