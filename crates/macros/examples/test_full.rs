//! Dev-only scratch binary: pretty-print a near-macro expansion wrapping a scenario body in `tokio::test`.

/// Parse a sample async scenario fn and emit the expanded token stream for inspection.
fn main() {
    let input = r#"async fn my_test(ctx: &mut shakedown::Context) {
        assert!(true);
    }"#;

    let func: syn::ItemFn = syn::parse_str(input).unwrap();
    eprintln!("Function name: {}", func.sig.ident);

    let fn_ident = &func.sig.ident;
    let user_block = &func.block;
    let expanded = quote::quote! {
        #[::tokio::test]
        async fn #fn_ident() {
            // User's scenario body
            #user_block
        }
    };

    let expanded_str = expanded.to_string();
    eprintln!("EXPANDED:\n{}", expanded_str);

    // Now try to compile it
    let _tokens: proc_macro2::TokenStream = expanded;
    eprintln!("Tokens created successfully");
}
