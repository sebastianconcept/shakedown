fn main() {
    let input = r#"async fn my_test(ctx: &mut shakedown::Context) {
        assert!(true);
    }"#;

    let func: syn::ItemFn = syn::parse_str(input).unwrap();

    // Try using the ident field directly
    let expanded = quote::quote! {
        fn #func.sig.ident() {}
    };
    eprintln!("Expanded: {}", expanded);

    // Try extracting it first
    let ident = func.sig.ident;
    let expanded2 = quote::quote! {
        fn #ident() {}
    };
    eprintln!("Expanded2: {}", expanded2);
}
