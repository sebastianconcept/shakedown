use proc_macro2::TokenStream;
use quote::quote;

#[test]
fn valid_item_passes_through_expansion() {
    // Given a plain function item token stream
    let item: TokenStream = quote! {
        fn example() {}
    };
    // When register_resource expands it
    let expanded = super::expand(TokenStream::new(), item);
    // Then output still parses as the same item shape
    let parsed = syn::parse2::<syn::Item>(expanded).expect("expanded tokens should parse");
    assert!(matches!(parsed, syn::Item::Fn(_)));
}
