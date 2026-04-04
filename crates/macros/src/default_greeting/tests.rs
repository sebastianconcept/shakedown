use proc_macro2::TokenStream;

#[test]
fn expands_to_hello_shakedown_literal() {
    // Given no macro input
    let input = TokenStream::new();
    // When default_greeting expands
    let expanded = super::expand(input);
    // Then the token stream is a string literal with the expected text
    let parsed = syn::parse2::<syn::LitStr>(expanded).expect("should be a string literal");
    assert_eq!(parsed.value(), "Hello, shakedown!");
}
