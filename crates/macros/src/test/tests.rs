use proc_macro2::TokenStream;
use quote::quote;

#[test]
fn empty_attribute_parses_as_no_resources_and_no_prelude() {
    // Given an empty attribute token stream
    let attr = TokenStream::new();
    // When it is parsed as test attribute arguments
    let parsed = super::parse_test_attr(attr).expect("empty attr should parse");
    // Then no resources or prelude are set
    assert!(parsed.resources.is_none());
    assert!(parsed.prelude.is_none());
}

#[test]
fn resources_only_parses_fake_builtin() {
    // Given attribute tokens `resources = [Fake]`
    let attr: TokenStream = quote!(resources = [Fake]);
    // When parsed as test attribute arguments
    let parsed = super::parse_test_attr(attr).expect("resources = [Fake] should parse");
    // Then one resource spec is present and names Fake
    let specs = parsed.resources.as_ref().expect("resources should be set");
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].ident.to_string(), "Fake");
}

#[test]
fn duplicate_resources_argument_is_rejected() {
    // Given duplicate `resources` keys in the attribute
    let attr: TokenStream = quote!(resources = [Fake], resources = [Kafka]);
    // When parsed as test attribute arguments
    let result = super::parse_test_attr(attr);
    // Then parsing fails
    assert!(result.is_err());
}
