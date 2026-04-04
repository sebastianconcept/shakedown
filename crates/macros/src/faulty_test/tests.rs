use proc_macro2::TokenStream;
use quote::quote;

#[test]
fn fault_profile_string_parses() {
    // Given `fault_profile = "latency"` alongside resources
    let attr: TokenStream = quote!(resources = [Fake], fault_profile = "latency");
    // When parsed as faulty_test attribute arguments
    let parsed = super::parse_faulty_test_attr(attr).expect("attr should parse");
    // Then fault profile is captured
    assert_eq!(parsed.fault_profile.as_deref(), Some("latency"));
    assert!(parsed.resources.is_some());
}

#[test]
fn fault_profile_non_string_is_rejected() {
    // Given a non-literal fault_profile value
    let attr: TokenStream = quote!(fault_profile = 1);
    // When parsed as faulty_test attribute arguments
    let result = super::parse_faulty_test_attr(attr);
    // Then parsing fails
    assert!(result.is_err());
}

#[test]
fn unknown_keys_are_ignored_and_parsed() {
    // Given a future/unknown key with an expression (parsed and dropped)
    let attr: TokenStream = quote!(future_flag = true);
    // When parsed
    let parsed = super::parse_faulty_test_attr(attr).expect("unknown keys should parse through");
    // Then resources and fault_profile stay unset
    assert!(parsed.resources.is_none());
    assert!(parsed.fault_profile.is_none());
}
