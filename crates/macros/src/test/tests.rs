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
    assert!(parsed.ignore.is_none());
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

#[test]
fn ignore_false_parses() {
    let attr: TokenStream = quote!(resources = [Fake], ignore = false);
    let parsed = super::parse_test_attr(attr).expect("parse");
    assert_eq!(parsed.ignore, Some(false));
}

#[test]
fn expand_resources_emits_single_ignore() {
    let attr: TokenStream = quote!(resources = [Fake]);
    let item: TokenStream = quote! {
        async fn scenario_auto(ctx: &mut Context) {}
    };
    let expanded = super::expand(attr, item);
    let rendered = expanded.to_string();
    assert!(
        rendered.contains("ignore"),
        "expected #[ignore] in expansion: {rendered}"
    );
}

#[test]
fn expand_resources_with_manual_ignore_is_idempotent() {
    let attr: TokenStream = quote!(resources = [Fake]);
    let item: TokenStream = quote! {
        #[ignore]
        async fn scenario_manual(ctx: &mut Context) {}
    };
    let expanded = super::expand(attr, item);
    let rendered = expanded.to_string();
    let ignore_count = rendered.matches("ignore").count();
    assert_eq!(
        ignore_count, 1,
        "expected exactly one ignore attribute, got {ignore_count} in: {rendered}"
    );
}

#[test]
fn expand_resources_with_ignore_false_suppresses_auto_ignore() {
    let attr: TokenStream = quote!(resources = [Fake], ignore = false);
    let item: TokenStream = quote! {
        async fn scenario_ci(ctx: &mut Context) {}
    };
    let expanded = super::expand(attr, item);
    let rendered = expanded.to_string();
    assert!(
        !rendered.contains("ignore"),
        "expected no #[ignore] when ignore = false: {rendered}"
    );
}

#[test]
fn expand_no_resources_does_not_emit_ignore() {
    let attr = TokenStream::new();
    let item: TokenStream = quote! {
        async fn scenario_plain(ctx: &mut Context) {}
    };
    let expanded = super::expand(attr, item);
    let rendered = expanded.to_string();
    assert!(
        !rendered.contains("ignore"),
        "expected no #[ignore] without resources: {rendered}"
    );
}

#[test]
fn expand_empty_resources_list_does_not_emit_ignore() {
    let attr: TokenStream = quote!(resources = []);
    let item: TokenStream = quote! {
        async fn scenario_empty_resources(ctx: &mut Context) {}
    };
    let expanded = super::expand(attr, item);
    let rendered = expanded.to_string();
    assert!(
        !rendered.contains("ignore"),
        "expected no #[ignore] when resources list is empty: {rendered}"
    );
}
