use proc_macro2::TokenStream;
use quote::quote;
use syn::parse2;

fn item_fn_from_expanded(expanded: TokenStream) -> syn::ItemFn {
    parse2(expanded).expect("macro expansion should parse as ItemFn")
}

fn ignore_attribute_count(item: &syn::ItemFn) -> usize {
    item.attrs
        .iter()
        .filter(|attr| attr.path().is_ident("ignore"))
        .count()
}

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
fn duplicate_ignore_argument_is_rejected() {
    // Given duplicate `ignore` keys in the attribute
    let attr: TokenStream = quote!(resources = [Fake], ignore = true, ignore = false);
    // When parsed as test attribute arguments
    let result = super::parse_test_attr(attr);
    // Then parsing fails
    assert!(result.is_err());
}

#[test]
fn ignore_false_parses() {
    // Given `resources` and `ignore = false`
    let attr: TokenStream = quote!(resources = [Fake], ignore = false);
    // When parsed as test attribute arguments
    let parsed = super::parse_test_attr(attr).expect("parse");
    // Then `ignore` is false
    assert_eq!(parsed.ignore, Some(false));
}

#[test]
fn expand_resources_emits_single_ignore() {
    // Given the macro attribute requests a Docker-backed resource
    let attr: TokenStream = quote!(resources = [Fake]);
    let item: TokenStream = quote! {
        async fn scenario_auto(ctx: &mut Context) {}
    };
    // When the test macro expands
    let expanded = super::expand(attr, item);
    let item_fn = item_fn_from_expanded(expanded);
    // Then exactly one `#[ignore]` is present on the generated test function
    assert_eq!(ignore_attribute_count(&item_fn), 1);
}

#[test]
fn expand_resources_with_manual_ignore_is_idempotent() {
    // Given resources and a manual `#[ignore]` on the scenario function
    let attr: TokenStream = quote!(resources = [Fake]);
    let item: TokenStream = quote! {
        #[ignore]
        async fn scenario_manual(ctx: &mut Context) {}
    };
    // When the macro expands
    let expanded = super::expand(attr, item);
    let item_fn = item_fn_from_expanded(expanded);
    // Then the expansion still has exactly one `ignore` attribute (no duplicate)
    assert_eq!(ignore_attribute_count(&item_fn), 1);
}

#[test]
fn expand_resources_with_ignore_false_suppresses_auto_ignore() {
    // Given `ignore = false` with resources
    let attr: TokenStream = quote!(resources = [Fake], ignore = false);
    let item: TokenStream = quote! {
        async fn scenario_ci(ctx: &mut Context) {}
    };
    // When the macro expands
    let expanded = super::expand(attr, item);
    let item_fn = item_fn_from_expanded(expanded);
    // Then no `#[ignore]` is emitted
    assert_eq!(ignore_attribute_count(&item_fn), 0);
}

#[test]
fn expand_no_resources_does_not_emit_ignore() {
    // Given no attribute arguments
    let attr = TokenStream::new();
    let item: TokenStream = quote! {
        async fn scenario_plain(ctx: &mut Context) {}
    };
    // When the macro expands
    let expanded = super::expand(attr, item);
    let item_fn = item_fn_from_expanded(expanded);
    // Then there is no `#[ignore]`
    assert_eq!(ignore_attribute_count(&item_fn), 0);
}

#[test]
fn expand_empty_resources_list_does_not_emit_ignore() {
    // Given an empty `resources` list
    let attr: TokenStream = quote!(resources = []);
    let item: TokenStream = quote! {
        async fn scenario_empty_resources(ctx: &mut Context) {}
    };
    // When the macro expands
    let expanded = super::expand(attr, item);
    let item_fn = item_fn_from_expanded(expanded);
    // Then there is no `#[ignore]`
    assert_eq!(ignore_attribute_count(&item_fn), 0);
}
