//! Dev-only scratch binary: parse a scenario-shaped function with `syn` and walk the AST.

use syn::{parse_str, Item};

/// Parse inline Rust source and print discovered item names.
fn main() {
    // Simulate parsing a test function
    let input = r#"async fn my_test(ctx: &mut shakedown::Context) {
        assert!(true);
    }"#;

    let ast: syn::File = parse_str(input).unwrap();
    println!("Parsed {} items", ast.items.len());
    for item in &ast.items {
        if let Item::Fn(func) = item {
            println!("  Function name: {}", func.sig.ident);
        }
    }
}
