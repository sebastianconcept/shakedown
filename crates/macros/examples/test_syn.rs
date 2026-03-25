use syn::{parse_str, Item};

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
