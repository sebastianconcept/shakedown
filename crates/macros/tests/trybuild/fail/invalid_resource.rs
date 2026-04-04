//! Fail: `#[shakedown::test(resources = [InvalidResource])]` with unknown builtin.
//!
//! **Given** a resource name that is not one of the supported builtins
//! **When** `build_resource_registration` is called during macro expansion
//! **Then** it returns a `syn::Error` that produces a helpful compiler diagnostic
//! listing all valid resource types.

#[shakedown::test(resources = [InvalidResource])]
async fn with_invalid_resource(ctx: &mut shakedown::Context) {
    // This should fail to compile because InvalidResource is not a known builtin
}

fn main() {}
