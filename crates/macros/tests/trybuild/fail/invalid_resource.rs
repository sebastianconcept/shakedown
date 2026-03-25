//! Compile-fail test: #[shakedown::test] with invalid resource name (should fail).
//!
//! Expected to fail with clear compile error about unknown builtin resource.

#[shakedown::test(resources = [InvalidResource])]
async fn with_invalid_resource(ctx: &mut shakedown::Context) {
    // This should fail to compile because InvalidResource is not a known builtin
}

fn main() {}
