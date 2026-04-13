//! Fail: `#[shakedown::test(resources = [InvalidResource(migrations = "...")])]` — unknown builtin with builtin-style config.
//!
//! Unknown resource names must use a Rust expression (e.g. `crate::MyResource::new()`), not the
//! `Name(key = value)` form reserved for first-party builtins.

#[shakedown::test(resources = [InvalidResource(migrations = "./migrations")])]
async fn with_invalid_resource(ctx: &mut shakedown::Context) {
    let _ = ctx;
}

fn main() {}
