//! Pass: `#[shakedown::test(resources = [...])]` with builtin resource registration.
//!
//! **Given** a test annotated with `resources = [Fake]`
//! **When** the macro expands the attribute
//! **Then** it generates code that registers the builtin resource on the `Context`
//! before the scenario runs (and the test can observe `!ctx.is_empty()`).

#[shakedown_macros::test(resources = [Fake])]
async fn test_with_fake(ctx: &mut shakedown::Context) {
    assert!(!ctx.is_empty());
}

fn main() {}
