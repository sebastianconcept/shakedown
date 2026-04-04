//! Fail: `#[shakedown::test]` with incorrect function signature.
//!
//! **Given** a test function whose first parameter is not `ctx: &mut shakedown::Context`
//! **When** the macro validates the signature
//! **Then** it emits a compile error with a clear diagnostic message.

#[shakedown::test]
async fn wrong_param(ctx: shakedown::Context) {
    // Wrong - should be &mut Context, not owned value
}

fn main() {}
