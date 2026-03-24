//! Compile-fail test: #[shakedown::test] with wrong parameter signature (should fail).
//!
//! Expected to fail with clear error message.

#[shakedown::test]
async fn event_without_ctx(ctx: i32) {
    // This should fail to compile because the macro expects ctx: &mut Context
}
