//! Compile-fail test: #[shakedown::test] on a sync function (should fail).
//!
//! Expected to fail with clear error message.

#[shakedown::test]
fn sync_order_event(ctx: &mut shakedown::Context) {
    // This should fail to compile because the macro requires an async function
}
