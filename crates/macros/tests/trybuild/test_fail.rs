//! Compile-fail tests for shakedown-macros.
//!
//! These tests verify that invalid macro usage produces clear compile errors.

use shakedown::Context;

#[shakedown::test]
fn sync_order_event(ctx: &mut Context) {
    // This should fail to compile because the macro requires an async function
}
