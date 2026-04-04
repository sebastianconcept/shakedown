//! Fail: `#[shakedown::test]` applied to a non-async function.
//!
//! **Given** a synchronous function annotated with the test attribute
//! **When** the macro checks for `asyncness`
//! **Then** it rejects the function with a clear compiler error.

#[shakedown::test]
fn sync_test(ctx: &mut shakedown::Context) {
    // This is wrong - must be async
}

fn main() {}
