//! Minimal macro integration test.
//!
//! This file validates US-006 acceptance criteria for `#[shakedown::test]`:
//! - At least one integration test using #[shakedown::test] passes without Docker

use shakedown::Context;

// Minimal scenario with no body
#[shakedown::test]
async fn minimal_test(ctx: &mut Context) {
    assert_eq!(ctx.scenario_id().to_string(), "minimal_test");
}
