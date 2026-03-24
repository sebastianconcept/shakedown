//! Minimal macro integration test.

use shakedown::Context;

#[shakedown::test]
async fn minimal_test(ctx: &mut Context) {
    assert_eq!(ctx.scenario_id().to_string(), "minimal_test");
}
