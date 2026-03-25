use shakedown::Context;

#[shakedown::test]
async fn simple_integration_test(ctx: &mut Context) {
    // Given a scenario context produced by the shakedown test harness
    // When we read the scenario id the macro attached
    // Then it matches this function's name
    assert_eq!(ctx.scenario_id().as_str(), "simple_integration_test");
}

#[test]
fn regular_test() {
    // Given a plain unit test outside the macro harness
    // When we assert a trivial truth
    // Then it holds
    assert!(true);
}
