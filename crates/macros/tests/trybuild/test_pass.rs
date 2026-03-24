//! Compile-pass tests for shakedown-macros.
//!
//! These tests verify that valid macro usage compiles successfully.

use shakedown::Context;

#[shakedown::test]
async fn order_created_event(ctx: &mut Context) {
    // Simple scenario that exercises the basic macro functionality
    let _scenario_id = ctx.scenario_id();
}

#[shakedown::faulty_test]
async fn faulty_scenario(ctx: &mut Context) {
    // Faulty scenario that marks the test as ScenarioKind::Faulty
    let _kind = ctx.scenario_kind();
}

#[shakedown::test]
async fn with_manual_resource_registration(ctx: &mut Context) {
    // Test without specifying resources in attribute (user registers manually)
    let _scenario_id = ctx.scenario_id();
}
