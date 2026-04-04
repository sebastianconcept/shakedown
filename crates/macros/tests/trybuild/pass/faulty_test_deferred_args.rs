//! Pass: `#[shakedown::faulty_test]` with unknown/deferred arguments.
//!
//! **Given** attribute arguments that are not `resources` or `fault_profile`
//! **When** the macro parses the attribute
//! **Then** unknown keys are ignored (no compile error) and the test still expands correctly,
//! with `ScenarioKind::Faulty` set.

#[shakedown::faulty_test(deferred_placeholder = 0, resources = [Fake])]
async fn faulty_with_deferred_then_resources(ctx: &mut shakedown::Context) {
    assert_eq!(ctx.scenario_kind(), shakedown::ScenarioKind::Faulty);
}

#[shakedown::faulty_test(resources = [Fake], deferred_placeholder = true)]
async fn faulty_resources_then_deferred(ctx: &mut shakedown::Context) {
    assert_eq!(ctx.scenario_kind(), shakedown::ScenarioKind::Faulty);
}

#[shakedown::faulty_test(deferred_only = "mvp-fault-injection")]
async fn faulty_deferred_only(ctx: &mut shakedown::Context) {
    assert_eq!(ctx.scenario_kind(), shakedown::ScenarioKind::Faulty);
}

fn main() {}
