//! Pass: reserved `key = <expr>` args (other than `resources` / `fault_profile`) are ignored; `resources` still works.

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
