//! Pass: `#[shakedown::faulty_test]` sets `ScenarioKind::Faulty` before the scenario body runs.

#[shakedown::faulty_test(resources = [Fake])]
async fn faulty_test(ctx: &mut shakedown::Context) {
    assert_eq!(ctx.scenario_kind(), shakedown::ScenarioKind::Faulty);
    assert!(!ctx.is_empty());
}

fn main() {}
