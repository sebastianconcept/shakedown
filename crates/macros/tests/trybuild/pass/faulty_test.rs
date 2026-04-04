//! Pass: basic `#[shakedown::faulty_test]` usage.
//!
//! **Given** a function annotated with `#[shakedown::faulty_test]`
//! **When** the macro expands it to a `#[tokio::test]`
//! **Then** the generated test sets `ScenarioKind::Faulty` on the context before calling `run_scenario`.

#[shakedown::faulty_test(resources = [Fake])]
async fn faulty_test(ctx: &mut shakedown::Context) {
    assert_eq!(ctx.scenario_kind(), shakedown::ScenarioKind::Faulty);
    assert!(!ctx.is_empty());
}

fn main() {}
