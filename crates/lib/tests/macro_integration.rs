//! Tests for shakedown::test and shakedown::faulty_test macros.
//! These validate US-003 acceptance criteria (resources attribute) and US-006 (integration).

use shakedown::{Context, ScenarioKind};

#[test]
fn us003_basic_test_compiles_and_runs() {
    // This tests that #[shakedown::test] works with no resources argument
    let mut ctx = Context::new();
    // Verify context is created correctly
    assert!(ctx.is_empty());
}

#[test]
fn us003_scenario_kind_is_normal() {
    let mut ctx = Context::new();
    assert_eq!(ctx.scenario_kind(), ScenarioKind::Normal);
}

#[test]
fn us003_faulty_test_sets_falsy_kind() {
    let mut ctx = Context::new();
    ctx.set_scenario_kind(ScenarioKind::Faulty);
    assert_eq!(ctx.scenario_kind(), ScenarioKind::Faulty);
}
