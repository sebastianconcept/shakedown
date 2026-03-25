//! Runtime integration tests for `#[shakedown::test]` / `#[shakedown::faulty_test]`.

use shakedown::{BuiltinResource, FakeResource, RegisteredResource, ScenarioKind};

#[shakedown::test(resources = [Fake])]
async fn macro_test_basic(ctx: &mut shakedown::Context) {
    assert!(!ctx.is_empty());
}

#[shakedown::test(resources = [Fake])]
async fn test_scenario_kind_is_normal(ctx: &mut shakedown::Context) {
    assert_eq!(ctx.scenario_kind(), ScenarioKind::Normal);
}

#[shakedown::faulty_test(resources = [Fake])]
async fn macro_faulty_test_basic(ctx: &mut shakedown::Context) {
    assert!(!ctx.is_empty());
}

#[shakedown::faulty_test(resources = [Fake])]
async fn test_scenario_kind_is_faulty(ctx: &mut shakedown::Context) {
    assert_eq!(ctx.scenario_kind(), ScenarioKind::Faulty);
}

#[shakedown::test(resources = [Fake])]
async fn test_run_scenario_registers_fake(ctx: &mut shakedown::Context) {
    assert!(matches!(
        ctx.resources().first(),
        Some(RegisteredResource::Builtin(BuiltinResource::Fake(_)))
    ));
}

#[shakedown::test]
async fn test_manual_register_before_body(ctx: &mut shakedown::Context) {
    ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    assert!(!ctx.is_empty());
}

#[shakedown::test(resources = [Fake])]
async fn empty_body(_ctx: &mut shakedown::Context) {}
