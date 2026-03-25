//! Runtime integration tests for `#[shakedown::test]` and `#[shakedown::faulty_test]`.
//!
//! These tests exercise the generated `run_scenario` wiring with [`FakeResource`] only — no Docker.
//!
//! **Optional Kafka + macro path:** when the `kafka` feature is enabled, see `tests/macro_kafka.rs`
//! (ignored by default; requires Docker). Run it with:
//!
//! ```text
//! cargo test -p shakedown --features kafka macro_kafka -- --ignored
//! ```

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

#[shakedown::test(resources = [Fake])]
async fn test_scenario_id_matches_function_name(ctx: &mut shakedown::Context) {
    assert_eq!(
        ctx.scenario_id().as_str(),
        "test_scenario_id_matches_function_name"
    );
}

#[shakedown::test]
async fn test_manual_register_before_body(ctx: &mut shakedown::Context) {
    ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    assert!(!ctx.is_empty());
}

#[shakedown::test(resources = [Fake])]
async fn empty_body(_ctx: &mut shakedown::Context) {}
