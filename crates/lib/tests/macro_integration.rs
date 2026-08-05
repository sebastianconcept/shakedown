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

#[shakedown::test(resources = [Fake], ignore = false)]
async fn macro_test_basic(ctx: &mut shakedown::Context) {
    // Given a context with Fake registered by the attribute
    // When the scenario runs
    // Then the context already holds resources
    assert!(!ctx.is_empty());
}

#[shakedown::test(resources = [Fake], ignore = false)]
async fn test_scenario_kind_is_normal(ctx: &mut shakedown::Context) {
    // Given a normal `#[shakedown::test]` scenario
    // When we read the scenario kind
    // Then it is Normal
    assert_eq!(ctx.scenario_kind(), ScenarioKind::Normal);
}

#[shakedown::faulty_test(resources = [Fake], ignore = false)]
async fn macro_faulty_test_basic(ctx: &mut shakedown::Context) {
    // Given a faulty scenario with Fake registered by the attribute
    // When the scenario runs
    // Then resources are present like a normal test
    assert!(!ctx.is_empty());
}

#[shakedown::faulty_test(resources = [Fake], ignore = false)]
async fn test_scenario_kind_is_faulty(ctx: &mut shakedown::Context) {
    // Given a `#[shakedown::faulty_test]` scenario
    // When we read the scenario kind
    // Then it is Faulty
    assert_eq!(ctx.scenario_kind(), ScenarioKind::Faulty);
}

#[shakedown::test(resources = [Fake], ignore = false)]
async fn test_run_scenario_registers_fake(ctx: &mut shakedown::Context) {
    // Given the macro-declared Fake resource list
    // When we inspect registered resources
    // Then the first entry is the fake builtin
    assert!(matches!(
        ctx.resources().first(),
        Some(RegisteredResource::Builtin(BuiltinResource::Fake(_)))
    ));
}

#[shakedown::test(resources = [Fake], ignore = false)]
async fn test_scenario_id_matches_function_name(ctx: &mut shakedown::Context) {
    // Given this function's Rust identifier as the scenario name source
    // When we read the scenario id from the context
    // Then it matches this async fn's name
    assert_eq!(
        ctx.scenario_id().as_str(),
        "test_scenario_id_matches_function_name"
    );
}

#[shakedown::test]
async fn test_manual_register_before_body(ctx: &mut shakedown::Context) {
    // Given a context without attribute-driven resources
    // When we register Fake manually before assertions
    ctx.register_builtin(BuiltinResource::Fake(FakeResource::default()));
    // Then the context is non-empty
    assert!(!ctx.is_empty());
}

#[shakedown::test(resources = [Fake], ignore = false)]
async fn empty_body(_ctx: &mut shakedown::Context) {
    // Given resources from the attribute and a scenario that asserts nothing
    // When the harness runs this body
    // Then completing without panic is success
}
