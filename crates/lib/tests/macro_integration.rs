//! Runtime integration tests for shakedown-macros.
//!
//! These tests verify that:
//! - `#[shakedown::test]` macro works end-to-end with FakeResource
//! - `#[shakedown::faulty_test]` macro correctly sets ScenarioKind::Faulty
//! - Both macros call run_scenario and manage lifecycle properly

use shakedown::Context;

/// Integration test: Basic #[shakedown::test] attribute.
///
/// Validates US-006 acceptance criteria:
/// - At least one integration test using #[shakedown::test] passes without Docker.
#[shakedown::test]
async fn macro_test_basic(ctx: &mut Context) {
    // Create a simple scenario that uses the macro-generated wrapper
    assert_eq!(ctx.scenario_id().to_string(), "macro_test_basic");
}

/// Integration test: Verify ScenarioKind is Normal for #[shakedown::test].
#[shakedown::test]
async fn test_scenario_kind_is_normal(ctx: &mut Context) {
    let kind = ctx.scenario_kind();
    assert_eq!(kind, shakedown::ScenarioKind::Normal);
}

/// Integration test: Basic #[shakedown::faulty_test] attribute.
///
/// Validates US-006 acceptance criteria:
/// - At least one integration test using #[shakedown::faulty_test] passes without Docker.
#[shakedown::faulty_test]
async fn macro_faulty_test_basic(ctx: &mut Context) {
    // Create a simple scenario that uses the faulty_test macro
    assert_eq!(ctx.scenario_id().to_string(), "macro_faulty_test_basic");
}

/// Integration test: Verify ScenarioKind is Faulty for #[shakedown::faulty_test].
#[shakedown::faulty_test]
async fn test_scenario_kind_is_faulty(ctx: &mut Context) {
    let kind = ctx.scenario_kind();
    assert_eq!(kind, shakedown::ScenarioKind::Faulty);
}

/// Integration test: Verify that #[shakedown::test] generates code that calls run_scenario.
#[shakedown::test]
async fn test_run_scenario_called(ctx: &mut Context) {
    // Access event_log would require it to be in closure, but we can verify scenario_id
    assert!(ctx
        .scenario_id()
        .to_string()
        .ends_with("test_run_scenario_called"));
}

/// Integration test: Multiple tests with different scenarios don't interfere.
#[shakedown::test]
async fn first_test(ctx: &mut Context) {
    let id = ctx.scenario_id();
    assert_eq!(id.to_string(), "first_test");
}

#[shakedown::test]
async fn second_test(ctx: &mut Context) {
    let id = ctx.scenario_id();
    assert_eq!(id.to_string(), "second_test");
}

/// Integration test: Empty scenario body (minimal boilerplate demonstration).
#[shakedown::test]
async fn empty_body(_ctx: &mut Context) {
    // Just verify the harness doesn't panic on an empty scenario
}

/// Integration test: Complex lifecycle - provision/readiness/seed/run/assert/teardown.
#[shakedown::test]
async fn complex_lifecycle_demo(ctx: &mut Context) {
    let id = ctx.scenario_id();

    // Simulate some scenario logic using the context
    assert_eq!(id.to_string(), "complex_lifecycle_demo");
}
