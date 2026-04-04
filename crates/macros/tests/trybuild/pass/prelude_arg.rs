//! Pass: `#[shakedown::test(prelude = { ... })]` executes user code before scenario lifecycle.
//!
//! **Given** a `prelude` block in the test attribute
//! **When** the macro expands the test
//! **Then** the prelude block runs after resource registration but before `run_scenario`,
//! allowing registration of mock services, etc.

#[shakedown_macros::test(prelude = {
    let _ = ctx.scenario_id();
})]
async fn test_with_prelude(ctx: &mut shakedown::Context) {
    let _ = ctx;
}

fn main() {}
