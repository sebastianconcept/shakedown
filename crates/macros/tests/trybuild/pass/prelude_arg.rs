//! Pass test: `#[shakedown::test(prelude = { ... })]` runs before `run_scenario`.

#[shakedown_macros::test(prelude = {
    let _ = ctx.scenario_id();
})]
async fn test_with_prelude(ctx: &mut shakedown::Context) {
    let _ = ctx;
}

fn main() {}
