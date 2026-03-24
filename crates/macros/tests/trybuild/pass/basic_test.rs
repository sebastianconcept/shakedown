//! Compile-pass test: Basic #[shakedown::test] with FakeResource.
//!
//! Expected to compile and pass. This verifies the macro generates valid Rust.

#[cfg(test)]
mod tests {
    use shakedown::{Context, resources::FakeResource};

    // Basic test using FakeResource
    #[shakedown::test(resources = [Fake])]
    async fn order_created_event(ctx: &mut Context) {
        // Simple scenario that exercises the basic macro functionality
        let _scenario_id = ctx.scenario_id();
    }

    // Verify we can access context fields
    #[shakedown::test]
    async fn test_scenario_kind_access(ctx: &mut Context) {
        let _kind = ctx.scenario_kind();
        let _id = ctx.scenario_id();
    }
}
