//! Compile-pass test: #[shakedown::test] with optional resources argument.
//!
//! Expected to compile and pass. This verifies the macro correctly handles the
//! resources attribute argument.

#[cfg(test)]
mod tests {
    use shakedown::{Context, resources::FakeResource};

    // Test with Fake resource specified in attribute
    #[shakedown::test(resources = [Fake])]
    async fn with_fake_resource(ctx: &mut Context) {
        let _scenario_id = ctx.scenario_id();
    }

    // Test without specifying resources (user registers manually)
    #[shakedown::test]
    async fn manual_resource_registration(ctx: &mut Context) {
        // User can register resources inside the test body before their logic
        let _ = ctx.scenario_id();
    }
}
