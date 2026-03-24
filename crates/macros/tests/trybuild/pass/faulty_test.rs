//! Compile-pass test: Basic #[shakedown::faulty_test] with FakeResource.
//!
//! Expected to compile and pass. This verifies the faulty_test macro generates valid Rust.

#[cfg(test)]
mod tests {
    use shakedown::{Context, resources::FakeResource};

    // Faulty test using FakeResource
    #[shakedown::faulty_test(resources = [Fake])]
    async fn faulty_scenario(ctx: &mut Context) {
        // Faulty scenario that marks the test as ScenarioKind::Faulty
        let _kind = ctx.scenario_kind();
        assert_eq!(_kind, shakedown::ScenarioKind::Faulty);
    }

    // Verify we can access context fields and verify ScenarioKind
    #[shakedown::faulty_test]
    async fn test_faulty_marker(ctx: &mut Context) {
        let kind = ctx.scenario_kind();
        assert_eq!(kind, shakedown::ScenarioKind::Faulty);
    }
}
