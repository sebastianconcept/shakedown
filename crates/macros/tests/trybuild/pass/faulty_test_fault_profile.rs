//! Pass: `#[shakedown::faulty_test(fault_profile = "...")]`.
//!
//! **Given** a `fault_profile` string literal in the attribute
//! **When** the macro expands `#[shakedown::faulty_test]`
//! **Then** it calls `ctx.set_fault_profile(...)` and sets `ScenarioKind::Faulty` before the scenario runs.

#[shakedown::faulty_test(fault_profile = "high_latency")]
async fn with_fault_profile(ctx: &mut shakedown::Context) {
    assert_eq!(ctx.fault_profile(), Some("high_latency"));
    assert_eq!(ctx.scenario_kind(), shakedown::ScenarioKind::Faulty);
}

#[shakedown::faulty_test(fault_profile = "kafka_timeout", resources = [Fake])]
async fn profile_and_resources(ctx: &mut shakedown::Context) {
    assert_eq!(ctx.fault_profile(), Some("kafka_timeout"));
}

fn main() {}
