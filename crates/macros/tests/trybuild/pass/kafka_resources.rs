//! Pass: placeholder for `resources = [Kafka]` when the `kafka` feature is enabled.
//!
//! **Given** a test that would use `resources = [Kafka]`
//! **When** the `kafka` feature is active on the `shakedown` dependency
//! **Then** the macro should generate the correct feature-gated registration code.
//! (Currently uses `Fake` so the file remains a valid compile test.)

#[shakedown::test(resources = [Fake])]
async fn kafka_feature_placeholder(ctx: &mut shakedown::Context) {
    let _ = ctx;
}

fn main() {}
