//! Reserved for Kafka compile-pass when `shakedown-macros` wires a `kafka` dev-dependency feature.
//! Today `Kafka` in `resources = [...]` is validated only under `cargo test -p shakedown --features kafka`
//! (integration tests). Duplicate Fake case kept so this file stays a valid future extension point.

#[shakedown::test(resources = [Fake])]
async fn kafka_feature_placeholder(ctx: &mut shakedown::Context) {
    let _ = ctx;
}

fn main() {}
