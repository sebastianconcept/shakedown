//! Macro + real Kafka end-to-end test (optional).
//!
//! This target is listed in `Cargo.toml` with `required-features = ["kafka"]`, so it is only built
//! when the `kafka` feature is enabled. The test is **`#[ignore]`** so normal `cargo test` runs skip
//! Docker; `cargo test --workspace` also skips this binary when `kafka` is off.
//!
//! With Docker available:
//!
//! ```text
//! cargo test -p shakedown --features kafka macro_kafka -- --ignored
//! ```

use shakedown::{BuiltinResource, RegisteredResource};

#[shakedown::test(resources = [Kafka])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features kafka macro_kafka -- --ignored"]
async fn macro_attributed_kafka_provisions_and_exposes_bindings(ctx: &mut shakedown::Context) {
    let bindings = ctx
        .kafka_bindings()
        .expect("kafka bindings after Kafka provision");
    assert!(!bindings.bootstrap_servers.is_empty());
    assert!(!bindings.topic_prefix.is_empty());

    assert!(
        matches!(
            ctx.resources().first(),
            Some(RegisteredResource::Builtin(BuiltinResource::Kafka(_)))
        ),
        "expected Kafka builtin registered from attribute"
    );
}
