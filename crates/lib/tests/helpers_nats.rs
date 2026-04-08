//! NATS typed helpers: publish + subscribe (Docker).
//!
//! Built with `--features nats`. **`#[ignore]`** unless `--ignored`.
//!
//! ```text
//! cargo test -p shakedown --features nats --test helpers_nats -- --ignored
//! ```
//!
//! Parallel isolation (pair with `--test-threads=2` and filter `parallel_same_logical`):
//!
//! ```text
//! cargo test -p shakedown --features nats --test helpers_nats -- --ignored --test-threads=2 parallel_same_logical
//! ```

use std::time::Duration;

use bytes::Bytes;

#[shakedown::test(resources = [Nats])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features nats --test helpers_nats -- --ignored"]
async fn nats_helpers_publish_and_await_fresh_subject(ctx: &mut shakedown::Context) {
    // Given: a fresh NATS subject where no subscriber exists yet — core NATS drops messages
    // published before a subscriber is registered. publish_and_await subscribes first to avoid
    // silent message loss.
    let payload: &[u8] = b"nats_paa_payload";

    // When: we call publish_and_await on a brand-new subject.
    let consumed = ctx
        .nats()
        .publish_and_await("fresh_subject_paa", payload, Some(Duration::from_secs(10)))
        .await
        .expect("publish_and_await should not lose messages on a fresh subject");

    // Then: the consumed message payload matches.
    assert_eq!(consumed.payload.as_deref(), Some(payload));
}

#[shakedown::test(resources = [Nats])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features nats --test helpers_nats -- --ignored"]
async fn nats_helpers_parallel_same_logical_subject_a(ctx: &mut shakedown::Context) {
    let payload: &[u8] = b"parallel-nats-a";
    let consumed = ctx
        .nats()
        .publish_and_await("shared_logical_subj", payload, None)
        .await
        .expect("publish_and_await should isolate by scenario namespace");
    assert_eq!(consumed.payload.as_deref(), Some(payload));
}

#[shakedown::test(resources = [Nats])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features nats --test helpers_nats -- --ignored"]
async fn nats_helpers_parallel_same_logical_subject_b(ctx: &mut shakedown::Context) {
    let payload: &[u8] = b"parallel-nats-b";
    let consumed = ctx
        .nats()
        .publish_and_await("shared_logical_subj", payload, None)
        .await
        .expect("publish_and_await should isolate by scenario namespace");
    assert_eq!(consumed.payload.as_deref(), Some(payload));
}

#[shakedown::test(resources = [Nats])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features nats --test helpers_nats -- --ignored"]
async fn nats_helpers_publish_then_subscribe(ctx: &mut shakedown::Context) {
    let bindings = ctx.nats_bindings().expect("nats bindings");
    let connection_url = bindings.connection_url.clone();
    let full_subject = bindings.subject_namespace("helpers_evt");
    let payload: &[u8] = b"helpers_nats_payload";

    let publisher = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let client = async_nats::connect(connection_url.as_str())
            .await
            .expect("publisher connect");
        client
            .publish(full_subject, Bytes::copy_from_slice(payload))
            .await
            .expect("publish");
        let _ = client.drain().await;
    });

    let messages = ctx
        .nats()
        .subscribe_until(
            "helpers_evt",
            |message| message.payload.as_deref() == Some(payload),
            Some(Duration::from_secs(10)),
        )
        .await
        .expect("subscribe_until");

    publisher.await.expect("publisher task join");

    assert!(
        messages
            .iter()
            .any(|message| message.payload.as_deref() == Some(payload)),
        "expected payload in collected messages"
    );
}
