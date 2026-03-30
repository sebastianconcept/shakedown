//! NATS typed helpers: publish + subscribe (Docker).
//!
//! Built with `--features nats`. **`#[ignore]`** unless `--ignored`.
//!
//! ```text
//! cargo test -p shakedown --features nats --test helpers_nats -- --ignored
//! ```

use std::time::Duration;

use bytes::Bytes;

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
