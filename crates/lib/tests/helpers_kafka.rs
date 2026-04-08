//! Kafka typed helpers: `produce_json` → `consume_one` and `consume_until` (Docker).
//!
//! This target is listed in `Cargo.toml` with `required-features = ["kafka"]`. Tests are
//! **`#[ignore]`** so `cargo test --workspace` skips Docker by default.
//!
//! With Docker available:
//!
//! ```text
//! cargo test -p shakedown --features kafka --test helpers_kafka -- --ignored
//! ```
//!
//! Parallel namespace isolation (same **logical** topic name, different scenario namespaces; run
//! with two test threads):
//!
//! ```text
//! cargo test -p shakedown --features kafka --test helpers_kafka -- --ignored --test-threads=2 parallel_same_logical_topic
//! ```

use serde::{Deserialize, Serialize};

/// Domain-style payload for JSON produce/consume round-trips.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct OrderEvent {
    order_id: String,
    amount_cents: u32,
}

#[shakedown::test(resources = [Kafka])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features kafka --test helpers_kafka -- --ignored"]
async fn kafka_helpers_produce_and_await_fresh_topic(ctx: &mut shakedown::Context) {
    // Given: a fresh topic where no consumer has subscribed yet — publish-first would lose the
    // message. produce_and_await subscribes before producing to avoid silent message loss.

    let event = OrderEvent {
        order_id: "paa-order-1".to_string(),
        amount_cents: 150,
    };
    let payload = serde_json::to_vec(&event).expect("serialize");

    // When: we call produce_and_await on a brand-new topic.
    let consumed = ctx
        .kafka()
        .produce_and_await("fresh_topic_paa", "paa-key-1", payload, None)
        .await
        .expect("produce_and_await should not lose messages on a fresh topic");

    // Then: the consumed message round-trips correctly.
    assert_eq!(consumed.key_str(), Some("paa-key-1"));
    let parsed: OrderEvent = consumed
        .payload_json()
        .expect("payload_json should deserialize the produced JSON body");
    assert_eq!(parsed, event);
}

#[shakedown::test(resources = [Kafka])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features kafka --test helpers_kafka -- --ignored"]
async fn kafka_helpers_parallel_same_logical_topic_a(ctx: &mut shakedown::Context) {
    let payload = b"parallel-thread-a".to_vec();
    let consumed = ctx
        .kafka()
        .produce_and_await("shared_logical_topic", "k", payload.clone(), None)
        .await
        .expect("produce_and_await should isolate by scenario namespace");
    assert_eq!(consumed.payload.as_deref(), Some(payload.as_slice()));
}

#[shakedown::test(resources = [Kafka])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features kafka --test helpers_kafka -- --ignored"]
async fn kafka_helpers_parallel_same_logical_topic_b(ctx: &mut shakedown::Context) {
    let payload = b"parallel-thread-b".to_vec();
    let consumed = ctx
        .kafka()
        .produce_and_await("shared_logical_topic", "k", payload.clone(), None)
        .await
        .expect("produce_and_await should isolate by scenario namespace");
    assert_eq!(consumed.payload.as_deref(), Some(payload.as_slice()));
}

#[shakedown::test(resources = [Kafka])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features kafka --test helpers_kafka -- --ignored"]
async fn kafka_helpers_produce_json_consume_one_and_consume_until_with_predicate(
    ctx: &mut shakedown::Context,
) {
    // Given: a scenario with the Kafka resource provisioned (macro attribute) and isolated topics.

    let order = OrderEvent {
        order_id: "order-1".to_string(),
        amount_cents: 99,
    };

    // When: we publish a single JSON order to the logical topic `orders`.
    ctx.kafka()
        .produce_json("orders", "order-1", &order)
        .await
        .expect("produce_json should succeed with provisioned Kafka bindings");

    // Then: `consume_one` returns that message; key and payload round-trip through JSON.
    let consumed = ctx
        .kafka()
        .consume_one("orders", None)
        .await
        .expect("consume_one should see the produced record");

    assert_eq!(consumed.key_str(), Some("order-1"));
    let parsed: OrderEvent = consumed
        .payload_json()
        .expect("payload_json should deserialize the produced JSON body");
    assert_eq!(parsed, order);

    // Given: a second logical topic with two messages where only one matches our assertion.
    ctx.kafka()
        .produce_json(
            "orders_filtered",
            "noise",
            &OrderEvent {
                order_id: "skip-me".to_string(),
                amount_cents: 1,
            },
        )
        .await
        .expect("produce_json noise");
    ctx.kafka()
        .produce_json(
            "orders_filtered",
            "order-target",
            &OrderEvent {
                order_id: "wanted".to_string(),
                amount_cents: 42,
            },
        )
        .await
        .expect("produce_json target");

    // When: we poll with a predicate that ignores the noise key.
    // Then: `consume_until` yields the matching message only.
    let matched = ctx
        .kafka()
        .consume_until(
            "orders_filtered",
            |message| {
                message.key_str() == Some("order-target")
                    && message
                        .payload_json::<OrderEvent>()
                        .map(|event| event.order_id == "wanted" && event.amount_cents == 42)
                        .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("consume_until should find the keyed message");

    assert_eq!(matched.key_str(), Some("order-target"));
    let wanted: OrderEvent = matched
        .payload_json()
        .expect("payload_json on filtered result");
    assert_eq!(wanted.order_id, "wanted");
    assert_eq!(wanted.amount_cents, 42);
}
