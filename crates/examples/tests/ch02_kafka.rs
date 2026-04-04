//! Chapter 2: Kafka produce and consume helpers
//!
//! **Resources:** Kafka (Docker)
//! **Assertions:** `produce_json`, `consume_one`, `consume_until` with predicate, key and payload checks
//! **Concepts:** logical topic names, scenario-scoped namespaces, JSON round-trips
//! **Teaching intent:** Learn the minimal publish → assert loop for order-domain events before you
//!   wire a real service.
//!
//! Run with Docker: `cargo test -p shakedown-examples --features kafka --test ch02_kafka -- --ignored`

use serde::{Deserialize, Serialize};

/// Order-placed event carried on the `orders` topic (same shape the example services use later).
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct OrderPlacedEvent {
    aggregate_id: String,
    customer_id: String,
    amount_cents: u32,
}

#[shakedown::test(resources = [Kafka])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features kafka --test ch02_kafka -- --ignored"]
async fn kafka_produce_consume_and_filter_by_predicate(ctx: &mut shakedown::Context) {
    let primary = OrderPlacedEvent {
        aggregate_id: "order-ch02-a".to_string(),
        customer_id: "customer-wholesale-7".to_string(),
        amount_cents: 12_450,
    };

    ctx.kafka()
        .produce_json("orders", primary.aggregate_id.as_str(), &primary)
        .await
        .expect("produce_json primary order event");

    let consumed = ctx
        .kafka()
        .consume_one("orders", None)
        .await
        .expect("consume_one should read the produced record");

    assert_eq!(consumed.key_str(), Some(primary.aggregate_id.as_str()));
    let parsed: OrderPlacedEvent = consumed
        .payload_json()
        .expect("payload should deserialize to OrderPlacedEvent");
    assert_eq!(parsed, primary);

    ctx.kafka()
        .produce_json(
            "orders",
            "order-ch02-noise",
            &OrderPlacedEvent {
                aggregate_id: "order-ch02-noise".to_string(),
                customer_id: "customer-retail-1".to_string(),
                amount_cents: 100,
            },
        )
        .await
        .expect("produce_json noise");

    let target = OrderPlacedEvent {
        aggregate_id: "order-ch02-target".to_string(),
        customer_id: "customer-retail-2".to_string(),
        amount_cents: 5_000,
    };
    ctx.kafka()
        .produce_json("orders", target.aggregate_id.as_str(), &target)
        .await
        .expect("produce_json target");

    let matched = ctx
        .kafka()
        .consume_until(
            "orders",
            |message| {
                message.key_str() == Some("order-ch02-target")
                    && message
                        .payload_json::<OrderPlacedEvent>()
                        .map(|event| {
                            event.aggregate_id == "order-ch02-target" && event.amount_cents == 5_000
                        })
                        .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("consume_until should skip noise and match the target key and payload");

    assert_eq!(matched.key_str(), Some("order-ch02-target"));
}
