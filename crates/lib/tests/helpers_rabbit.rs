//! RabbitMQ typed helpers: `declare_topology` + `publish_json` → `consume_until` (Docker).
//!
//! This target is listed in `Cargo.toml` with `required-features = ["rabbitmq"]`. Tests are
//! **`#[ignore]`** so `cargo test --workspace` skips Docker by default.
//!
//! `declare_topology` replaces the typical manual sequence (exchange_declare, queue_declare,
//! queue_bind across tens of lines) with one harness call that applies scenario namespacing.
//!
//! With Docker available:
//!
//! ```text
//! cargo test -p shakedown --features rabbitmq --test helpers_rabbit -- --ignored
//! ```
//!
//! Parallel isolation (pair with `--test-threads=2` and filter `parallel_same_logical`):
//!
//! ```text
//! cargo test -p shakedown --features rabbitmq --test helpers_rabbit -- --ignored --test-threads=2 parallel_same_logical
//! ```

use serde::{Deserialize, Serialize};

use shakedown::ExchangeKind;

/// Domain event published to the logical exchange `events` with routing key `order.created`.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct OrderCreatedEvent {
    order_id: String,
    sku: String,
}

#[shakedown::test(resources = [Rabbit])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features rabbitmq --test helpers_rabbit -- --ignored"]
async fn rabbit_helpers_publish_and_await_fresh_exchange(ctx: &mut shakedown::Context) {
    // Given: a fresh exchange/queue where no consumer has declared the topology yet — publishing
    // before declaring would drop messages. publish_and_await declares topology, subscribes, then
    // publishes to guarantee delivery.

    let event = OrderCreatedEvent {
        order_id: "paa-ord-1".to_string(),
        sku: "SKU-PAA".to_string(),
    };
    let payload = serde_json::to_vec(&event).expect("serialize");

    // When: we call publish_and_await on a brand-new exchange and queue.
    let consumed = ctx
        .rabbit()
        .publish_and_await(
            "paa_events",
            shakedown::ExchangeKind::Direct,
            "paa_events",
            "order.paa",
            payload,
            None,
        )
        .await
        .expect("publish_and_await should not lose messages on a fresh exchange");

    // Then: the consumed message routing key and payload round-trip correctly.
    assert_eq!(consumed.key_str(), Some("order.paa"));
    let parsed: OrderCreatedEvent = consumed
        .payload_json()
        .expect("payload_json should deserialize the published body");
    assert_eq!(parsed, event);
}

#[shakedown::test(resources = [Rabbit])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features rabbitmq --test helpers_rabbit -- --ignored"]
async fn rabbit_helpers_parallel_same_logical_exchange_a(ctx: &mut shakedown::Context) {
    let payload = b"parallel-rabbit-a".to_vec();
    let consumed = ctx
        .rabbit()
        .publish_and_await(
            "shared_logical_ex",
            ExchangeKind::Direct,
            "shared_logical_q",
            "rk.shared",
            payload.clone(),
            None,
        )
        .await
        .expect("publish_and_await should isolate by scenario namespace");
    assert_eq!(consumed.payload.as_deref(), Some(payload.as_slice()));
}

#[shakedown::test(resources = [Rabbit])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features rabbitmq --test helpers_rabbit -- --ignored"]
async fn rabbit_helpers_parallel_same_logical_exchange_b(ctx: &mut shakedown::Context) {
    let payload = b"parallel-rabbit-b".to_vec();
    let consumed = ctx
        .rabbit()
        .publish_and_await(
            "shared_logical_ex",
            ExchangeKind::Direct,
            "shared_logical_q",
            "rk.shared",
            payload.clone(),
            None,
        )
        .await
        .expect("publish_and_await should isolate by scenario namespace");
    assert_eq!(consumed.payload.as_deref(), Some(payload.as_slice()));
}

#[shakedown::test(resources = [Rabbit])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features rabbitmq --test helpers_rabbit -- --ignored"]
async fn rabbit_helpers_topology_publish_json_and_consume_until(ctx: &mut shakedown::Context) {
    // Given: a scenario with the RabbitMQ resource provisioned (macro attribute) and isolated
    // exchange/queue names.

    let event = OrderCreatedEvent {
        order_id: "ord-42".to_string(),
        sku: "SKU-9".to_string(),
    };

    // When: we declare a direct exchange, a durable queue, and bind `order.created` in one call
    // (instead of hand-rolling exchange_declare + queue_declare + queue_bind with manual prefixes).
    ctx.rabbit()
        .declare_topology("events", ExchangeKind::Direct, "events", "order.created")
        .await
        .expect("declare_topology should succeed with provisioned RabbitMQ bindings");

    // And: we publish JSON to the logical exchange; routing key is sent as-is per helper contract.
    ctx.rabbit()
        .publish_json("events", "order.created", &event)
        .await
        .expect("publish_json should succeed after topology setup");

    // Then: `consume_until` on the logical queue name returns the message; routing key and body
    // round-trip through JSON.
    let consumed = ctx
        .rabbit()
        .consume_until(
            "events",
            |message| {
                message.key_str() == Some("order.created")
                    && message
                        .payload_json::<OrderCreatedEvent>()
                        .map(|parsed| parsed == event)
                        .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("consume_until should receive the published order event");

    assert_eq!(consumed.key_str(), Some("order.created"));
    let parsed: OrderCreatedEvent = consumed
        .payload_json()
        .expect("payload_json should deserialize the published JSON body");
    assert_eq!(parsed, event);
}
