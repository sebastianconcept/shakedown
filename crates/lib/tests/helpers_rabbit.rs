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
