//! Chapter 5: RabbitMQ topology, publish, and consume
//!
//! **Resources:** RabbitMQ (Docker)
//! **Assertions:** `declare_topology`, `publish_json`, `consume_until`
//! **Concepts:** exchange + queue + binding in one call, routing keys, JSON payloads
//! **Teaching intent:** Stand up a minimal `orders` messaging path the way outbox verifiers expect —
//!   topology first, then publish, then assert with `consume_until`.
//!
//! Run with Docker: `cargo test -p shakedown-examples --features rabbitmq --test ch05_messaging_rabbit -- --ignored`

use serde::{Deserialize, Serialize};
use shakedown::ExchangeKind;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct OrderPaymentCaptured {
    order_id: String,
    payment_reference: String,
    amount_cents: u32,
}

#[shakedown::test(resources = [Rabbit])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features rabbitmq --test ch05_messaging_rabbit -- --ignored"]
async fn rabbitmq_declare_publish_and_consume_until(ctx: &mut shakedown::Context) {
    ctx.rabbit()
        .declare_topology(
            "orders",
            ExchangeKind::Direct,
            "orders_notifications",
            "order.payment_captured",
        )
        .await
        .expect("declare exchange, queue, and binding");

    let event = OrderPaymentCaptured {
        order_id: "order-ch05-1".to_string(),
        payment_reference: "pay-ref-99".to_string(),
        amount_cents: 4_200,
    };

    ctx.rabbit()
        .publish_json("orders", "order.payment_captured", &event)
        .await
        .expect("publish_json to logical exchange");

    let consumed = ctx
        .rabbit()
        .consume_until(
            "orders_notifications",
            |message| {
                message.key_str() == Some("order.payment_captured")
                    && message
                        .payload_json::<OrderPaymentCaptured>()
                        .map(|parsed| parsed == event)
                        .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("consume_until should observe the published capture event");

    assert_eq!(consumed.key_str(), Some("order.payment_captured"));
}
