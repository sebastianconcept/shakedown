//! Kafka topic names aligned with the book’s Order → Kafka → Fulfillment chain.
//!
//! The book outline sometimes says `order.created`; this codebase uses topic **`orders`**
//! with JSON payloads containing `aggregate_id` and `items` — see chapter tests that assert
//! `consume_until` on this topic.

/// Topic for `OrderCreated`-style events (producer: order service; consumer: fulfillment).
pub const KAFKA_TOPIC_ORDERS: &str = "orders";
