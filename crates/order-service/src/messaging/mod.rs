//! Kafka produce path when outbox is disabled (direct publish after `POST /orders`).

mod order_messaging;

pub use order_messaging::OrderMessaging;
