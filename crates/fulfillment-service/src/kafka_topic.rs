//! Kafka topic for order events (must match the order service producer and book chapter tests).

/// Same topic string as `order_service::kafka_topic::KAFKA_TOPIC_ORDERS`.
pub const KAFKA_TOPIC_ORDERS: &str = "orders";
