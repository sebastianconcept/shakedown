//! Wraps the Kafka [`FutureProducer`](rdkafka::producer::FutureProducer) for order events.

use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

/// Publishes order-domain events to Kafka (non-outbox mode).
#[derive(Clone)]
pub struct OrderMessaging {
    producer: FutureProducer,
}

impl OrderMessaging {
    pub fn new(producer: FutureProducer) -> Self {
        Self { producer }
    }

    /// Fire-and-forget style produce with timeout; logs warnings on failure (matches prior behavior).
    pub async fn try_publish_order_json(&self, topic: &str, order_id: &str, payload: String) {
        let send_result = self
            .producer
            .send(
                FutureRecord::to(topic)
                    .key(order_id)
                    .payload(payload.as_str()),
                Duration::from_secs(5),
            )
            .await;
        if let Err((error, _)) = send_result {
            tracing::warn!("kafka produce failed: {error}");
        }
    }
}
