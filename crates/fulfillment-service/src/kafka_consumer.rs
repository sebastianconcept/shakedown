//! Background Kafka consumer: `orders` topic → fulfillment workflow.
//!
//! **Supervision:** failures in this task are logged; they do **not** crash the HTTP server.
//! That matches typical “sidecar consumer” deployments where the API stays up while the
//! consumer retries or awaits broker recovery.

use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;

use crate::fulfillment::{create_fulfillment, CreateFulfillmentOutcome, FulfillRequest};
use crate::kafka_topic::KAFKA_TOPIC_ORDERS;
use crate::state::AppState;

/// Subscribe to [`KAFKA_TOPIC_ORDERS`] and create fulfillments for each event (runs until the stream ends).
pub async fn kafka_orders_consumer_loop(bootstrap: String, state: AppState) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "fulfillment-service")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .create()
    {
        Ok(consumer) => consumer,
        Err(error) => {
            tracing::error!(?error, "kafka consumer create failed");
            return;
        }
    };

    if let Err(error) = consumer.subscribe(&[KAFKA_TOPIC_ORDERS]) {
        tracing::error!(?error, topic = KAFKA_TOPIC_ORDERS, "kafka subscribe failed");
        return;
    }

    tracing::info!(topic = KAFKA_TOPIC_ORDERS, "kafka consumer subscribed");
    let mut message_stream = consumer.stream();

    while let Some(message_result) = message_stream.next().await {
        match message_result {
            Ok(message) => {
                let Some(payload_slice) = message.payload() else {
                    continue;
                };
                process_orders_topic_payload(&state, payload_slice).await;
            }
            Err(error) => tracing::warn!(?error, "kafka consume error"),
        }
    }
}

/// Parse one `orders` topic payload and run the shared fulfillment path (idempotent).
async fn process_orders_topic_payload(state: &AppState, payload: &[u8]) {
    let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return;
    };
    let order_id = event["aggregate_id"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();

    let items_vec = match event.get("items") {
        Some(serde_json::Value::Array(entries)) => entries.clone(),
        Some(other) => {
            tracing::warn!(
                ?other,
                "kafka order event items is not an array for {order_id}"
            );
            vec![]
        }
        None => {
            tracing::warn!("kafka order event missing items for {order_id}");
            vec![]
        }
    };

    let fulfill_request = FulfillRequest {
        order_id: order_id.clone(),
        items: items_vec,
    };

    let kafka_state = state.clone();
    let span = tracing::info_span!("kafka_order_message", order_id = %order_id);
    let _guard = span.enter();

    match create_fulfillment(&kafka_state, fulfill_request).await {
        Ok(CreateFulfillmentOutcome::Created { fulfillment_id }) => {
            tracing::info!(
                fulfillment_id = %fulfillment_id,
                "kafka path created fulfillment"
            );
        }
        Ok(CreateFulfillmentOutcome::AlreadyExisted { fulfillment_id }) => {
            tracing::info!(
                fulfillment_id = %fulfillment_id,
                "kafka path skipped duplicate (idempotent)"
            );
        }
        Err(error) => {
            tracing::warn!(?error, "kafka path fulfillment failed");
        }
    }
}
