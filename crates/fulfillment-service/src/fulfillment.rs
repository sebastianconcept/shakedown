//! Fulfillment workflow: idempotent create, optional payment mock, RabbitMQ publish.
//!
//! Uses [`idempotency::IdempotentDecision`] so **Kafka**, **gRPC**, and **HTTP** share the same
//! rule: at most one fulfillment per `order_id`.

use idempotency::IdempotentDecision;
use lapin::options::{BasicPublishOptions, ExchangeDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, ExchangeKind};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::http::FulfillmentApiError;
use crate::state::AppState;

#[derive(Deserialize, Serialize, Clone)]
pub struct FulfillRequest {
    pub order_id: String,
    pub items: Vec<serde_json::Value>,
}

/// Result of attempting to create a fulfillment (new insert vs idempotent replay).
#[derive(Debug, Clone)]
pub enum CreateFulfillmentOutcome {
    Created { fulfillment_id: String },
    AlreadyExisted { fulfillment_id: String },
}

/// Create a fulfillment record and publish — shared by HTTP and Kafka ingestion.
pub async fn create_fulfillment(
    state: &AppState,
    payload: FulfillRequest,
) -> Result<CreateFulfillmentOutcome, FulfillmentApiError> {
    let existing_id = state
        .fulfillments
        .find_fulfillment_id_by_order_id(&payload.order_id)
        .await
        .map_err(|error| {
            tracing::error!(?error, order_id = %payload.order_id, "mongo find by order_id");
            FulfillmentApiError::Internal
        })?;

    let decision: IdempotentDecision<String> = match existing_id {
        Some(id) => IdempotentDecision::AlreadyHandled { existing: id },
        None => IdempotentDecision::Proceed,
    };

    if let IdempotentDecision::AlreadyHandled { existing } = decision {
        tracing::info!(
            order_id = %payload.order_id,
            fulfillment_id = %existing,
            "skip duplicate fulfillment (idempotent replay)"
        );
        return Ok(CreateFulfillmentOutcome::AlreadyExisted {
            fulfillment_id: existing,
        });
    }

    let fulfillment_id = Uuid::new_v4().to_string();

    if let Some(payment_url) = &state.payment_gateway_base_url {
        let http_client = reqwest::Client::new();
        let _ = http_client
            .post(format!("{payment_url}/charge"))
            .json(&serde_json::json!({
                "order_id": &payload.order_id,
                "amount": 99.99,
            }))
            .send()
            .await;
    }

    let items_bson = mongodb::bson::to_bson(&payload.items).map_err(|error| {
        tracing::error!(?error, "items to bson");
        FulfillmentApiError::Internal
    })?;

    state
        .fulfillments
        .insert_fulfillment(&fulfillment_id, &payload.order_id, items_bson)
        .await
        .map_err(|error| {
            tracing::error!(?error, order_id = %payload.order_id, "mongo insert fulfillment");
            FulfillmentApiError::Internal
        })?;

    let channel: lapin::Channel =
        state
            .rabbit_connection
            .create_channel()
            .await
            .map_err(|error| {
                tracing::error!(?error, "rabbit create channel");
                FulfillmentApiError::Internal
            })?;
    channel
        .exchange_declare(
            "fulfillments".into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|error| {
            tracing::error!(?error, "rabbit declare exchange");
            FulfillmentApiError::Internal
        })?;

    let event = serde_json::json!({
        "aggregate_id": &fulfillment_id,
        "order_id": &payload.order_id,
    });
    let _ = channel
        .basic_publish(
            "fulfillments".into(),
            fulfillment_id.as_str().into(),
            BasicPublishOptions::default(),
            event.to_string().as_bytes(),
            BasicProperties::default(),
        )
        .await;

    Ok(CreateFulfillmentOutcome::Created { fulfillment_id })
}
