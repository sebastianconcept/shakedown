//! HTTP handlers for the order API.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use shared_grpc::{FulfillRequest as ProtoFulfillRequest, OrderItem as ProtoOrderItem};

use crate::http::api_error::OrderApiError;
use crate::kafka_topic::KAFKA_TOPIC_ORDERS;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct CreateOrderRequest {
    pub customer_id: String,
    pub items: Vec<OrderItem>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct OrderItem {
    pub product_id: String,
    pub quantity: u32,
}

#[derive(Serialize)]
pub struct CreateOrderResponse {
    pub id: String,
}

#[derive(Serialize)]
pub struct OrderDetailResponse {
    pub id: String,
    pub customer_id: String,
    pub items: Vec<OrderItem>,
    pub status: String,
}

pub async fn health_check() -> StatusCode {
    StatusCode::OK
}

pub async fn create_order(
    State(state): State<AppState>,
    Json(payload): Json<CreateOrderRequest>,
) -> Result<(StatusCode, Json<CreateOrderResponse>), OrderApiError> {
    let order_id = Uuid::new_v4().to_string();
    let items_json = serde_json::to_string(&payload.items).map_err(|error| {
        tracing::error!(?error, "serialize order items");
        OrderApiError::Internal
    })?;

    state
        .orders
        .insert_order(&order_id, &payload.customer_id, &items_json)
        .await
        .map_err(|error| {
            tracing::error!(?error, "insert order");
            OrderApiError::Internal
        })?;

    let _ = state.cache.set_status(&order_id, "created").await;

    if state.outbox_enabled {
        let outbox_payload = serde_json::json!({
            "aggregate_id": &order_id,
            "customer_id": &payload.customer_id,
            "items": &payload.items,
        });
        state
            .orders
            .insert_outbox_order_created(&order_id, &outbox_payload.to_string())
            .await
            .map_err(|error| {
                tracing::error!(?error, "insert outbox");
                OrderApiError::Internal
            })?;
    } else {
        let event = serde_json::json!({
            "aggregate_id": &order_id,
            "customer_id": &payload.customer_id,
            "items": &payload.items,
        });
        state
            .messaging
            .try_publish_order_json(KAFKA_TOPIC_ORDERS, &order_id, event.to_string())
            .await;
    }

    Ok((
        StatusCode::CREATED,
        Json(CreateOrderResponse { id: order_id }),
    ))
}

pub async fn get_order_by_id(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> Result<Json<OrderDetailResponse>, OrderApiError> {
    let cached_status = state.cache.get_status(&order_id).await.ok().flatten();

    let row = state
        .orders
        .select_order_for_get(&order_id)
        .await
        .map_err(|error| {
            tracing::error!(?error, "select order");
            OrderApiError::Internal
        })?
        .ok_or(OrderApiError::NotFound)?;

    let status: String = cached_status.unwrap_or_else(|| row.get(3));
    let items_json: String = row.get(2);
    let items: Vec<OrderItem> = serde_json::from_str(&items_json).map_err(|error| {
        tracing::error!(?error, "deserialize items");
        OrderApiError::Internal
    })?;

    Ok(Json(OrderDetailResponse {
        id: row.get(0),
        customer_id: row.get(1),
        items,
        status,
    }))
}

pub async fn fulfill_order_via_downstream(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> Result<StatusCode, OrderApiError> {
    let Some(client) = state.fulfillment_grpc.as_ref() else {
        return Err(OrderApiError::ServiceUnavailable);
    };

    let items_json = state
        .orders
        .select_items_json(&order_id)
        .await
        .map_err(|error| {
            tracing::error!(?error, "select items for fulfill");
            OrderApiError::Internal
        })?
        .ok_or(OrderApiError::NotFound)?;

    let items_value: serde_json::Value = serde_json::from_str(&items_json).map_err(|error| {
        tracing::error!(?error, "deserialize items for fulfill");
        OrderApiError::Internal
    })?;
    let items = match items_value {
        serde_json::Value::Array(entries) => entries
            .into_iter()
            .filter_map(|entry| {
                let product_id = entry.get("product_id")?.as_str()?.to_string();
                let quantity = entry.get("quantity")?.as_u64()? as u32;
                Some(ProtoOrderItem {
                    product_id,
                    quantity,
                })
            })
            .collect::<Vec<_>>(),
        _ => {
            tracing::error!("order items for fulfill are not an array");
            return Err(OrderApiError::Internal);
        }
    };

    let request = ProtoFulfillRequest { order_id, items };
    let mut guard = client.lock().await;
    let response = guard
        .fulfill(tonic::Request::new(request))
        .await
        .map_err(|error| {
            tracing::error!(?error, "fulfillment gRPC");
            OrderApiError::BadGateway
        })?;

    let _inner = response.into_inner();
    Ok(StatusCode::OK)
}
