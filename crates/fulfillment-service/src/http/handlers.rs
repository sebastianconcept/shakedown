//! HTTP handlers for the fulfillment API.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;

use crate::fulfillment::{create_fulfillment, CreateFulfillmentOutcome, FulfillRequest};
use crate::http::api_error::FulfillmentApiError;
use crate::state::AppState;

#[derive(Serialize)]
pub struct FulfillResponse {
    pub id: String,
}

pub async fn health_check() -> StatusCode {
    StatusCode::OK
}

pub async fn create_fulfillment_http(
    State(state): State<AppState>,
    Json(payload): Json<FulfillRequest>,
) -> Result<(StatusCode, Json<FulfillResponse>), FulfillmentApiError> {
    let outcome = create_fulfillment(&state, payload).await?;
    let (status, fulfillment_id) = match outcome {
        CreateFulfillmentOutcome::Created { fulfillment_id } => {
            (StatusCode::CREATED, fulfillment_id)
        }
        CreateFulfillmentOutcome::AlreadyExisted { fulfillment_id } => {
            (StatusCode::OK, fulfillment_id)
        }
    };
    Ok((status, Json(FulfillResponse { id: fulfillment_id })))
}

pub async fn get_fulfillment_by_id(
    State(state): State<AppState>,
    Path(fulfillment_id): Path<String>,
) -> Result<Json<serde_json::Value>, FulfillmentApiError> {
    let document = state
        .fulfillments
        .find_document_by_id(&fulfillment_id)
        .await
        .map_err(|error| {
            tracing::error!(?error, "mongo find fulfillment");
            FulfillmentApiError::Internal
        })?
        .ok_or(FulfillmentApiError::NotFound)?;

    let json_value: serde_json::Value =
        mongodb::bson::from_document(document).map_err(|error| {
            tracing::error!(?error, "bson to json");
            FulfillmentApiError::Internal
        })?;
    Ok(Json(json_value))
}
