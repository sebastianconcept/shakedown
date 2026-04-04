//! HTTP-facing errors for fulfillment routes.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// API-layer failures for Axum handlers.
#[derive(Debug)]
pub enum FulfillmentApiError {
    Internal,
    NotFound,
}

impl IntoResponse for FulfillmentApiError {
    fn into_response(self) -> Response {
        let status = match self {
            FulfillmentApiError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            FulfillmentApiError::NotFound => StatusCode::NOT_FOUND,
        };
        status.into_response()
    }
}
