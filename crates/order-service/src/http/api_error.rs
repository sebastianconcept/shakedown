//! HTTP-facing errors mapped to status codes (separate from [`crate::error::ServiceError`](crate::error::ServiceError) for `run`).

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// API-layer failures for Axum handlers.
#[derive(Debug)]
pub enum OrderApiError {
    Internal,
    NotFound,
    BadGateway,
    ServiceUnavailable,
}

impl IntoResponse for OrderApiError {
    fn into_response(self) -> Response {
        let status = match self {
            OrderApiError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            OrderApiError::NotFound => StatusCode::NOT_FOUND,
            OrderApiError::BadGateway => StatusCode::BAD_GATEWAY,
            OrderApiError::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        };
        status.into_response()
    }
}
