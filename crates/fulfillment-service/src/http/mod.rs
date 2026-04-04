//! HTTP server: routes and graceful shutdown.

mod api_error;
mod handlers;

use std::net::SocketAddr;

use axum::routing::{get, post};
use axum::Router;

pub use api_error::FulfillmentApiError;

use crate::error::ServiceError;
use crate::state::AppState;

use handlers::{create_fulfillment_http, get_fulfillment_by_id, health_check};

/// Build the Axum router with all fulfillment-service routes.
pub fn router(application_state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/fulfill", post(create_fulfillment_http))
        .route("/fulfill/{id}", get(get_fulfillment_by_id))
        .with_state(application_state)
}

/// Bind, log, and serve until graceful shutdown (Ctrl+C).
pub async fn serve(application_router: Router, listen_port: u16) -> Result<(), ServiceError> {
    let socket_address = SocketAddr::from(([0, 0, 0, 0], listen_port));
    tracing::info!("fulfillment-service listening on {socket_address}");
    let listener = tokio::net::TcpListener::bind(socket_address)
        .await
        .map_err(ServiceError::Serve)?;

    axum::serve(listener, application_router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(ServiceError::Serve)?;
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
}
