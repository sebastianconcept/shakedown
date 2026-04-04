//! HTTP server: routes and graceful shutdown.

mod api_error;
mod handlers;

use std::net::SocketAddr;

use axum::routing::{get, post};
use axum::Router;

pub use api_error::OrderApiError;

use crate::error::ServiceError;
use crate::state::AppState;

use handlers::{create_order, fulfill_order_via_downstream, get_order_by_id, health_check};

/// Build the Axum router with all order-service routes.
pub fn router(application_state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/orders", post(create_order))
        .route("/orders/{id}", get(get_order_by_id))
        .route("/orders/{id}/fulfill", post(fulfill_order_via_downstream))
        .with_state(application_state)
}

/// Bind, log, and serve until graceful shutdown (Ctrl+C).
pub async fn serve(application_router: Router, listen_port: u16) -> Result<(), ServiceError> {
    let socket_address = SocketAddr::from(([0, 0, 0, 0], listen_port));
    tracing::info!("order-service listening on {socket_address}");
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
