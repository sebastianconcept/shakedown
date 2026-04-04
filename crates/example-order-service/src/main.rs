//! Minimal order service for Shakedown examples — Postgres, Redis, Kafka, optional outbox.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    postgres_client: Arc<tokio_postgres::Client>,
    redis_connection: redis::aio::MultiplexedConnection,
    kafka_producer: rdkafka::producer::FutureProducer,
    fulfillment_base_url: Option<String>,
    outbox_enabled: bool,
}

#[derive(Deserialize)]
struct CreateOrderRequest {
    customer_id: String,
    items: Vec<OrderItem>,
}

#[derive(Deserialize, Serialize, Clone)]
struct OrderItem {
    product_id: String,
    quantity: u32,
}

#[derive(Serialize)]
struct CreateOrderResponse {
    id: String,
}

#[derive(Serialize)]
struct OrderDetailResponse {
    id: String,
    customer_id: String,
    items: Vec<OrderItem>,
    status: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL");
    let kafka_bootstrap =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").expect("KAFKA_BOOTSTRAP_SERVERS");
    let fulfillment_base_url = std::env::var("FULFILLMENT_URL").ok();
    let outbox_enabled = std::env::var("OUTBOX_ENABLED")
        .map(|value| value == "true" || value == "1")
        .unwrap_or(false);
    let listen_port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3000);

    let (postgres_client, postgres_connection) =
        tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
            .await
            .expect("postgres connect");
    tokio::spawn(async move {
        let _ = postgres_connection.await;
    });

    let redis_client = redis::Client::open(redis_url.as_str()).expect("redis client");
    let redis_connection = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection");

    use rdkafka::config::ClientConfig;
    let kafka_producer: rdkafka::producer::FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_bootstrap)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("kafka producer");

    let state = AppState {
        postgres_client: Arc::new(postgres_client),
        redis_connection,
        kafka_producer,
        fulfillment_base_url,
        outbox_enabled,
    };

    let application_router = Router::new()
        .route("/health", get(health_check))
        .route("/orders", post(create_order))
        .route("/orders/{id}", get(get_order_by_id))
        .route("/orders/{id}/fulfill", post(fulfill_order_via_downstream))
        .with_state(state);

    let socket_address = SocketAddr::from(([0, 0, 0, 0], listen_port));
    tracing::info!("order-service listening on {socket_address}");

    let listener = tokio::net::TcpListener::bind(socket_address)
        .await
        .expect("bind listen port");
    axum::serve(listener, application_router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("serve");
}

async fn health_check() -> StatusCode {
    StatusCode::OK
}

async fn create_order(
    State(state): State<AppState>,
    Json(payload): Json<CreateOrderRequest>,
) -> Result<(StatusCode, Json<CreateOrderResponse>), StatusCode> {
    let order_id = Uuid::new_v4().to_string();
    let items_json =
        serde_json::to_string(&payload.items).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    state
        .postgres_client
        .execute(
            "INSERT INTO orders (id, customer_id, items, status) VALUES ($1, $2, $3, $4)",
            &[&order_id, &payload.customer_id, &items_json, &"created"],
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut redis = state.redis_connection.clone();
    let cache_key = format!("order:{}:status", &order_id);
    let _: Result<(), redis::RedisError> = redis.set(&cache_key, "created").await;

    if state.outbox_enabled {
        let outbox_payload = serde_json::json!({
            "aggregate_id": &order_id,
            "customer_id": &payload.customer_id,
            "items": &payload.items,
        });
        state
            .postgres_client
            .execute(
                "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload) \
                 VALUES ($1, $2, $3, $4)",
                &[
                    &"order",
                    &order_id,
                    &"OrderCreated",
                    &outbox_payload.to_string(),
                ],
            )
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    } else {
        use rdkafka::producer::FutureRecord;
        let event = serde_json::json!({
            "aggregate_id": &order_id,
            "customer_id": &payload.customer_id,
            "items": &payload.items,
        });
        let send_result = state
            .kafka_producer
            .send(
                FutureRecord::to("orders")
                    .key(&order_id)
                    .payload(event.to_string().as_str()),
                std::time::Duration::from_secs(5),
            )
            .await;
        if let Err((error, _)) = send_result {
            tracing::warn!("kafka produce failed: {error}");
        }
    }

    Ok((
        StatusCode::CREATED,
        Json(CreateOrderResponse { id: order_id }),
    ))
}

async fn get_order_by_id(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> Result<Json<OrderDetailResponse>, StatusCode> {
    let mut redis = state.redis_connection.clone();
    let cache_key = format!("order:{}:status", &order_id);
    let cached_status: Option<String> = redis.get(&cache_key).await.ok().flatten();

    let row = state
        .postgres_client
        .query_opt(
            "SELECT id, customer_id, items, status FROM orders WHERE id = $1",
            &[&order_id],
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let status: String = cached_status.unwrap_or_else(|| row.get(3));
    let items_json: String = row.get(2);
    let items: Vec<OrderItem> = serde_json::from_str(&items_json).unwrap_or_default();

    Ok(Json(OrderDetailResponse {
        id: row.get(0),
        customer_id: row.get(1),
        items,
        status,
    }))
}

async fn fulfill_order_via_downstream(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let fulfillment_base_url = state
        .fulfillment_base_url
        .as_deref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let row = state
        .postgres_client
        .query_opt("SELECT items FROM orders WHERE id = $1", &[&order_id])
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let items_json: String = row.get(0);
    let http_client = reqwest::Client::new();
    let response = http_client
        .post(format!("{fulfillment_base_url}/fulfill"))
        .json(&serde_json::json!({
            "order_id": &order_id,
            "items": serde_json::from_str::<serde_json::Value>(&items_json).unwrap_or_default(),
        }))
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    if response.status().is_success() {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::BAD_GATEWAY)
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
}
