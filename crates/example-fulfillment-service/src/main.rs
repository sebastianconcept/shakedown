//! Minimal fulfillment service for Shakedown examples — MongoDB, RabbitMQ, Kafka consumer.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    mongo_database: mongodb::Database,
    rabbit_connection: Arc<lapin::Connection>,
    payment_gateway_base_url: Option<String>,
}

#[derive(Deserialize, Serialize, Clone)]
struct FulfillRequest {
    order_id: String,
    items: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct FulfillResponse {
    id: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let mongodb_uri = std::env::var("MONGODB_URI").expect("MONGODB_URI");
    let kafka_bootstrap =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").expect("KAFKA_BOOTSTRAP_SERVERS");
    let rabbitmq_url = std::env::var("RABBITMQ_URL").expect("RABBITMQ_URL");
    let payment_gateway_base_url = std::env::var("PAYMENT_GATEWAY_URL").ok();
    let listen_port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3001);

    let mongo_client = mongodb::Client::with_uri_str(&mongodb_uri)
        .await
        .expect("mongo client");
    let mongo_database = mongo_client.database("fulfillment");

    let rabbit_connection = Arc::new(
        lapin::Connection::connect(&rabbitmq_url, lapin::ConnectionProperties::default())
            .await
            .expect("rabbit connection"),
    );

    let state = AppState {
        mongo_database: mongo_database.clone(),
        rabbit_connection: rabbit_connection.clone(),
        payment_gateway_base_url: payment_gateway_base_url.clone(),
    };

    let kafka_state = state.clone();
    tokio::spawn(async move {
        kafka_orders_consumer_loop(kafka_bootstrap, kafka_state).await;
    });

    let application_router = Router::new()
        .route("/health", get(health_check))
        .route("/fulfill", post(create_fulfillment))
        .route("/fulfill/{id}", get(get_fulfillment_by_id))
        .with_state(state);

    let socket_address = SocketAddr::from(([0, 0, 0, 0], listen_port));
    tracing::info!("fulfillment-service listening on {socket_address}");

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

async fn create_fulfillment(
    State(state): State<AppState>,
    Json(payload): Json<FulfillRequest>,
) -> Result<(StatusCode, Json<FulfillResponse>), StatusCode> {
    let fulfillment_id = create_fulfillment_internal(&state, payload).await?;
    Ok((
        StatusCode::CREATED,
        Json(FulfillResponse { id: fulfillment_id }),
    ))
}

async fn get_fulfillment_by_id(
    State(state): State<AppState>,
    Path(fulfillment_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let collection = state
        .mongo_database
        .collection::<mongodb::bson::Document>("fulfillments");
    let document = collection
        .find_one(doc! { "_id": &fulfillment_id })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let json_value: serde_json::Value =
        mongodb::bson::from_document(document).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json_value))
}

async fn create_fulfillment_internal(
    state: &AppState,
    payload: FulfillRequest,
) -> Result<String, StatusCode> {
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

    let collection = state
        .mongo_database
        .collection::<mongodb::bson::Document>("fulfillments");
    let insert_document = doc! {
        "_id": &fulfillment_id,
        "order_id": &payload.order_id,
        "items": mongodb::bson::to_bson(&payload.items).unwrap_or_default(),
        "status": "fulfilled",
    };
    collection
        .insert_one(insert_document)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let channel = state
        .rabbit_connection
        .create_channel()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    channel
        .exchange_declare(
            "fulfillments".into(),
            lapin::ExchangeKind::Direct,
            lapin::options::ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let event = serde_json::json!({
        "aggregate_id": &fulfillment_id,
        "order_id": &payload.order_id,
    });
    let _ = channel
        .basic_publish(
            "fulfillments".into(),
            fulfillment_id.as_str().into(),
            lapin::options::BasicPublishOptions::default(),
            event.to_string().as_bytes(),
            lapin::BasicProperties::default(),
        )
        .await;

    Ok(fulfillment_id)
}

async fn kafka_orders_consumer_loop(bootstrap: String, state: AppState) {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::message::Message;

    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "fulfillment-service")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .create()
    {
        Ok(consumer) => consumer,
        Err(error) => {
            tracing::error!("kafka consumer create failed: {error}");
            return;
        }
    };

    if let Err(error) = consumer.subscribe(&["orders"]) {
        tracing::error!("kafka subscribe failed: {error}");
        return;
    }

    tracing::info!("kafka consumer subscribed to orders");
    let mut message_stream = consumer.stream();

    while let Some(message_result) = message_stream.next().await {
        match message_result {
            Ok(message) => {
                let Some(payload_slice) = message.payload() else {
                    continue;
                };
                let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload_slice) else {
                    continue;
                };
                let order_id = event["aggregate_id"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_string();
                let items = event.get("items").cloned().unwrap_or_default();
                let items_vec = if items.is_array() {
                    items.as_array().cloned().unwrap_or_default()
                } else {
                    vec![]
                };

                tracing::info!("kafka consumed order event for {order_id}");

                let fulfill_request = FulfillRequest {
                    order_id,
                    items: items_vec,
                };

                let kafka_state = state.clone();
                let _ = create_fulfillment_internal(&kafka_state, fulfill_request).await;
            }
            Err(error) => tracing::warn!("kafka consume error: {error}"),
        }
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
}
