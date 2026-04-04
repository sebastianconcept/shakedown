//! Shared [`AppState`]: repositories, cache, and messaging clients.

use std::sync::Arc;

use tokio::sync::Mutex;

use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;

use shared_grpc::FulfillmentClient;
use tonic::transport::Channel;

use crate::cache::OrderCache;
use crate::config::OrderServiceConfig;
use crate::error::StartupError;
use crate::messaging::OrderMessaging;
use crate::repository::OrderRepository;

/// Axum [`State`](axum::extract::State) for HTTP handlers.
#[derive(Clone)]
pub struct AppState {
    pub orders: OrderRepository,
    pub cache: OrderCache,
    pub messaging: OrderMessaging,
    /// Unary client for Fulfillment sync path when [`OrderServiceConfig::fulfillment_grpc_uri`] is set.
    pub fulfillment_grpc: Option<Arc<Mutex<FulfillmentClient<Channel>>>>,
    pub outbox_enabled: bool,
}

impl AppState {
    /// Connect to Postgres, Redis, and Kafka using the given configuration.
    pub async fn connect(configuration: OrderServiceConfig) -> Result<Self, StartupError> {
        let (postgres_client, postgres_connection) =
            tokio_postgres::connect(&configuration.database_url, tokio_postgres::NoTls)
                .await
                .map_err(StartupError::Postgres)?;
        tokio::spawn(async move {
            let _ = postgres_connection.await;
        });

        let redis_client =
            redis::Client::open(configuration.redis_url.as_str()).map_err(StartupError::Redis)?;
        let redis_connection = redis_client
            .get_multiplexed_async_connection()
            .await
            .map_err(StartupError::Redis)?;

        let kafka_producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &configuration.kafka_bootstrap)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(StartupError::KafkaProducer)?;

        let postgres_client = Arc::new(postgres_client);

        let fulfillment_grpc = match configuration.fulfillment_grpc_uri.as_deref() {
            Some(uri) => {
                let channel = tonic::transport::Endpoint::new(uri.to_owned())
                    .map_err(StartupError::FulfillmentGrpc)?
                    .connect()
                    .await
                    .map_err(StartupError::FulfillmentGrpc)?;
                Some(Arc::new(Mutex::new(FulfillmentClient::new(channel))))
            }
            None => None,
        };

        Ok(Self {
            orders: OrderRepository::new(Arc::clone(&postgres_client)),
            cache: OrderCache::new(redis_connection),
            messaging: OrderMessaging::new(kafka_producer),
            fulfillment_grpc,
            outbox_enabled: configuration.outbox_enabled,
        })
    }
}
