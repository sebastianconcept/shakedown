//! Binary entrypoint: tracing init, then [`fulfillment_service::run`].

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    if let Err(error) = fulfillment_service::run().await {
        tracing::error!("fulfillment service failed: {error}");
        std::process::exit(1);
    }
}
