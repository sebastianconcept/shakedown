//! Binary entrypoint: tracing init, then [`order_service::run`].

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    if let Err(error) = order_service::run().await {
        tracing::error!("order service failed: {error}");
        std::process::exit(1);
    }
}
