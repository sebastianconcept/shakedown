//! Multi-broker KRaft Kafka cluster: provision, metadata, helpers produce/consume (`kafka-cluster`).
//!
//! **`#[ignore]`** — run with Docker:
//!
//! ```text
//! cargo test -p shakedown --features kafka-cluster --test kafka_cluster_vertical -- --ignored
//! ```

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct PingEvent {
    correlation_id: String,
}

#[shakedown::test(resources = [KafkaCluster])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features kafka-cluster --test kafka_cluster_vertical -- --ignored"]
async fn kafka_cluster_three_brokers_produce_consume(ctx: &mut shakedown::Context) {
    let bindings = ctx
        .kafka_bindings()
        .expect("Kafka bindings after KafkaCluster provision");
    let bootstrap = bindings.bootstrap_servers.as_str();
    assert_eq!(
        bootstrap.matches(',').count(),
        2,
        "expected 3 bootstrap hosts in comma-separated list"
    );

    let payload = PingEvent {
        correlation_id: "cluster-proof".to_string(),
    };

    ctx.kafka()
        .produce_json("cluster_vertical", "k1", &payload)
        .await
        .expect("produce_json against multi-broker bootstrap");

    let got = ctx
        .kafka()
        .consume_one("cluster_vertical", None)
        .await
        .expect("consume_one");
    let parsed: PingEvent = got.payload_json().expect("json");
    assert_eq!(parsed, payload);
}
