//! Multi-broker KRaft Kafka cluster: provision, metadata, helpers produce/consume (`kafka-cluster`).
//!
//! **`#[ignore]`** — run with Docker:
//!
//! ```text
//! cargo test -p shakedown --features kafka-cluster --test kafka_cluster_vertical -- --ignored
//! ```

use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::types::RDKafkaErrorCode;
use serde::{Deserialize, Serialize};
use serial_test::serial;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct PingEvent {
    correlation_id: String,
}

#[shakedown::test(resources = [KafkaCluster])]
#[serial]
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

/// US-011: stop the broker leading a partition, wait for a new leader via metadata, then produce/consume.
#[shakedown::test(resources = [KafkaCluster])]
#[serial]
#[ignore = "requires Docker; run: cargo test -p shakedown --features kafka-cluster --test kafka_cluster_vertical -- --ignored"]
async fn kafka_cluster_leader_election_after_broker_stop(ctx: &mut shakedown::Context) {
    let bindings = ctx
        .kafka_bindings()
        .expect("Kafka bindings after KafkaCluster provision");
    let bootstrap = bindings.bootstrap_servers.as_str();
    let full_topic = bindings.topic_namespace("leader_election_vertical");

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("client.id", "shakedown_kafka_cluster_leader_test")
        .create()
        .expect("admin client");

    let new_topics = [NewTopic::new(
        full_topic.as_str(),
        1,
        TopicReplication::Fixed(3),
    )];
    let results = admin
        .create_topics(&new_topics, &AdminOptions::new())
        .await
        .expect("create_topics");
    for result in results {
        if let Err((_, code)) = result {
            assert_eq!(code, RDKafkaErrorCode::TopicAlreadyExists);
        }
    }

    let cluster = ctx.kafka_cluster().expect("Kafka cluster control");
    let leader_idx = cluster
        .leader_for_partition(full_topic.as_str(), 0)
        .await
        .expect("leader_for_partition");

    let generic = ctx
        .cluster_handle::<shakedown::KafkaClusterMarker>()
        .expect("cluster handle");
    generic
        .stop_node(leader_idx)
        .await
        .expect("stop_node leading broker");

    let new_leader = cluster
        .await_leader_election(full_topic.as_str(), 0, Duration::from_secs(90))
        .await
        .expect("await_leader_election");
    assert_ne!(
        new_leader, leader_idx,
        "expected partition leader to move to a different node index"
    );

    let payload = PingEvent {
        correlation_id: "after-election".to_string(),
    };

    ctx.kafka()
        .produce_json("leader_election_vertical", "k1", &payload)
        .await
        .expect("produce_json after leader election");

    let got = ctx
        .kafka()
        .consume_one("leader_election_vertical", None)
        .await
        .expect("consume_one");
    let parsed: PingEvent = got.payload_json().expect("json");
    assert_eq!(parsed, payload);
}
