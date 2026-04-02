//! RabbitMQ 3-node Erlang cluster: provision, multi-host URI, helpers, quorum queue (`rabbitmq-cluster`).
//!
//! **`#[ignore]`** — run with Docker:
//!
//! ```text
//! cargo test -p shakedown --features rabbitmq-cluster --test rabbit_cluster_vertical -- --ignored
//! ```

use std::time::Duration;

use lapin::options::QueueDeclareOptions;
use lapin::types::{AMQPValue, FieldTable, LongString};
use lapin::{Connection, ConnectionProperties};
use serde::{Deserialize, Serialize};
use serial_test::serial;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ClusterPing {
    proof: String,
}

#[shakedown::test(resources = [RabbitCluster])]
#[serial]
#[ignore = "requires Docker; run: cargo test -p shakedown --features rabbitmq-cluster --test rabbit_cluster_vertical -- --ignored"]
async fn rabbit_cluster_three_nodes_helpers_and_quorum_queue(ctx: &mut shakedown::Context) {
    let bindings = ctx
        .rabbit_bindings()
        .expect("RabbitMQ bindings after RabbitCluster provision");

    assert!(
        bindings.connection_url.contains("127.0.0.1")
            && bindings.connection_url.matches(',').count() >= 2,
        "expected multi-host AMQP URI in connection_url, got {}",
        bindings.connection_url
    );
    assert!(
        !bindings.amqp_uri.contains(','),
        "expected single-broker amqp_uri for lapin helpers, got {}",
        bindings.amqp_uri
    );

    ctx.rabbit()
        .declare_topology(
            "cluster_ex",
            shakedown::ExchangeKind::Direct,
            "cluster_q",
            "rk",
        )
        .await
        .expect("declare_topology against cluster");

    let payload = ClusterPing {
        proof: "cluster-vertical".to_string(),
    };
    ctx.rabbit()
        .publish_json("cluster_ex", "rk", &payload)
        .await
        .expect("publish_json");

    let consumed = ctx
        .rabbit()
        .consume_until(
            "cluster_q",
            |message| {
                message
                    .payload_json::<ClusterPing>()
                    .map(|parsed| parsed == payload)
                    .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("consume_until");

    let parsed: ClusterPing = consumed.payload_json().expect("json");
    assert_eq!(parsed, payload);

    // Explicit quorum queue declaration (server default queue type is quorum via conf drop-in).
    let connection =
        Connection::connect(bindings.amqp_uri.as_str(), ConnectionProperties::default())
            .await
            .expect("lapin connect for quorum queue declare");
    let channel = connection.create_channel().await.expect("channel");
    let mut arguments = FieldTable::default();
    arguments.insert(
        "x-queue-type".into(),
        AMQPValue::LongString(LongString::from("quorum")),
    );
    let quorum_queue = bindings.namespaced_name("quorum_proof");
    channel
        .queue_declare(
            quorum_queue.as_str().into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .expect("declare quorum queue");
    let _ = quorum_queue;
    let _ = connection.close(200, "test_done".into()).await;
}

/// Quorum queue Raft leader moves after stopping the leader broker; publish/consume still succeeds.
#[shakedown::test(resources = [RabbitCluster])]
#[serial]
#[ignore = "requires Docker; run: cargo test -p shakedown --features rabbitmq-cluster --test rabbit_cluster_vertical -- --ignored"]
async fn rabbit_cluster_quorum_queue_leader_election_after_stop(ctx: &mut shakedown::Context) {
    let bindings = ctx
        .rabbit_bindings()
        .expect("RabbitMQ bindings after RabbitCluster provision");

    let queue_name = bindings.namespaced_name("quorum_leader_ops");

    let connection =
        Connection::connect(bindings.amqp_uri.as_str(), ConnectionProperties::default())
            .await
            .expect("lapin connect for quorum queue declare");
    let channel = connection.create_channel().await.expect("channel");
    let mut arguments = FieldTable::default();
    arguments.insert(
        "x-queue-type".into(),
        AMQPValue::LongString(LongString::from("quorum")),
    );
    channel
        .queue_declare(
            queue_name.as_str().into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .expect("declare quorum queue");
    let _ = connection.close(200, "prep_done".into()).await;

    let cluster = ctx
        .rabbit_cluster()
        .expect("RabbitClusterClusterControl registered for RabbitCluster resource");

    let leader_before = cluster
        .quorum_queue_leader(queue_name.as_str())
        .await
        .expect("quorum_queue_leader");

    let generic = ctx
        .cluster_handle::<shakedown::RabbitClusterMarker>()
        .expect("cluster handle");
    generic
        .stop_node(leader_before)
        .await
        .expect("stop_node on quorum leader");

    let new_leader = cluster
        .await_queue_leader_election(queue_name.as_str(), Duration::from_secs(90))
        .await
        .expect("await_queue_leader_election");

    assert_ne!(
        new_leader, leader_before,
        "expected a different node index as quorum leader after stopping the prior leader"
    );

    ctx.rabbit()
        .declare_topology(
            "leader_election_ex",
            shakedown::ExchangeKind::Direct,
            "leader_election_q",
            "leader_rk",
        )
        .await
        .expect("declare_topology after election");

    let payload = ClusterPing {
        proof: "after-leader-election".to_string(),
    };
    ctx.rabbit()
        .publish_json("leader_election_ex", "leader_rk", &payload)
        .await
        .expect("publish_json after election");

    let consumed = ctx
        .rabbit()
        .consume_until(
            "leader_election_q",
            |message| {
                message
                    .payload_json::<ClusterPing>()
                    .map(|parsed| parsed == payload)
                    .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("consume_until after election");

    let parsed: ClusterPing = consumed.payload_json().expect("json");
    assert_eq!(parsed, payload);
}
