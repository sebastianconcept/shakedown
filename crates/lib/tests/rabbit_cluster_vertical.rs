//! RabbitMQ 3-node Erlang cluster: provision, multi-host URI, helpers, quorum queue (`rabbitmq-cluster`).
//!
//! **`#[ignore]`** — run with Docker:
//!
//! ```text
//! cargo test -p shakedown --features rabbitmq-cluster --test rabbit_cluster_vertical -- --ignored
//! ```

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
