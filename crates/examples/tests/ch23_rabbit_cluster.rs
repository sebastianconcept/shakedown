//! Chapter 23: Fulfillment Service on a three-node RabbitMQ Erlang cluster
//!
//! **Resources:** MongoDB, Kafka, `RabbitCluster`
//! **Service:** `fulfillment-service`
//! **Assertions:** `rabbit_cluster().quorum_queue_leader`, `await_queue_leader_election`,
//!   `cluster_handle::stop_node`, `rabbit().publish_json` / `consume_until` after leader change
//! **Concepts:** Quorum queues use Raft; stopping the leader node forces a new queue leader while
//!   publish/consume through shakedown helpers keeps working
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,rabbitmq-cluster,mongodb,kafka --test ch23_rabbit_cluster -- --ignored`

use std::time::Duration;

use lapin::options::QueueDeclareOptions;
use lapin::types::{AMQPValue, FieldTable, LongString};
use lapin::{Connection, ConnectionProperties};
use serde::{Deserialize, Serialize};
use serial_test::serial;
use shakedown::ExchangeKind;
use shakedown::RabbitClusterMarker;
use shakedown_examples::sample_fulfillment;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct FulfillmentPublished {
    proof: String,
}

#[shakedown::test(resources = [Mongo, Kafka, RabbitCluster])]
#[serial]
#[ignore = "requires Docker + locally-built fulfillment-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,rabbitmq-cluster,mongodb,kafka --test ch23_rabbit_cluster -- --ignored"]
async fn fulfillment_rabbit_cluster_quorum_queue_survives_leader_loss(
    ctx: &mut shakedown::Context,
) {
    ctx.rabbit()
        .declare_topology(
            "fulfillments",
            ExchangeKind::Direct,
            "fulfillments-assert",
            "#",
        )
        .await
        .expect("declare fulfillment fan-in before the service starts");

    let rabbit_bindings = ctx
        .rabbit_bindings()
        .expect("RabbitMQ bindings after RabbitCluster provision");

    let queue_name = rabbit_bindings.namespaced_name("fulfillment_quorum_ch23");
    let connection = Connection::connect(
        rabbit_bindings.amqp_uri.as_str(),
        ConnectionProperties::default(),
    )
    .await
    .expect("lapin connect for quorum queue declaration");
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
        .expect("declare quorum queue on the cluster");
    let _ = connection.close(200, "ch23_quorum_prep".into()).await;

    let fulfillment_spec = shakedown::ServiceUnderTest::docker("fulfillment-service:latest")
        .name("fulfillment-service")
        .port(3001)
        .health_check(shakedown::HealthCheck::http("/health"))
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("fulfillment service spec");

    ctx.register_service(fulfillment_spec)
        .expect("register fulfillment-service");

    let base_url = ctx
        .service_url("fulfillment-service")
        .expect("fulfillment URL after readiness");

    let http_client = reqwest::Client::new();
    let create = http_client
        .post(format!("{base_url}/fulfill"))
        .json(&sample_fulfillment("order-ch23-before"))
        .send()
        .await
        .expect("POST /fulfill before quorum leader change");

    assert_eq!(create.status(), reqwest::StatusCode::CREATED);

    let cluster = ctx.rabbit_cluster().expect("RabbitMQ cluster control");
    let leader_before = cluster
        .quorum_queue_leader(queue_name.as_str())
        .await
        .expect("quorum queue leader before stop");

    let generic = ctx
        .cluster_handle::<RabbitClusterMarker>()
        .expect("Rabbit cluster generic handle");
    generic
        .stop_node(leader_before)
        .await
        .expect("stop node hosting quorum leader");

    let leader_after = cluster
        .await_queue_leader_election(queue_name.as_str(), Duration::from_secs(90))
        .await
        .expect("await new quorum leader");
    assert_ne!(
        leader_after, leader_before,
        "expected a different broker index as quorum leader"
    );

    let payload = FulfillmentPublished {
        proof: "after-quorum-election".to_string(),
    };
    ctx.rabbit()
        .declare_topology("ch23_ex", ExchangeKind::Direct, "ch23_q", "ch23_rk")
        .await
        .expect("declare topology for post-election messaging");

    ctx.rabbit()
        .publish_json("ch23_ex", "ch23_rk", &payload)
        .await
        .expect("publish_json after quorum election");

    let consumed = ctx
        .rabbit()
        .consume_until(
            "ch23_q",
            |message| {
                message
                    .payload_json::<FulfillmentPublished>()
                    .map(|parsed| parsed == payload)
                    .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("consume_until after leader change");

    let parsed: FulfillmentPublished = consumed.payload_json().expect("json");
    assert_eq!(parsed, payload);
}
