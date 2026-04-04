//! Chapter 20: Order Service against a three-broker KRaft Kafka cluster
//!
//! **Resources:** Postgres, Redis, `KafkaCluster`
//! **Service:** `order-service`
//! **Assertions:** `kafka_cluster().leader_for_partition`, `await_leader_election`,
//!   `assert_row_exists` after HTTP create, `kafka().produce_json` / `consume_one` after broker loss
//! **Concepts:** Multi-broker bootstrap via convention injection; partition leadership moves when
//!   the leading broker stops; application traffic keeps working with the same topic helpers
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,kafka-cluster,postgres,redis --test ch20_kafka_cluster -- --ignored`

use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::types::RDKafkaErrorCode;
use serde::{Deserialize, Serialize};
use serial_test::serial;
use shakedown::KafkaClusterMarker;
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct OrderCreatedEvent {
    aggregate_id: String,
    customer_id: String,
}

#[shakedown::test(resources = [Postgres, Redis, KafkaCluster])]
#[serial]
#[ignore = "requires Docker + locally-built order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,kafka-cluster,postgres,redis --test ch20_kafka_cluster -- --ignored"]
async fn order_service_kafka_cluster_survives_partition_leader_change(
    ctx: &mut shakedown::Context,
) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema");

    let kafka_bindings = ctx
        .kafka_bindings()
        .expect("Kafka bindings after KafkaCluster provision");
    let bootstrap = kafka_bindings.bootstrap_servers.as_str();
    assert_eq!(
        bootstrap.matches(',').count(),
        2,
        "expected three comma-separated bootstrap hosts for a three-broker cluster"
    );

    let orders_topic = kafka_bindings.topic_namespace("orders");
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("client.id", "shakedown_examples_ch20_admin")
        .create()
        .expect("Kafka admin client");

    let new_topics = [NewTopic::new(
        orders_topic.as_str(),
        1,
        TopicReplication::Fixed(3),
    )];
    let results = admin
        .create_topics(&new_topics, &AdminOptions::new())
        .await
        .expect("create orders topic with RF=3");
    for result in results {
        if let Err((_, code)) = result {
            assert_eq!(code, RDKafkaErrorCode::TopicAlreadyExists);
        }
    }

    let order_spec = shakedown::ServiceUnderTest::docker("order-service:latest")
        .name("order-service")
        .port(3000)
        .health_check(shakedown::HealthCheck::http("/health"))
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("order service spec");

    ctx.register_service(order_spec)
        .expect("register order-service");

    let base_url = ctx
        .service_url("order-service")
        .expect("order service URL after readiness");

    let http_client = reqwest::Client::new();
    let create_response = http_client
        .post(format!("{base_url}/orders"))
        .json(&sample_order("customer-ch20"))
        .send()
        .await
        .expect("POST /orders before broker stop");

    assert_eq!(create_response.status(), reqwest::StatusCode::CREATED);
    let created: serde_json::Value = create_response.json().await.expect("created JSON");
    let order_id = created["id"].as_str().expect("order id");

    ctx.postgres()
        .assert_row_exists(
            "SELECT 1 FROM orders WHERE id = $1 AND customer_id = $2",
            &[&order_id, &"customer-ch20"],
        )
        .await
        .expect("order row in Postgres before broker stop");

    let cluster = ctx
        .kafka_cluster()
        .expect("Kafka cluster control after provision");
    let leader_idx = cluster
        .leader_for_partition(orders_topic.as_str(), 0)
        .await
        .expect("leader_for_partition for orders topic");

    let generic = ctx
        .cluster_handle::<KafkaClusterMarker>()
        .expect("Kafka cluster generic handle");
    generic
        .stop_node(leader_idx)
        .await
        .expect("stop the broker that leads partition 0");

    let new_leader = cluster
        .await_leader_election(orders_topic.as_str(), 0, Duration::from_secs(90))
        .await
        .expect("await new partition leader");
    assert_ne!(
        new_leader, leader_idx,
        "expected leadership to move to another broker index"
    );

    let proof = OrderCreatedEvent {
        aggregate_id: "ch20-after-election".to_string(),
        customer_id: "customer-ch20".to_string(),
    };
    ctx.kafka()
        .produce_json("orders", "proof-key", &proof)
        .await
        .expect("produce_json after leader election");

    let expected_aggregate_id = proof.aggregate_id.clone();
    let consumed = ctx
        .kafka()
        .consume_until(
            "orders",
            move |message| {
                message
                    .payload_json::<OrderCreatedEvent>()
                    .map(|event| event.aggregate_id == expected_aggregate_id)
                    .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("consume_until proof event after election");
    let parsed: OrderCreatedEvent = consumed.payload_json().expect("json payload");
    assert_eq!(parsed, proof);
}
