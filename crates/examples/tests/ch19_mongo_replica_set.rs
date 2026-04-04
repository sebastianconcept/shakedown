//! Chapter 19: Fulfillment Service on a three-member MongoDB replica set
//!
//! **Resources:** `MongoReplicaSet`, Kafka, RabbitMQ (companion service contract)
//! **Service:** `fulfillment-service`
//! **Assertions:** `mongo().assert_document_exists`, `mongo_cluster().step_down_primary`,
//!   `await_new_primary`, `current_primary`, `cluster_handle::reset_topology`
//! **Concepts:** Same HTTP + Mongo path as Parts II–III with suite-level replica set;
//!   primary step-down + election; restoring all members with `reset_topology`
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,mongodb-cluster,mongodb,kafka,rabbitmq --test ch19_mongo_replica_set -- --ignored`

use std::time::Duration;

use mongodb::bson::doc;
use serial_test::serial;
use shakedown::ExchangeKind;
use shakedown::MongoReplicaSetMarker;
use shakedown_examples::sample_fulfillment;

#[shakedown::test(resources = [MongoReplicaSet, Kafka, Rabbit])]
#[serial]
#[ignore = "requires Docker + locally-built fulfillment-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,mongodb-cluster,mongodb,kafka,rabbitmq --test ch19_mongo_replica_set -- --ignored"]
async fn fulfillment_writes_survive_mongo_primary_election(ctx: &mut shakedown::Context) {
    ctx.rabbit()
        .declare_topology(
            "fulfillments",
            ExchangeKind::Direct,
            "fulfillments-assert",
            "#",
        )
        .await
        .expect("declare RabbitMQ topology before the service publishes");

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
    let first = http_client
        .post(format!("{base_url}/fulfill"))
        .json(&sample_fulfillment("order-ch19-primary-a"))
        .send()
        .await
        .expect("POST /fulfill before election");

    assert_eq!(first.status(), reqwest::StatusCode::CREATED);

    ctx.mongo()
        .assert_document_exists("fulfillments", doc! { "order_id": "order-ch19-primary-a" })
        .await
        .expect("first fulfillment document on the replica set");

    let mongo_cluster = ctx
        .mongo_cluster()
        .expect("Mongo replica set control after provision");
    let primary_before = mongo_cluster
        .current_primary()
        .await
        .expect("current primary before step-down");
    mongo_cluster
        .step_down_primary()
        .await
        .expect("replSetStepDown on current primary");
    let primary_after = mongo_cluster
        .await_new_primary(Duration::from_secs(90))
        .await
        .expect("wait for new primary");
    assert_ne!(
        primary_before, primary_after,
        "expected a different member to become PRIMARY (before={primary_before} after={primary_after})"
    );

    let second = http_client
        .post(format!("{base_url}/fulfill"))
        .json(&sample_fulfillment("order-ch19-primary-b"))
        .send()
        .await
        .expect("POST /fulfill after election");

    assert_eq!(second.status(), reqwest::StatusCode::CREATED);

    ctx.mongo()
        .assert_document_exists("fulfillments", doc! { "order_id": "order-ch19-primary-b" })
        .await
        .expect("second fulfillment visible via harness mongo helpers after election");

    // `reset_topology` restores suite members after a deliberate stop (same scenario — DQ-8 keeps
    // ServiceUnderTest registered throughout).
    let cluster = ctx
        .cluster_handle::<MongoReplicaSetMarker>()
        .expect("suite Mongo replica set cluster handle");
    cluster.stop_node(1).await.expect("stop secondary member");
    assert!(
        cluster.healthy_node_count().await.expect("healthy count") < 3,
        "expected fewer than three healthy members after stop"
    );
    cluster
        .reset_topology()
        .await
        .expect("reset_topology should restart stopped members");
    assert_eq!(
        cluster
            .healthy_node_count()
            .await
            .expect("healthy count after reset"),
        3,
        "expected three healthy mongod containers after reset_topology"
    );
    let primary_after_reset = mongo_cluster
        .current_primary()
        .await
        .expect("primary after topology reset");
    assert!(
        primary_after_reset < 3,
        "expected primary index in 0..3, got {primary_after_reset}"
    );

    let third = http_client
        .post(format!("{base_url}/fulfill"))
        .json(&sample_fulfillment("order-ch19-reset-c"))
        .send()
        .await
        .expect("POST /fulfill after reset_topology");

    assert_eq!(third.status(), reqwest::StatusCode::CREATED);
    ctx.mongo()
        .assert_document_exists("fulfillments", doc! { "order_id": "order-ch19-reset-c" })
        .await
        .expect("fulfillment still lands in Mongo after cluster reset");
}
