//! Chapter 22: Order Service with PostgreSQL primary + streaming replica
//!
//! **Resources:** `PostgresReplicaSet`, Redis, Kafka
//! **Service:** `order-service` (started against the initial primary)
//! **Assertions:** `postgres_replica_set_cluster().promote_replica`, `stop_node`,
//!   `current_primary`, `set_postgres_bindings` + `postgres().execute` on the promoted primary
//! **Concepts:** Streaming replica promotion + stopping the former primary; harness bindings must
//!   follow the writable instance for SQL assertions (the container keeps its original `DATABASE_URL`)
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres-cluster,redis,kafka --test ch22_postgres_replica_set -- --ignored`

use std::time::Duration;

use serial_test::serial;
use shakedown::PostgresReplicaSetMarker;
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::test(resources = [PostgresReplicaSet, Redis, Kafka])]
#[serial]
#[ignore = "requires Docker + locally-built order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres-cluster,redis,kafka --test ch22_postgres_replica_set -- --ignored"]
async fn postgres_promotion_then_harness_writes_on_new_primary(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema on initial primary");

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
        .json(&sample_order("customer-ch22"))
        .send()
        .await
        .expect("POST /orders before promotion");

    assert_eq!(create_response.status(), reqwest::StatusCode::CREATED);
    let created: serde_json::Value = create_response.json().await.expect("created JSON");
    let order_id = created["id"].as_str().expect("order id");

    ctx.postgres()
        .assert_row_exists("SELECT 1 FROM orders WHERE id = $1", &[&order_id])
        .await
        .expect("order visible on initial primary");

    let cluster = ctx
        .postgres_replica_set_cluster()
        .expect("PostgreSQL replica set control");

    assert_eq!(
        cluster
            .current_primary()
            .await
            .expect("primary before promotion"),
        0
    );

    cluster
        .promote_replica(1)
        .await
        .expect("promote streaming replica to primary");

    let cluster_handle = ctx
        .cluster_handle::<PostgresReplicaSetMarker>()
        .expect("Postgres cluster handle");
    cluster_handle
        .stop_node(0)
        .await
        .expect("stop former primary container");

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert_eq!(
        cluster
            .current_primary()
            .await
            .expect("primary after promotion"),
        1
    );

    let mut bindings = ctx.postgres_bindings().expect("postgres bindings").clone();
    let replica_port = bindings
        .replica_port
        .expect("replica_port for postgres-cluster bindings");
    bindings.port = replica_port;
    bindings.connection_url = format!(
        "postgresql://{}:{}@{}:{}/{}",
        bindings.username, bindings.password, bindings.host, replica_port, bindings.database
    );
    ctx.set_postgres_bindings(bindings);

    ctx.postgres()
        .execute(
            "INSERT INTO orders (id, customer_id, items, status) VALUES ($1, $2, $3, $4)",
            &[
                &"order-ch22-harness",
                &"customer-ch22-harness",
                &"[{\"product_id\":\"widget-1\",\"quantity\":1}]",
                &"created",
            ],
        )
        .await
        .expect("insert on promoted primary via updated bindings");

    ctx.postgres()
        .assert_row_exists(
            "SELECT 1 FROM orders WHERE id = $1 AND customer_id = $2",
            &[&"order-ch22-harness", &"customer-ch22-harness"],
        )
        .await
        .expect("harness-inserted row on new primary");
}
