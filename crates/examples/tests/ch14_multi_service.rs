//! Chapter 14: Two services under test — Order calls Fulfillment over HTTP
//!
//! **Resources:** Postgres, Redis, Kafka, MongoDB
//! **Services:** `example-order-service` (3000) + `example-fulfillment-service` (3001)
//! **Assertions:** `assert_row_exists` (Postgres), `assert_document_exists` (MongoDB)
//! **Concepts:** multi-service registration, inter-service URL injection for `FULFILLMENT_URL`
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! The Order Service reads `FULFILLMENT_URL` once at process start. The harness therefore starts
//! **fulfillment-service** before **order-service** by registering Fulfillment first and giving
//! **order-service** `.depends_on("fulfillment-service")` plus
//! `EnvMapping::service("fulfillment-service", "FULFILLMENT_URL")`
//! so the URL exists when the Order container is configured. (A fulfillment → order `depends_on`
//! alone would start Order first and leave `FULFILLMENT_URL` unset at Order startup.)
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,mongodb,kafka,redis --test ch14_multi_service -- --ignored`

use mongodb::bson::doc;
use shakedown::EnvMapping;
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::test(resources = [Postgres, Redis, Kafka, Mongo])]
#[ignore = "requires Docker + locally-built example-order-service:latest and example-fulfillment-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,mongodb,kafka,redis --test ch14_multi_service -- --ignored"]
async fn order_and_fulfillment_http_handshake_writes_postgres_and_mongo(
    ctx: &mut shakedown::Context,
) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema");

    let fulfillment_spec =
        shakedown::ServiceUnderTest::docker("example-fulfillment-service:latest")
            .name("fulfillment-service")
            .port(3001)
            .health_check(shakedown::HealthCheck::http("/health"))
            .pull_policy(shakedown::PullPolicy::Never)
            .build()
            .expect("fulfillment service spec");

    ctx.register_service(fulfillment_spec)
        .expect("register fulfillment-service");

    let order_spec = shakedown::ServiceUnderTest::docker("example-order-service:latest")
        .name("order-service")
        .port(3000)
        .health_check(shakedown::HealthCheck::http("/health"))
        .depends_on("fulfillment-service")
        .env_mapping(EnvMapping::service(
            "fulfillment-service",
            "FULFILLMENT_URL",
        ))
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("order service spec");

    ctx.register_service(order_spec)
        .expect("register order-service");

    let order_base_url = ctx
        .service_url("order-service")
        .expect("order service URL after readiness");
    assert!(
        ctx.service_url("fulfillment-service").is_some(),
        "fulfillment-service should be running before order-service body runs"
    );

    let http_client = reqwest::Client::new();
    let create_response = http_client
        .post(format!("{order_base_url}/orders"))
        .json(&sample_order("customer-ch14"))
        .send()
        .await
        .expect("POST /orders");

    assert_eq!(create_response.status(), reqwest::StatusCode::CREATED);
    let created: serde_json::Value = create_response.json().await.expect("created JSON");
    let order_id = created["id"].as_str().expect("order id");

    ctx.postgres()
        .assert_row_exists(
            "SELECT 1 FROM orders WHERE id = $1 AND customer_id = $2",
            &[&order_id, &"customer-ch14"],
        )
        .await
        .expect("order row in Postgres after create");

    let fulfill_response = http_client
        .post(format!("{order_base_url}/orders/{order_id}/fulfill"))
        .send()
        .await
        .expect("POST /orders/:id/fulfill");

    assert_eq!(
        fulfill_response.status(),
        reqwest::StatusCode::OK,
        "order-service should reach fulfillment-service over injected FULFILLMENT_URL"
    );

    ctx.mongo()
        .assert_document_exists("fulfillments", doc! { "order_id": order_id })
        .await
        .expect("fulfillment document in MongoDB after HTTP fulfill handoff");
}
