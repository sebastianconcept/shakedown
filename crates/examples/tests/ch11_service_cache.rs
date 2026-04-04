//! Chapter 11: Redis cache populated by the Order Service
//!
//! **Resources:** Postgres, Redis, Kafka
//! **Service:** `order-service`
//! **Assertions:** `assert_key_exists`, `assert_key_equals` after HTTP `GET`
//! **Concepts:** status cache key shape `order:<id>:status`; `GET /orders/:id` prefers Redis then
//!   falls back to Postgres — we assert the hot-cache path and value
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,postgres,redis,kafka --test ch11_service_cache -- --ignored`

use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::test(resources = [Postgres, Redis, Kafka])]
#[ignore = "requires Docker + locally-built order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,postgres,redis,kafka --test ch11_service_cache -- --ignored"]
async fn order_service_redis_cache_hit_on_get_order(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("seed orders schema");

    let order_spec = shakedown::ServiceUnderTest::docker("order-service:latest")
        .name("order-service")
        .port(3000)
        .health_check(shakedown::HealthCheck::http("/health"))
        .pull_policy(shakedown::PullPolicy::Never)
        .build()
        .expect("order service spec");

    ctx.register_service(order_spec)
        .expect("register order-service");

    let base_url = ctx.service_url("order-service").expect("order service URL");
    assert!(ctx
        .service_handle("order-service")
        .expect("handle")
        .is_healthy());

    let http_client = reqwest::Client::new();
    let create_response = http_client
        .post(format!("{base_url}/orders"))
        .json(&sample_order("customer-ch11"))
        .send()
        .await
        .expect("POST /orders");

    assert_eq!(create_response.status(), reqwest::StatusCode::CREATED);
    let created: serde_json::Value = create_response.json().await.expect("created body JSON");
    let order_id = created["id"].as_str().expect("order id");

    let cache_key = format!("order:{order_id}:status");
    ctx.redis()
        .assert_key_exists(cache_key.as_str())
        .await
        .expect("create_order should prime Redis with order status");

    let get_response = http_client
        .get(format!("{base_url}/orders/{order_id}"))
        .send()
        .await
        .expect("GET /orders/:id");

    assert_eq!(get_response.status(), reqwest::StatusCode::OK);
    let detail: serde_json::Value = get_response.json().await.expect("order detail JSON");
    assert_eq!(detail["id"].as_str(), Some(order_id));
    assert_eq!(detail["status"].as_str(), Some("created"));

    ctx.redis()
        .assert_key_equals(cache_key.as_str(), "created")
        .await
        .expect("cache should still hold created status after GET");
}
