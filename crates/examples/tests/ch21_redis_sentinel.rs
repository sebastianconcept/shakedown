//! Chapter 21: Order Service with Redis Sentinel (three Redis nodes + three Sentinels)
//!
//! **Resources:** Postgres, `RedisSentinel`, Kafka
//! **Service:** `order-service`
//! **Assertions:** `redis_sentinel_cluster().trigger_failover`, `await_failover_complete`,
//!   `current_master`, `redis().assert_key_equals` after failover
//! **Concepts:** Convention injection resolves the Sentinel-managed primary; cache reads through
//!   the same `ctx.redis()` helpers after an orchestrated failover
//! **Prerequisites:** `just build-example-services`, Docker
//!
//! Run: `cargo test -p shakedown-examples --features services,redis-sentinel,postgres,kafka --test ch21_redis_sentinel -- --ignored`

use std::time::Duration;

use serial_test::serial;
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[shakedown::test(resources = [Postgres, RedisSentinel, Kafka])]
#[serial]
#[ignore = "requires Docker + locally-built order-service:latest; run: just build-example-services && cargo test -p shakedown-examples --features services,redis-sentinel,postgres,kafka --test ch21_redis_sentinel -- --ignored"]
async fn order_service_redis_cache_survives_sentinel_failover(ctx: &mut shakedown::Context) {
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

    let base_url = ctx
        .service_url("order-service")
        .expect("order service URL after readiness");

    let http_client = reqwest::Client::new();
    let create_response = http_client
        .post(format!("{base_url}/orders"))
        .json(&sample_order("customer-ch21"))
        .send()
        .await
        .expect("POST /orders before failover");

    assert_eq!(create_response.status(), reqwest::StatusCode::CREATED);
    let created: serde_json::Value = create_response.json().await.expect("created JSON");
    let order_id = created["id"].as_str().expect("order id");

    let cache_key = format!("order:{order_id}:status");
    ctx.redis()
        .assert_key_equals(cache_key.as_str(), "created")
        .await
        .expect("Redis cache should mirror order status before failover");

    let sentinel = ctx
        .redis_sentinel_cluster()
        .expect("Redis Sentinel cluster control");
    let master_before = sentinel
        .current_master()
        .await
        .expect("current master before failover");
    sentinel
        .trigger_failover()
        .await
        .expect("SENTINEL failover trigger");
    let master_after = sentinel
        .await_failover_complete(Duration::from_secs(90))
        .await
        .expect("failover completion");
    assert_ne!(
        master_before, master_after,
        "Sentinel should elect a different Redis primary (before={master_before} after={master_after})"
    );

    ctx.redis()
        .assert_key_equals(cache_key.as_str(), "created")
        .await
        .expect("same cache key readable via harness after Sentinel failover");
}
