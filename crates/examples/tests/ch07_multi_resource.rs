//! Chapter 7: Kafka, Postgres, and Redis together
//!
//! **Resources:** Kafka, Postgres, Redis (Docker)
//! **Assertions:** Postgres `execute` + `assert_row_exists`, Redis `set` + `assert_key_equals`,
//!   Kafka `produce_json` + `consume_until`
//! **Concepts:** Seed relational truth, mirror hot state in Redis, broadcast on Kafka — the same
//!   triangle the Order Service exercises in Part II.
//! **Teaching intent:** Compose three first-party resources in one scenario with a single coherent
//!   order narrative.
//!
//! Run with Docker: `cargo test -p shakedown-examples --features kafka,postgres,redis --test ch07_multi_resource -- --ignored`

use serde::{Deserialize, Serialize};
use shakedown_examples::{sample_order, ORDERS_SCHEMA_SQL};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct OrderAcceptedEvent {
    aggregate_id: String,
    customer_id: String,
}

#[shakedown::test(resources = [Kafka, Postgres, Redis])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features kafka,postgres,redis --test ch07_multi_resource -- --ignored"]
async fn kafka_postgres_redis_order_acceptance_flow(ctx: &mut shakedown::Context) {
    ctx.postgres()
        .execute(ORDERS_SCHEMA_SQL, &[])
        .await
        .expect("orders table");

    let order_body = sample_order("customer-ch07");
    let order_id = "order-ch07-1";
    let customer_id = order_body["customer_id"]
        .as_str()
        .expect("customer_id in sample payload");
    let items_json = serde_json::to_string(&order_body["items"]).expect("items json");
    let status = "created";

    ctx.postgres()
        .execute(
            "INSERT INTO orders (id, customer_id, items, status) VALUES ($1, $2, $3, $4)",
            &[&order_id, &customer_id, &items_json, &status],
        )
        .await
        .expect("insert order");

    let cache_key = format!("order:{order_id}:status");
    ctx.redis()
        .set(cache_key.as_str(), "created")
        .await
        .expect("mirror status in Redis");

    let event = OrderAcceptedEvent {
        aggregate_id: order_id.to_string(),
        customer_id: customer_id.to_string(),
    };
    ctx.kafka()
        .produce_json("orders", order_id, &event)
        .await
        .expect("publish acceptance event");

    ctx.postgres()
        .assert_row_exists(
            "SELECT 1 FROM orders WHERE id = $1 AND customer_id = $2",
            &[&order_id, &customer_id],
        )
        .await
        .expect("order row in Postgres");

    ctx.redis()
        .assert_key_equals(cache_key.as_str(), "created")
        .await
        .expect("Redis cache matches inserted status");

    let order_id_for_predicate = order_id.to_string();
    let customer_for_predicate = customer_id.to_string();
    let consumed = ctx
        .kafka()
        .consume_until(
            "orders",
            move |message| {
                message.key_str() == Some(order_id_for_predicate.as_str())
                    && message
                        .payload_json::<OrderAcceptedEvent>()
                        .map(|parsed| {
                            parsed.aggregate_id == order_id_for_predicate
                                && parsed.customer_id == customer_for_predicate
                        })
                        .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("Kafka carries the acceptance event for this order");

    assert_eq!(consumed.key_str(), Some(order_id));
}
