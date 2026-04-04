//! Appendix L: Valkey — Redis-compatible helpers and Redis Streams publication proof
//!
//! **Resources:** Valkey (Docker), SQLite (in-process) for outbox storage in the second test
//! **Assertions:** `set`, `get`, `assert_key_exists`, `assert_key_equals`, `outbox().setup`,
//!   `sqlite().execute`, `outbox().assert_redis_streams_published`
//! **Teaching intent:** Swap Redis for Valkey without changing helper calls, then prove outbox
//!   relays can target Redis Streams with scenario-namespaced stream keys.
//!
//! Run: `cargo test -p shakedown-examples --features valkey --test appendix_valkey -- --ignored`
//! Streams proof: add `sqlite,outbox` features to the cargo command.

use std::time::Duration;

use redis::AsyncCommands;

#[shakedown::test(resources = [Valkey])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features valkey --test appendix_valkey -- --ignored"]
async fn valkey_order_status_cache_helpers(ctx: &mut shakedown::Context) {
    let logical_key = "order:status:order-valkey-1";

    ctx.redis()
        .set(logical_key, "awaiting_payment")
        .await
        .expect("seed order status");

    ctx.redis()
        .assert_key_exists(logical_key)
        .await
        .expect("cache key exists");

    ctx.redis()
        .assert_key_equals(logical_key, "awaiting_payment")
        .await
        .expect("value matches seeded status");

    ctx.redis()
        .set(logical_key, "paid")
        .await
        .expect("transition order status");

    let current = ctx
        .redis()
        .get(logical_key)
        .await
        .expect("read updated status");
    assert_eq!(current.as_deref(), Some("paid"));
}

#[shakedown::test(resources = [Sqlite, Valkey])]
#[ignore = "requires Docker for Valkey; run: cargo test -p shakedown-examples --features valkey,sqlite,outbox --test appendix_valkey -- --ignored"]
async fn sqlite_outbox_row_plus_valkey_streams_publication_proof(ctx: &mut shakedown::Context) {
    ctx.outbox().setup().await.expect("outbox DDL on SQLite");

    let aggregate_id = "order-valkey-streams-1";
    let payload_json = serde_json::json!({
        "aggregate_id": aggregate_id,
        "customer_id": "customer-valkey-streams",
        "total": 88.0
    });
    let payload_string = payload_json.to_string();
    let aggregate_id_owned = aggregate_id.to_string();

    ctx.sqlite()
        .execute(
            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts) \
             VALUES ('order', ?1, 'OrderPaid', ?2, 'pending', 0)",
            (aggregate_id_owned, payload_string.clone()),
        )
        .await
        .expect("insert pending outbox row");

    let base_stream = "orders.stream";
    let redis_bindings = ctx
        .redis_bindings()
        .expect("Valkey bindings after provision");
    let connection_url = redis_bindings.connection_url.clone();
    let namespaced_stream = format!("{}{}", redis_bindings.key_prefix, base_stream);
    let payload_for_stream = payload_string;

    let publish = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(80)).await;
        let client = redis::Client::open(connection_url.as_str()).expect("open Valkey client");
        let mut connection = client
            .get_multiplexed_async_connection()
            .await
            .expect("Valkey connection for XADD");
        let _: String = connection
            .xadd(
                namespaced_stream.as_str(),
                "*",
                &[("payload", payload_for_stream.as_str())],
            )
            .await
            .expect("XADD onto namespaced stream");
    });

    let outbox = ctx.outbox();
    let verify = outbox.assert_redis_streams_published(base_stream, aggregate_id);

    let (publish_result, verify_result) = tokio::join!(publish, verify);
    publish_result.expect("publish task");
    verify_result.expect("Redis Streams verifier should observe aggregate_id in payload");
}
