//! Chapter 4: Redis cache-style assertions
//!
//! **Resources:** Redis (Docker)
//! **Assertions:** `set`, `get`, `assert_key_exists`, `assert_key_equals`, `assert_key_absent`
//! **Concepts:** logical keys with scenario prefixing, negative assertions
//! **Teaching intent:** Model a short-lived order status cache and prove both presence and deliberate
//!   absence before integration tests hit HTTP + Redis together.
//!
//! Run with Docker: `cargo test -p shakedown-examples --features redis --test ch04_redis -- --ignored`

#[shakedown::test(resources = [Redis])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features redis --test ch04_redis -- --ignored"]
async fn redis_order_status_cache_assertions(ctx: &mut shakedown::Context) {
    let logical_key = "order:status:order-ch04-1";

    ctx.redis()
        .set(logical_key, "payment_pending")
        .await
        .expect("seed cache with order status");

    ctx.redis()
        .assert_key_exists(logical_key)
        .await
        .expect("cached status key should exist");

    ctx.redis()
        .assert_key_equals(logical_key, "payment_pending")
        .await
        .expect("value should match what fulfillment will later overwrite");

    ctx.redis()
        .set(logical_key, "ready_to_ship")
        .await
        .expect("simulate downstream update");

    ctx.redis()
        .assert_key_equals(logical_key, "ready_to_ship")
        .await
        .expect("cache reflects updated fulfillment state");

    ctx.redis()
        .del(logical_key)
        .await
        .expect("remove cache entry after archival");

    ctx.redis()
        .assert_key_absent(logical_key)
        .await
        .expect("archived orders should not retain a hot cache key");

    ctx.redis()
        .assert_key_absent("order:status:unknown-order")
        .await
        .expect("unknown orders stay absent");
}
