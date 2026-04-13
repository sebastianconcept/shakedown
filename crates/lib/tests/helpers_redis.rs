//! Redis typed helpers: set + assert (Docker).
//!
//! Built with `--features redis`. **`#[ignore]`** unless `--ignored`.
//!
//! ```text
//! cargo test -p shakedown --features redis --test helpers_redis -- --ignored
//! ```

#[shakedown::test(resources = [Redis])]
#[ignore = "requires Docker; run: cargo test -p shakedown --features redis --test helpers_redis -- --ignored"]
async fn redis_helpers_set_then_assert(ctx: &mut shakedown::Context) {
    ctx.redis()
        .expect("redis helpers")
        .set("helper_key", "helper_value")
        .await
        .expect("set via helpers");
    ctx.redis()
        .expect("redis helpers")
        .assert_key_exists("helper_key")
        .await
        .expect("assert_key_exists");
    ctx.redis()
        .expect("redis helpers")
        .assert_key_equals("helper_key", "helper_value")
        .await
        .expect("assert_key_equals");
    ctx.redis()
        .expect("redis helpers")
        .assert_key_absent("missing_helper_key")
        .await
        .expect("assert_key_absent");
}
