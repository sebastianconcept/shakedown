#![cfg(all(feature = "outbox", feature = "sqlite", feature = "redis"))]

//! SQLite outbox + Redis Streams publication proof (Redis Docker; SQLite in-process).
//!
//! ```text
//! cargo test -p shakedown --features outbox,sqlite,redis --test outbox_redis_streams_publication -- --ignored
//! ```

use std::time::Duration;

use redis::AsyncCommands;
use shakedown::{run_scenario, BuiltinResource, Context, EventLog, RedisResource, SqliteResource};

#[tokio::test]
#[ignore = "requires Docker for Redis; run: cargo test -p shakedown --features outbox,sqlite,redis --test outbox_redis_streams_publication -- --ignored"]
async fn outbox_redis_streams_publication_verifier_observes_json_aggregate_id() {
    let mut context = Context::new();
    context.register_builtin(BuiltinResource::Sqlite(SqliteResource::new()));
    context.register_builtin(BuiltinResource::Redis(
        RedisResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut context,
        "outbox_redis_streams_publication_proof",
        |scenario_context| {
            Box::pin(async move {
                scenario_context
                    .outbox()
                    .setup()
                    .await
                    .expect("setup creates outbox_event");

                let base_stream = "orders.stream";
                let aggregate_id = "order-redis-1";
                let payload = format!(r#"{{"aggregate_id":"{aggregate_id}","total":1.0}}"#);

                let redis_bindings = scenario_context
                    .redis_bindings()
                    .expect("redis bindings after provision");
                let connection_url = redis_bindings.connection_url.clone();
                let namespaced_stream = format!("{}{}", redis_bindings.key_prefix, base_stream);

                let publish = tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    let client = redis::Client::open(connection_url.as_str())
                        .expect("open redis client for XADD");
                    let mut connection = client
                        .get_multiplexed_async_connection()
                        .await
                        .expect("redis connection for XADD");
                    let _: String = connection
                        .xadd(
                            namespaced_stream.as_str(),
                            "*",
                            &[("payload", payload.as_str())],
                        )
                        .await
                        .expect("XADD to namespaced stream");
                });

                let outbox = scenario_context.outbox();
                let verify = outbox.assert_redis_streams_published(base_stream, aggregate_id);

                let (publish_result, verify_result) = tokio::join!(publish, verify);
                publish_result.expect("publish task");
                verify_result.expect("Redis Stream message should be observed");
            })
        },
        &mut event_log,
    )
    .await
    .expect("scenario should pass");
}
