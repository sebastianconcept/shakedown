//! Redis Sentinel suite: 3 Redis nodes + 3 Sentinels on a bridge network (`redis-sentinel` feature).
//!
//! Built with `--features redis-sentinel`. **`#[ignore]`** so default `cargo test` skips Docker.
//!
//! ```text
//! cargo test -p shakedown --features redis-sentinel --test redis_sentinel_vertical -- --ignored
//! ```

use std::time::Duration;

use redis::AsyncCommands;
use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, LogEntry, RedisSentinelResource,
    RegisteredResource, CLUSTER_EVENT_SOURCE,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features redis-sentinel --test redis_sentinel_vertical -- --ignored"]
async fn redis_sentinel_quorum_helpers_and_replicas() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::RedisSentinel(
        RedisSentinelResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "redis_sentinel_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async move {
                let sentinel = ctx
                    .redis_sentinel_bindings()
                    .expect("redis sentinel bindings after provision");
                assert!(
                    sentinel.sentinel_hosts.split(',').count() >= 3,
                    "expected 3 sentinel endpoints"
                );
                assert_eq!(sentinel.master_name, "mymaster");

                let bindings = ctx.redis_bindings().expect("redis bindings for helpers");
                assert!(
                    bindings.connection_url.starts_with("redis://"),
                    "primary URL"
                );

                let first_sentinel = sentinel
                    .sentinel_hosts
                    .split(',')
                    .next()
                    .expect("one sentinel")
                    .trim();
                let sentinel_url = format!("redis://{first_sentinel}/");
                let sentinel_client =
                    redis::Client::open(sentinel_url.as_str()).expect("sentinel redis client");
                let mut sentinel_connection = sentinel_client
                    .get_multiplexed_async_connection()
                    .await
                    .expect("sentinel connection");
                let _: String = redis::cmd("SENTINEL")
                    .arg("CKQUORUM")
                    .arg(sentinel.master_name.as_str())
                    .query_async(&mut sentinel_connection)
                    .await
                    .expect("SENTINEL CKQUORUM");

                ctx.redis()
                    .expect("redis helpers")
                    .set("sentinel_vertical_k", "sentinel_vertical_v")
                    .await
                    .expect("set on primary via helpers");

                assert_eq!(
                    ctx.redis()
                        .expect("redis helpers")
                        .get("sentinel_vertical_k")
                        .await
                        .expect("get on primary"),
                    Some("sentinel_vertical_v".to_string()),
                    "primary should hold the value before replica read"
                );

                let replica_url = sentinel
                    .replica_connection_urls
                    .first()
                    .expect("one replica URL");
                let replica_client =
                    redis::Client::open(replica_url.as_str()).expect("replica redis client");
                let mut replica_connection = replica_client
                    .get_multiplexed_async_connection()
                    .await
                    .expect("replica connection");

                let namespaced_key = "shakedown:redis_sentinel_vertical_proof:sentinel_vertical_k";
                let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
                let mut replicated: Option<String> = None;
                while tokio::time::Instant::now() < deadline {
                    replicated = replica_connection
                        .get(namespaced_key)
                        .await
                        .expect("get from replica");
                    if replicated.as_deref() == Some("sentinel_vertical_v") {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                assert_eq!(
                    replicated.as_deref(),
                    Some("sentinel_vertical_v"),
                    "replica should serve read after replication"
                );

                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::RedisSentinel(
                            _
                        )))
                    ),
                    "expected RedisSentinel builtin"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features redis-sentinel --test redis_sentinel_vertical -- --ignored"]
async fn redis_sentinel_sentinel_client_set_get_and_survives_failover() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::RedisSentinel(
        RedisSentinelResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "redis_sentinel_sentinel_client_proof",
        |ctx: &mut Context| {
            Box::pin(async move {
                let sentinel = ctx
                    .redis_sentinel_bindings()
                    .expect("redis sentinel bindings after provision");

                let mut sentinel_topology_client = sentinel
                    .sentinel_client()
                    .expect("RedisSentinelBindings::sentinel_client");

                let redis_client = sentinel_topology_client
                    .async_get_client()
                    .await
                    .expect("SentinelClient::async_get_client (master)");
                let mut connection = redis_client
                    .get_multiplexed_async_connection()
                    .await
                    .expect("multiplexed connection via SentinelClient");

                let key = sentinel.key_namespace("sentinel_client_k");
                let _: () = connection
                    .set::<_, _, ()>(key.as_str(), "sentinel_client_v1")
                    .await
                    .expect("SET via SentinelClient-resolved primary");
                let value: Option<String> = connection
                    .get(key.as_str())
                    .await
                    .expect("GET via SentinelClient-resolved primary");
                assert_eq!(value.as_deref(), Some("sentinel_client_v1"));

                let cluster = ctx
                    .redis_sentinel_cluster()
                    .expect("redis sentinel cluster control after provision");
                cluster.trigger_failover().await.expect("trigger_failover");
                cluster
                    .await_failover_complete(Duration::from_secs(90))
                    .await
                    .expect("await_failover_complete");

                let redis_client_after = sentinel_topology_client
                    .async_get_client()
                    .await
                    .expect("async_get_client after failover");
                let mut connection_after = redis_client_after
                    .get_multiplexed_async_connection()
                    .await
                    .expect("connection after failover");
                let key_after = sentinel.key_namespace("sentinel_client_after_failover");
                let _: () = connection_after
                    .set::<_, _, ()>(key_after.as_str(), "sentinel_client_v2")
                    .await
                    .expect("SET after failover via fresh SentinelClient resolution");
                let read_back: Option<String> = connection_after
                    .get(key_after.as_str())
                    .await
                    .expect("GET after failover");
                assert_eq!(read_back.as_deref(), Some("sentinel_client_v2"));
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");
}

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features redis-sentinel --test redis_sentinel_vertical -- --ignored"]
async fn redis_sentinel_trigger_failover_changes_primary() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::RedisSentinel(
        RedisSentinelResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "redis_sentinel_failover_ops",
        |ctx: &mut Context| {
            Box::pin(async move {
                let cluster = ctx
                    .redis_sentinel_cluster()
                    .expect("redis sentinel cluster control after provision");
                let before = cluster.current_master().await.expect("current_master");
                cluster
                    .trigger_failover()
                    .await
                    .expect("trigger_failover");
                let after = cluster
                    .await_failover_complete(std::time::Duration::from_secs(90))
                    .await
                    .expect("await_failover_complete");
                assert_ne!(
                    before, after,
                    "failover should elect a different node as primary (before={before} after={after})"
                );
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");

    let cluster_events: Vec<_> = event_log
        .entries()
        .iter()
        .filter_map(|entry| match entry {
            LogEntry::Application(ev) if ev.source == CLUSTER_EVENT_SOURCE => {
                Some(ev.message.as_str())
            }
            _ => None,
        })
        .collect();
    assert!(
        cluster_events
            .iter()
            .any(|message| message.contains("trigger_failover")),
        "expected trigger_failover in event log: {cluster_events:?}"
    );
    assert!(
        cluster_events
            .iter()
            .any(|message| message.contains("await_failover_complete")),
        "expected await_failover_complete in event log: {cluster_events:?}"
    );
    assert!(
        cluster_events
            .iter()
            .any(|message| message.contains("current_master")),
        "expected current_master in event log: {cluster_events:?}"
    );
}
