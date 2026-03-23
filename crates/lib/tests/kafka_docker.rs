//! Docker-backed Kafka tests (`--features kafka`). Ignored by default so `cargo test` without Docker stays green.
//!
//! Run locally:
//! `cargo test -p shakedown --features kafka --test kafka_docker -- --ignored`

#![cfg(feature = "kafka")]

use async_trait::async_trait;
use shakedown::resources::{BuiltinResource, KafkaResource, Resource};
use shakedown::{
    run_scenario, Context, EventLog, InfrastructureError, ResourceHandle, ScenarioId,
    ShakedownError,
};

#[tokio::test]
#[ignore = "requires Docker"]
async fn kafka_suite_provision_teardown_then_provision_again() {
    let mut a = KafkaResource::new();
    let h1 = a
        .provision(&ScenarioId::new("s1"))
        .await
        .expect("first provision");
    let bs1 = &h1[0].connection_string;
    assert!(
        bs1.starts_with("127.0.0.1:"),
        "unexpected bootstrap {bs1:?}"
    );
    a.teardown().await.expect("first teardown");

    let mut b = KafkaResource::new();
    let h2 = b
        .provision(&ScenarioId::new("s2"))
        .await
        .expect("second provision after suite idle");
    let bs2 = &h2[0].connection_string;
    assert!(
        bs2.starts_with("127.0.0.1:"),
        "unexpected bootstrap after re-provision {bs2:?}"
    );
    b.teardown().await.expect("second teardown");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn kafka_suite_two_holders_share_broker_until_last_teardown() {
    let mut first = KafkaResource::new();
    let ha = first
        .provision(&ScenarioId::new("share-a"))
        .await
        .expect("provision a");
    let bs = ha[0].connection_string.clone();

    let mut second = KafkaResource::new();
    let hb = second
        .provision(&ScenarioId::new("share-b"))
        .await
        .expect("provision b");
    assert_eq!(hb[0].connection_string, bs);

    first.teardown().await.expect("teardown first holder");
    second.teardown().await.expect("teardown second holder");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn kafka_image_tag_override_matches_default_module_tag() {
    // testcontainers-modules `apache::Kafka` defaults to tag `3.8.0`; explicit pin exercises ImageExt path.
    let mut k = KafkaResource::new().with_image_tag("3.8.0");
    k.provision(&ScenarioId::new("tag-pin"))
        .await
        .expect("provision with tag override");
    k.check_ready().await.expect("readiness");
    k.teardown().await.expect("teardown");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn run_scenario_teardowns_kafka_after_readiness_failure() {
    struct AlwaysUnready;

    #[async_trait]
    impl Resource for AlwaysUnready {
        async fn provision(
            &mut self,
            _scenario_id: &ScenarioId,
        ) -> Result<Vec<ResourceHandle>, InfrastructureError> {
            Ok(vec![ResourceHandle {
                id: "unready".to_string(),
                connection_string: "://noop".to_string(),
                metadata: None,
            }])
        }

        async fn check_ready(&self) -> Result<(), InfrastructureError> {
            Err(InfrastructureError::Readiness {
                resource: "unready".to_string(),
                source: Box::new(std::io::Error::other("not ready")),
            })
        }

        async fn teardown(&mut self) -> Result<(), InfrastructureError> {
            Ok(())
        }
    }

    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Kafka(KafkaResource::new()));
    ctx.register_extension(AlwaysUnready);

    let mut log = EventLog::new();
    let err = run_scenario(&mut ctx, "fail-readiness", |_ctx, _log| async {}, &mut log)
        .await
        .expect_err("readiness should fail");

    assert!(
        matches!(
            err,
            ShakedownError::Infrastructure(InfrastructureError::Readiness { .. })
        ),
        "expected readiness error, got {err:?}"
    );

    let mut ctx2 = Context::new();
    ctx2.register_builtin(BuiltinResource::Kafka(KafkaResource::new()));
    run_scenario(
        &mut ctx2,
        "after-failure-teardown",
        |_ctx, _log| async {},
        &mut log,
    )
    .await
    .expect("kafka suite should be idle and provision again");
}
