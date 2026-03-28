//! Kafka bootstrap is rewritten to the Toxiproxy listener for `ScenarioKind::Faulty` when both
//! Kafka and Toxiproxy are registered (US-004). Built with `--features kafka,fault-injection`.
//! **`#[ignore]`** — requires Docker.
//!
//! Full rdkafka produce/consume through Toxiproxy may require broker `advertised.listeners` aligned
//! with the proxy; this test proves the harness binding rewrite and pass-through proxy. Data-plane
//! latency checks belong in broker-tuned integration or follow-up work.
//!
//! ```text
//! cargo test -p shakedown --features kafka,fault-injection --test fault_injection_kafka -- --ignored
//! ```

use serial_test::serial;
use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, KafkaResource, ScenarioKind, StreamDirection,
    Toxic, ToxicKind, ToxiproxyResource,
};

fn register_kafka_then_toxiproxy(ctx: &mut Context) {
    ctx.register_builtin(BuiltinResource::Kafka(
        KafkaResource::new().with_image_tag_from_env(),
    ));
    ctx.register_builtin(BuiltinResource::Toxiproxy(
        ToxiproxyResource::new().with_image_tag_from_env(),
    ));
}

#[tokio::test]
#[serial]
#[ignore = "requires Docker; run: cargo test -p shakedown --features kafka,fault-injection --test fault_injection_kafka -- --ignored"]
async fn faulty_kafka_bootstrap_matches_toxiproxy_listen_and_toxic_applies() {
    let mut ctx = Context::new();
    ctx.set_scenario_kind(ScenarioKind::Faulty);
    register_kafka_then_toxiproxy(&mut ctx);
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "fault_injection_kafka_faulty",
        |ctx: &mut Context| {
            Box::pin(async move {
                // Given a Faulty scenario with Kafka + Toxiproxy, client-facing bootstrap is the proxy.
                let kafka_bindings = ctx
                    .kafka_bindings()
                    .expect("kafka bindings after provision")
                    .clone();
                let proxy_listen = {
                    let faults = ctx.fault_handle_mut().expect("FaultHandle");
                    faults
                        .proxy_listen_address("shakedown_kafka")
                        .expect("provision-time kafka proxy")
                        .to_string()
                };
                assert_eq!(
                    kafka_bindings.bootstrap_servers, proxy_listen,
                    "KafkaBindings must use the Toxiproxy listen address for Faulty + both resources"
                );

                // When we add a latency toxic, the control plane accepts it (data path is pass-through).
                let faults = ctx.fault_handle_mut().expect("FaultHandle");
                let toxic = Toxic {
                    kind: ToxicKind::Latency {
                        latency: 100,
                        jitter: 0,
                    },
                    name: "proof_latency".to_string(),
                    toxicity: 1.0,
                    direction: StreamDirection::Downstream,
                };
                faults
                    .add_toxic("shakedown_kafka", &toxic)
                    .await
                    .expect("add toxic on kafka proxy");
                faults
                    .remove_toxic("shakedown_kafka", "proof_latency")
                    .await
                    .expect("remove toxic");

                // Then the harness kept bootstrap aligned with the proxy and the control plane
                // accepted add/remove (pass-through data path; full rdkafka timing checks are optional).
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}

#[tokio::test]
#[serial]
#[ignore = "requires Docker; run: cargo test -p shakedown --features kafka,fault-injection --test fault_injection_kafka -- --ignored"]
async fn normal_kafka_bootstrap_stays_direct_with_toxiproxy_registered() {
    let mut ctx = Context::new();
    ctx.set_scenario_kind(ScenarioKind::Normal);
    register_kafka_then_toxiproxy(&mut ctx);
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "fault_injection_kafka_normal",
        |ctx: &mut Context| {
            Box::pin(async move {
                // Given Normal kind, no suite kafka proxy is created; bootstrap is the real broker.
                let kafka_bindings = ctx.kafka_bindings().expect("kafka bindings").clone();
                assert!(
                    ctx.fault_handle()
                        .expect("Toxiproxy provisioned")
                        .proxy_listen_address("shakedown_kafka")
                        .is_none(),
                    "Normal scenarios must not auto-create the kafka Toxiproxy proxy"
                );
                assert!(
                    !kafka_bindings.bootstrap_servers.is_empty(),
                    "direct broker bootstrap should be set"
                );

                // Then Normal runs use the real broker string; no automatic kafka proxy row exists.
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}
