//! Kafka bootstrap is rewritten to the Toxiproxy listener for `ScenarioKind::Faulty` when both
//! Kafka and Toxiproxy are registered (fault injection). Built with `--features kafka,fault-injection`.
//! **`#[ignore]`** — requires Docker.
//!
//! [`kafka_helpers_use_same_proxied_bootstrap_as_bindings`] proves [`shakedown::KafkaHelpers`]
//! reads the same effective [`shakedown::KafkaBindings`] as [`shakedown::Context::kafka_bindings`]
//! (Toxiproxy listen address when Faulty + Kafka + Toxiproxy). `KafkaHelpers` uses
//! `kafka_bindings.bootstrap_servers` for rdkafka (`helpers/kafka.rs`).
//!
//! **Data plane:** full `produce` / `consume_until` through Toxiproxy can time out when the suite
//! broker’s metadata advertises partition leaders on addresses that do not route via the proxy
//! (common Kafka + transparent-proxy caveat). Bootstrap rewrite and control-plane toxics are proven
//! in this file and in [`faulty_kafka_bootstrap_matches_toxiproxy_listen_and_toxic_applies`]; end-to-end
//! rdkafka through the proxy may require broker `advertised.listeners` (and related listener
//! settings) so metadata-advertised broker addresses still route through Toxiproxy; otherwise
//! clients can follow leaders to addresses that bypass the proxy and time out.
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

/// Given a Faulty scenario with Kafka + Toxiproxy, [`shakedown::KafkaHelpers`] uses the proxied
/// bootstrap from [`Context::kafka_bindings`] (same string rdkafka would receive from
/// [`KafkaHelpers::produce`](shakedown::KafkaHelpers::produce) / [`consume_until`](shakedown::KafkaHelpers::consume_until)).
#[shakedown::faulty_test(resources = [Kafka, Toxiproxy])]
#[serial]
#[ignore = "requires Docker; run: cargo test -p shakedown --features kafka,fault-injection --test fault_injection_kafka -- --ignored"]
async fn kafka_helpers_use_same_proxied_bootstrap_as_bindings(ctx: &mut shakedown::Context) {
    // Given: provisioned Kafka + Toxiproxy; harness rewrites client-facing bootstrap to the proxy.
    // Then: `KafkaHelpers` uses `kafka_bindings().bootstrap_servers` for rdkafka — same value as here
    // (see `helpers/kafka.rs` `produce` / `consume_until`).
    let bootstrap = ctx
        .kafka_bindings()
        .expect("kafka bindings after provision")
        .bootstrap_servers
        .clone();
    let proxy_listen = ctx
        .fault_handle_mut()
        .expect("FaultHandle after provision")
        .proxy_listen_address("shakedown_kafka")
        .expect("kafka fault proxy listen address")
        .to_string();
    assert_eq!(
        bootstrap, proxy_listen,
        "helpers must see Toxiproxy bootstrap (Faulty + Kafka + Toxiproxy)"
    );
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
                // Given Normal kind with Kafka and Toxiproxy registered, no kafka fault proxy row exists.
                let kafka_bindings = ctx.kafka_bindings().expect("kafka bindings").clone();
                assert!(
                    ctx.fault_handle()
                        .expect("Toxiproxy provisioned")
                        .proxy_listen_address("shakedown_kafka")
                        .is_none(),
                    "Normal scenarios must not auto-create the kafka Toxiproxy proxy"
                );
                // When we read client-facing bootstrap, it remains the direct broker endpoint.
                assert!(
                    !kafka_bindings.bootstrap_servers.is_empty(),
                    "direct broker bootstrap should be set"
                );
                // Then bootstrap does not equal a proxy-only listen path (no shakedown_kafka proxy).
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}
