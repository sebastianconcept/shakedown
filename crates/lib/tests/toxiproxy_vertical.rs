//! Toxiproxy suite container + control API readiness (US-002).
//!
//! This target is built only with `--features fault-injection`. The test is **`#[ignore]`** so
//! `cargo test --workspace` skips Docker by default.
//!
//! With Docker available:
//!
//! ```text
//! cargo test -p shakedown --features fault-injection --test toxiproxy_vertical -- --ignored
//! ```

use shakedown::{
    run_scenario, BuiltinResource, Context, EventLog, RegisteredResource, StreamDirection, Toxic,
    ToxicKind, ToxiproxyResource, FAULT_INJECTION_EVENT_SOURCE,
};

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features fault-injection --test toxiproxy_vertical -- --ignored"]
async fn toxiproxy_provisions_version_ready_and_teardown_clears_proxies() {
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Toxiproxy(
        ToxiproxyResource::new().with_image_tag_from_env(),
    ));
    let mut event_log = EventLog::new();

    run_scenario(
        &mut ctx,
        "toxiproxy_vertical_proof",
        |ctx: &mut Context| {
            Box::pin(async move {
                assert!(
                    matches!(
                        ctx.resources().first(),
                        Some(RegisteredResource::Builtin(BuiltinResource::Toxiproxy(_)))
                    ),
                    "expected Toxiproxy builtin"
                );
                let faults = ctx
                    .fault_handle_mut()
                    .expect("FaultHandle after Toxiproxy provision");
                faults
                    .create_proxy("latency_proof", "127.0.0.1:0", "127.0.0.1:9")
                    .await
                    .expect("create_proxy");
                assert!(
                    faults.proxy_listen_address("latency_proof").is_some(),
                    "listen address recorded after create_proxy"
                );
                let toxic = Toxic {
                    kind: ToxicKind::Latency {
                        latency: 50,
                        jitter: 0,
                    },
                    name: "latency_toxic".to_string(),
                    toxicity: 1.0,
                    direction: StreamDirection::Downstream,
                };
                faults
                    .add_toxic("latency_proof", &toxic)
                    .await
                    .expect("add_toxic");
                faults
                    .remove_toxic("latency_proof", "latency_toxic")
                    .await
                    .expect("remove_toxic");
                faults
                    .reset_proxy("latency_proof")
                    .await
                    .expect("reset_proxy");
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario should succeed");

    let fault_events: Vec<_> = event_log
        .events()
        .filter(|event| event.source == FAULT_INJECTION_EVENT_SOURCE)
        .collect();
    assert_eq!(
        fault_events.len(),
        3,
        "add_toxic, remove_toxic, and reset_proxy each emit one Application event"
    );
    assert!(fault_events
        .iter()
        .any(|event| event.message.contains("action=added")));
    assert!(fault_events
        .iter()
        .any(|event| event.message.contains("action=removed")));
    assert!(fault_events
        .iter()
        .any(|event| event.message.contains("action=reset")));
}
