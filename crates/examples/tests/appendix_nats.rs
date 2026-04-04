//! Appendix A: NATS — publish, subscribe, scenario subject isolation, and outbox + JetStream proof
//!
//! **Resources:** NATS (Docker), SQLite (in-process) for outbox storage in the second test
//! **Assertions:** `publish`, `subscribe_until`, `nats_bindings().subject_namespace`, `outbox().setup`,
//!   `sqlite().execute`, `outbox().assert_nats_published`
//! **Teaching intent:** Use logical subject names in tests; the harness namespaces them per scenario.
//!   Pair SQLite outbox rows with JetStream publication verification the same way a relay would.
//!
//! Run (Docker): `cargo test -p shakedown-examples --features sqlite,nats,outbox --test appendix_nats -- --ignored`

use serde_json::json;

#[shakedown::test(resources = [Nats])]
#[ignore = "requires Docker; run: cargo test -p shakedown-examples --features nats --test appendix_nats -- --ignored"]
async fn nats_publish_subscribe_and_subject_namespace_isolation(ctx: &mut shakedown::Context) {
    let logical_subject = "orders.created";
    let payload = br#"{"aggregate_id":"order-nats-evt-1","event":"OrderCreated"}"#;

    let expected_full_subject = ctx
        .nats_bindings()
        .expect("NATS bindings after provision")
        .subject_namespace(logical_subject);

    ctx.nats()
        .publish(logical_subject, payload)
        .await
        .expect("publish to logical subject");

    let messages = ctx
        .nats()
        .subscribe_until(
            logical_subject,
            |message| {
                message
                    .payload
                    .as_deref()
                    .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
                    .and_then(|value| {
                        value["aggregate_id"]
                            .as_str()
                            .map(|id| id == "order-nats-evt-1")
                    })
                    .unwrap_or(false)
            },
            None,
        )
        .await
        .expect("subscribe_until should see the published order event");

    assert!(
        !messages.is_empty(),
        "expected at least one NATS message for the order event"
    );
    assert_eq!(
        messages.last().expect("last message").source,
        expected_full_subject,
        "subscriber observes the scenario-namespaced subject, not the bare logical name"
    );
}

#[shakedown::test(resources = [Sqlite, Nats])]
#[ignore = "requires Docker for NATS; run: cargo test -p shakedown-examples --features sqlite,nats,outbox --test appendix_nats -- --ignored"]
async fn sqlite_outbox_row_plus_nats_jetstream_publication_proof(ctx: &mut shakedown::Context) {
    ctx.outbox().setup().await.expect("outbox DDL on SQLite");

    let aggregate_id = "order-nats-outbox-1";
    let payload_json = json!({
        "aggregate_id": aggregate_id,
        "customer_id": "customer-appendix-nats",
        "total": 120.50
    });
    let payload_string = payload_json.to_string();
    let aggregate_id_owned = aggregate_id.to_string();

    ctx.sqlite()
        .execute(
            "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload, status, attempts) \
             VALUES ('order', ?1, 'OrderCreated', ?2, 'pending', 0)",
            (aggregate_id_owned, payload_string.clone()),
        )
        .await
        .expect("insert pending outbox row");

    let base_subject = "orders.events";
    let wire_payload = payload_string.as_bytes();
    ctx.nats()
        .publish(base_subject, wire_payload)
        .await
        .expect("simulate publisher delivering to JetStream");

    ctx.outbox()
        .assert_nats_published(base_subject, aggregate_id)
        .await
        .expect("JetStream retains the message for aggregate_id");
}
