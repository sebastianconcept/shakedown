# Shakedown — Orchestration library for shakedown testing

**Shakedown** is a Rust framework for shakedown testing — proving systems are safe to
promote to production by running realistic scenarios that exercise the full stack.

## Overview

This library provides:

- **Scenario orchestration** — lifecycle phases: provision → readiness → seed → run → assert → teardown
- **Resource providers** — async [`Resource`] trait for databases, message queues, and other infrastructure
- **Error classification** — structured errors distinguishing scenario vs infrastructure failures
- **Event logging hooks** — structured event collection for failure timelines

## Workspace layout

```text
shakedown/
├── crates/
│   ├── lib/          # this crate: orchestration engine
│   ├── macros/       # procedural macros (shakedown-macros)
│   └── cli/          # CLI binary (shakedown-cli)
```

## Extension points

- **[`Resource`] trait** — implement for new infrastructure (with `async_trait` for `Box<dyn Resource>` extensions)
- **[`EnvironmentBackend`] trait** — implement for alternative execution environments (also `async_trait` for `dyn` use)
- **`Context::register_builtin` / `register_extension`** — wire first-party or third-party resources

## Milestone status

- **Current**: `mvp-macros-system-test` — procedural macros `#[shakedown::test]` and `#[shakedown::faulty_test]` for test discovery. Adds `ScenarioKind` enum to distinguish normal vs faulty scenarios. See [`crates/macros/README.md`](../macros/README.md).
- **Next**: `mvp-postgres-mariadb` — Postgres and MariaDB resources

Default `cargo test -p shakedown` does not require Docker.

**Kafka (KRaft, `testcontainers-modules::kafka::apache`):**

- [`KafkaResource`](src/resources.rs) uses a **suite-level** broker: the first provision in a process starts one container; further `KafkaResource` instances share it until the last `Resource::teardown` removes the container (see adopted docs `SPECS-IMPLEMENTATION.md`, linked from [`feature/mvp-kafka-vertical/README.md`](../../feature/mvp-kafka-vertical/README.md)).
- **Image tag:** [`KafkaResource::with_image_tag`](src/resources.rs), or set `SHAKEDOWN_KAFKA_IMAGE_TAG` and use [`KafkaResource::with_image_tag_from_env`](src/resources.rs).
- **JVM image:** [`KafkaResource::with_jvm_image`](src/resources.rs) (`apache/kafka` instead of `apache/kafka-native`).

### KafkaBindings (US-004)

[`KafkaBindings`](src/resources.rs) exposes structured bindings on [`Context`]:

```rust
#[cfg(feature = "kafka")]
{
    let mut ctx = Context::new();
    ctx.register_builtin(BuiltinResource::Kafka(KafkaResource::new()));
    
    // After provision in run_scenario(), KafkaBindings are set automatically:
    if let Some(bindings) = ctx.kafka_bindings() {
        println!("Bootstrap: {}", bindings.bootstrap_servers);
        println!("Topic prefix: {}", bindings.topic_prefix);
        
        // Use bindings to construct an rdkafka client:
        // let client = ClientConfig::new()
        //     .set("bootstrap.servers", &bindings.bootstrap_servers)
        //     .set("client.id", format!("{}client", bindings.client_id_prefix.as_ref().unwrap_or(&"")))
        //     .create()?;
    }
}
```

Topics are created in `apply_default_seed` phase under the scenario's isolated namespace.
See [`KafkaResource::apply_default_seed`](src/resources.rs) and [DECISIONS.md D-4](../../DECISIONS.md) for details.

Integration tests that start Kafka are `#[ignore]` unless Docker is available:

`cargo test -p shakedown --features kafka --test kafka_docker -- --ignored`

Verification steps: adopted docs `VERIFY.md` (see [`feature/mvp-kafka-vertical/README.md`](../../feature/mvp-kafka-vertical/README.md) for the path under `~/docs/shakedown/`).

## See also

Product docs (PRD, SPECS, TECH-STACK) live in the adopted documentation root — workspace [`docs/README.md`](../../docs/README.md) points at **`~/docs/shakedown/`**.
