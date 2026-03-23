# Verification — `mvp-kafka-vertical`

## Kafka + Docker (integration)

Docker-backed tests live in [`crates/lib/tests/kafka_docker.rs`](../../crates/lib/tests/kafka_docker.rs). They are **`#[ignore = "requires Docker"]`** so default `cargo test` stays free of Docker.

Run when Docker is available:

```bash
cargo test -p shakedown --features kafka --test kafka_docker -- --ignored
```

What they cover:

- Suite broker reuse and refcounted teardown (provision → teardown → provision again).
- Two [`KafkaResource`](../../crates/lib/src/resources.rs) holders sharing one bootstrap address.
- [`KafkaResource::with_image_tag`](../../crates/lib/src/resources.rs) (production-parity pin).
- [`run_scenario`](../../crates/lib/src/lifecycle.rs) readiness failure still tears down Kafka so a later scenario can start a fresh broker.

## CI matrix (recommended)

- Job with Docker socket mounted: `cargo test --workspace` and `cargo test --workspace --features kafka` (ignored tests optional: add `-- --ignored` on the `kafka_docker` target only).

## Environment

- **`SHAKEDOWN_KAFKA_IMAGE_TAG`:** optional tag override; call [`KafkaResource::with_image_tag_from_env`](../../crates/lib/src/resources.rs) when building the resource (see [`crates/lib/README.md`](../../crates/lib/README.md)).
