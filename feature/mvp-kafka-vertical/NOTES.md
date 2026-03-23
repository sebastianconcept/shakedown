# Execution notes — `mvp-kafka-vertical`

## 2026-03-23 — US-003 Docker / suite Kafka

- Implemented **suite-level** Kafka via `OnceLock` + `Mutex` + ref-count in `kafka_impl`; last `teardown` calls `ContainerAsync::rm`.
- Added [`KafkaResource::with_image_tag_from_env`](../../crates/lib/src/resources.rs) for `SHAKEDOWN_KAFKA_IMAGE_TAG`.
- Added ignored integration tests [`crates/lib/tests/kafka_docker.rs`](../../crates/lib/tests/kafka_docker.rs); documented in [`VERIFY.md`](VERIFY.md).
- Recorded broker lifecycle decision in [`SPECS.md`](SPECS.md).

Quality gates (local): `cargo fmt --check`, `cargo clippy --workspace --all-targets --features kafka -- -D warnings`, `cargo test --workspace`, `cargo test --workspace --features kafka`.
