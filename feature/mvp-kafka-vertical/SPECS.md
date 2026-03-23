# Technical specifications — `mvp-kafka-vertical` (implementation notes)

This file tracks **implementation decisions** that complement the adopted product SPECS in the docs corpus.

## Suite-level Kafka broker (D-3) — resolved

**Decision:** Use a process-wide `std::sync::OnceLock<tokio::sync::Mutex<SuiteBrokerState>>` in `crates/lib/src/resources.rs` (`kafka_impl`), not a separate `DockerEnvironmentBackend` type.

**Rationale:**

- `KafkaResource` already owns the testcontainers start/stop path; lifting containers into `EnvironmentBackend` would duplicate configuration (image, tag) across two layers without a clear win for this milestone.
- A `Mutex` around `Idle | Running { container, bootstrap, refs, image_key }` gives **reference-counted** sharing: multiple [`KafkaResource`](../../crates/lib/src/resources.rs) instances in one process reuse one broker until the **last** [`Resource::teardown`](../../crates/lib/src/resources.rs) runs `ContainerAsync::rm`, satisfying D-3 and US-003 teardown requirements on provision/readiness/seed failure paths (via existing `run_scenario` → `teardown_all_resources`).
- **Image identity:** the first successful start pins `image:tag` (`apache::Kafka` name + optional [`KafkaResource::with_image_tag`](../../crates/lib/src/resources.rs)). A later holder with a different fingerprint returns [`InfrastructureError::Backend`](../../crates/lib/src/error.rs).

**Not chosen:** `static OnceLock<Container>` without refcount (would prevent `rm` between sequential integration tests in the same process).

## KRaft / no ZooKeeper (D-1)

Default path uses `testcontainers_modules::kafka::apache::Kafka` (see upstream module docs): **Apache Kafka with KRaft**, default image `apache/kafka-native`, optional JVM image via [`KafkaResource::with_jvm_image`](../../crates/lib/src/resources.rs). No Confluent + ZooKeeper module is used in the default API surface.

## Panics in the scenario `run` phase

If user scenario code panics after provision, `run_scenario` does not run `teardown_all_resources` today. US-003 covers success and **infrastructure-phase** failure paths; panic safety remains a follow-up.
