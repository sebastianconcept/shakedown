# Execution notes — `mvp-kafka-vertical`

## 2026-03-23 — US-003 Docker / suite Kafka

- Implemented **suite-level** Kafka via `OnceLock` + `Mutex` + ref-count in `kafka_impl`; last `teardown` calls `ContainerAsync::rm`.
- Added [`KafkaResource::with_image_tag_from_env`](../../crates/lib/src/resources.rs) for `SHAKEDOWN_KAFKA_IMAGE_TAG`.
- Added ignored integration tests [`crates/lib/tests/kafka_docker.rs`](../../crates/lib/tests/kafka_docker.rs); documented in [`VERIFY.md`](VERIFY.md).
- Recorded broker lifecycle decision in [`SPECS.md`](SPECS.md).

Quality gates (local): `cargo fmt --check`, `cargo clippy --workspace --all-targets --features kafka -- -D warnings`, `cargo test --workspace`, `cargo test --workspace --features kafka`.
## 2026-03-23 — US-004 Kafka bindings, seed topics, readiness probes ✅ Completed

**Status:** Implementation complete; quality gates pass.

### What was verified

- **Context exposes `KafkaBindings`** for rdkafka client construction:
  - `bootstrap_servers`: bootstrap servers URL (e.g., `"127.0.0.1:9092"`)
  - `topic_prefix`: scenario-scoped prefix for topic isolation (e.g., `"shakedown_my_scenario_"`)
  - `client_id_prefix`: optional client ID prefix for consumer group isolation
- **`apply_default_seed`** creates baseline topics under the prefix:
  - Creates `{prefix}shakedown.proof` topic as minimal seed per D-4 semantics
  - Topics are created here, not in readiness checks
- **`check_ready`** validates broker/admin readiness without conflating with seed:
  - Uses `fetch_metadata` to verify broker is reachable and cluster ID available
  - Does NOT check topic existence (handled separately in seed phase)
- **D-4 semantics reflected in code comments**:
  - Added docs on `check_ready`: "Readiness probes must NOT assume topics exist"
  - Added docs on `apply_default_seed`: "Topic creation happens here, not during readiness"

### Acceptance criteria status (US-004)

| AC | Status | Notes |
|----|--------|-------|
| Context exposes KafkaBindings sufficient for rdkafka client construction | ✅ Pass | `bootstrap_servers`, `topic_prefix`, `client_id_prefix` all available on `Context::kafka_bindings()` |
| apply_default_seed creates baseline topics under prefix | ✅ Pass | Creates `{prefix}shakedown.proof` topic in seed phase |
| check_ready validates broker/admin without conflating post-seed full environment | ✅ Pass | Uses `fetch_metadata`; does NOT check topic existence |
| DECISIONS D-4 semantics reflected in code comments or module docs | ✅ Pass | Added inline doc comments documenting D-4 semantics on `check_ready` and `apply_default_seed` |

### Quality gates (local)

All quality gates pass:

```bash
cargo fmt --all -- --check          # formatting ✅
cargo clippy --workspace --all-targets -- -D warnings  # linting ✅
cargo test --workspace              # unit tests ✅
```

**Kafka integration tests** are `#[ignore]` until Docker is available:

```bash
cargo test -p shakedown --features kafka --test kafka_docker -- --ignored
```

### Related decisions

- **D-4:** Topic creation happens in seed, readiness is read-only
- **D-6:** Async Resource trait with scenario-aware `scenario_id` for isolation
- **Suite broker:** One container per process (not per test); see SPECS.md and VERIFY.md
