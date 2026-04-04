set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default: help

# Show available recipes.
help:
  @just --list

# Install cargo-nextest locally (recommended for faster test runs).
install-nextest:
  cargo install cargo-nextest --locked

# ----------------------------
# Core quality loop
# ----------------------------

fmt:
  cargo fmt --all

fmt-check:
  cargo fmt --all -- --check

check:
  cargo check --workspace

clippy:
  cargo clippy --workspace --all-targets -- -D warnings

# Workspace tests: uses **nextest** when `cargo-nextest` is on PATH; otherwise plain `cargo test`.
# For Cargo parity without nextest, use `just test-workspace`.
test:
  if command -v cargo-nextest >/dev/null 2>&1; then \
    cargo nextest run --workspace; \
  else \
    echo "cargo-nextest not installed; run 'just install-nextest' for faster runs. Falling back to cargo test --workspace." >&2; \
    cargo test --workspace; \
  fi

qa: fmt-check check clippy test

qa-full: fmt-check check clippy
  cargo test --workspace

# ----------------------------
# Workspace test runs (all crates: lib, macros)
# ----------------------------

# Same as `cargo test --workspace` (always plain Cargo). Use `just test` if you want nextest when installed.
test-workspace:
  cargo test --workspace

# Every optional `shakedown` module at once. Enables `no-diagnostics`, so bollard diagnostics tests are not compiled.
test-workspace-all-features:
  cargo test --workspace --all-features

# ----------------------------
# `shakedown` library — fast local loop
# ----------------------------

test-lib:
  cargo test -p shakedown

test-macros:
  cargo test -p shakedown-macros

test-outbox-sqlite:
  cargo test -p shakedown --features outbox,sqlite --test outbox_sqlite

# ----------------------------
# `shakedown` — load / performance (`load` feature)
# ----------------------------

# Open-model load driver, HdrHistogram metrics, warmup, JSON reports, baselines, performance assertions,
# and `run_scenario` + failure-timeline wiring. Runs `#[cfg(feature = "load")]` unit tests plus
# `load_handle` and `load_performance_us008` integration binaries (no Docker).
check-load:
  cargo check -p shakedown --features load --all-targets

clippy-load:
  cargo clippy -p shakedown --features load --all-targets -- -D warnings

# Full load feature test run; uses nextest when `cargo-nextest` is on PATH (same idea as `just test`).
test-load:
  if command -v cargo-nextest >/dev/null 2>&1; then \
    cargo nextest run -p shakedown --features load; \
  else \
    echo "cargo-nextest not installed; falling back to cargo test." >&2; \
    cargo test -p shakedown --features load; \
  fi

# Quick smoke: open-model scheduling + event log merge (`tests/load_handle.rs` only).
test-load-handle:
  cargo test -p shakedown --features load --test load_handle

# Performance stack integration: SLO assertions, JSON reports, baselines, regression checks, and failure-timeline JSON.
test-load-performance:
  cargo test -p shakedown --features load --test load_performance_us008

# Every integration feature, every test target, including `#[ignore]` (needs Docker for ignored tests). Does not enable `no-diagnostics`, so bollard diagnostics tests run. Uses nextest when installed (same idea as `just test`).
test-all-include-ignored:
  if command -v cargo-nextest >/dev/null 2>&1; then \
    cargo nextest run -p shakedown --features "kafka,postgres,mariadb,mysql,outbox,rabbitmq,sqlite,fault-injection" -- --include-ignored; \
  else \
    echo "cargo-nextest not installed; falling back to cargo test." >&2; \
    cargo test -p shakedown --features "kafka,postgres,mariadb,mysql,outbox,rabbitmq,sqlite,fault-injection" -- --include-ignored; \
  fi

# ----------------------------
# Docker-backed `#[ignore]` tests (one recipe per integration binary)
# ----------------------------

test-kafka-ignored:
  cargo test -p shakedown --features kafka --test macro_kafka -- --ignored

test-postgres-ignored:
  cargo test -p shakedown --features postgres --test postgres_vertical -- --ignored

test-mariadb-ignored:
  cargo test -p shakedown --features mariadb --test mariadb_vertical -- --ignored

test-mysql-ignored:
  cargo test -p shakedown --features mysql --test mysql_vertical -- --ignored

test-outbox-postgres-ignored:
  cargo test -p shakedown --features outbox,postgres,kafka --test outbox_postgres -- --ignored

test-outbox-rabbit-ignored:
  cargo test -p shakedown --features outbox,postgres,rabbitmq --test outbox_rabbit -- --ignored

# Bollard log stream / Docker events / stats rings (requires Docker; do not combine with `--all-features` on `shakedown`, which sets `no-diagnostics`).
test-diagnostics-integration-ignored:
  cargo test -p shakedown --features kafka --test diagnostics_integration -- --ignored

test-toxiproxy-ignored:
  cargo test -p shakedown --features fault-injection --test toxiproxy_vertical -- --ignored

test-fault-injection-kafka-ignored:
  cargo test -p shakedown --features kafka,fault-injection --test fault_injection_kafka -- --ignored

# MongoDB replica set under open-model load while forcing a primary step-down (Docker; `#[ignore]` by default).
test-mongo-cluster-load-disruption-ignored:
  cargo test -p shakedown --features mongodb-cluster,load --test mongo_cluster_load_disruption -- --ignored

# ----------------------------
# Example companion services + `shakedown-examples` crate
# ----------------------------

# Build example companion service Docker images (workspace root; uses root Dockerfiles).
build-example-services:
  docker build -f Dockerfile.example-order-service -t example-order-service:latest .
  docker build -f Dockerfile.example-fulfillment-service -t example-fulfillment-service:latest .

# Non-Docker examples only (ch01 + appendix_duckdb when un-ignored).
test-examples:
  cargo test -p shakedown-examples --features sqlite,duckdb,outbox

# All Docker-backed examples (`#[ignore]` targets).
test-examples-ignored:
  cargo test -p shakedown-examples --all-features -- --ignored

# Part I: Foundations (Docker infra only, no service images).
test-examples-part1:
  cargo test -p shakedown-examples --features sqlite,kafka,postgres,redis,rabbitmq,mongodb,mock-services,grpc-mock-services -- --ignored \
    ch01 ch02 ch03 ch04 ch05 ch06 ch07 ch08

# Part II: One service under test (Order Service image).
test-examples-part2:
  cargo test -p shakedown-examples --features services,postgres,redis,kafka,rabbitmq,outbox,mock-services -- --ignored \
    ch09 ch10 ch11 ch12 ch13

# Part III: Two services under test (both images).
test-examples-part3:
  cargo test -p shakedown-examples --features services,postgres,mongodb,kafka,rabbitmq,redis,mock-services -- --ignored \
    ch14 ch15 ch16

# Part IV: Fault injection + diagnostics.
test-examples-part4:
  cargo test -p shakedown-examples --features services,postgres,mongodb,kafka,redis,fault-injection,diagnostics -- --ignored \
    ch17 ch18

# Part V: Cluster topologies.
test-examples-part5:
  cargo test -p shakedown-examples --features services,mongodb-cluster,kafka-cluster,redis-sentinel,postgres-cluster,rabbitmq-cluster -- --ignored \
    ch19 ch20 ch21 ch22 ch23

# Part VI: Load, soak + resilience.
test-examples-part6:
  cargo test -p shakedown-examples --features services,postgres,kafka,redis,mongodb-cluster,load,fault-injection -- --ignored \
    ch24 ch25 ch26 ch27 ch28

# Full suite (all chapters + appendices).
test-examples-full:
  cargo test -p shakedown-examples --all-features -- --ignored
