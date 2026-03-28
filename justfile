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
# Workspace test runs (all crates: lib, macros, CLI)
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

# Thin CLI crate; add `[[test]]` targets here as the binary grows.
test-cli:
  cargo test -p shakedown-cli

test-outbox-sqlite:
  cargo test -p shakedown --features outbox,sqlite --test outbox_sqlite

# Every integration feature, every test target, including `#[ignore]` (needs Docker for ignored tests). Does not enable `no-diagnostics`, so bollard diagnostics tests run. Uses nextest when installed (same idea as `just test`).
test-shakedown-all-include-ignored:
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
