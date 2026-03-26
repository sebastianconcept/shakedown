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
# Fast local loop
# ----------------------------

test-lib:
  cargo test -p shakedown

test-macros:
  cargo test -p shakedown-macros

test-outbox-sqlite:
  cargo test -p shakedown --features outbox,sqlite --test outbox_sqlite

# ----------------------------
# Docker-backed ignored tests
# ----------------------------

test-kafka-ignored:
  cargo test -p shakedown --features kafka --test macro_kafka -- --ignored

test-postgres-ignored:
  cargo test -p shakedown --features postgres --test postgres_vertical -- --ignored

test-outbox-postgres-ignored:
  cargo test -p shakedown --features outbox,postgres,kafka --test outbox_postgres -- --ignored
