# Shakedown examples (`shakedown-examples`)

Progressive **book-style** scenarios for learning Shakedown: order and fulfillment companion services, six parts (28 chapters), plus resource appendices.

## Layout

| Path | Role |
|------|------|
| `src/lib.rs` | Shared DDL, sample payloads, domain types |
| `tests/ch*.rs` | Chapters (feature-gated integration tests) |
| `tests/appendix_*.rs` | Reference cards per engine |

Chapter 1 runs **without Docker** (SQLite). Most other targets are `#[ignore]` until you enable them and have Docker (and locally built service images for Parts II–VI).

## Quick start

```bash
# Non-Docker chapters (from repo root)
just test-examples

# Build Order + Fulfillment images (requires Docker)
just build-example-services
```

See the workspace `justfile` for part-by-part recipes (`test-examples-part1`, …).

## Workspace crates

- **`example-order-service`** — Postgres, Redis, Kafka, optional outbox; HTTP API for orders.
- **`example-fulfillment-service`** — MongoDB, RabbitMQ, Kafka consumer; HTTP API for fulfillments.

Image names: `example-order-service:latest`, `example-fulfillment-service:latest`.
