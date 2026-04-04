# Order Service

Minimal Axum service for **Shakedown book** chapters (Parts II+): Postgres orders, Redis cache, Kafka producer (or transactional outbox), optional call to Fulfillment.

## Reading order (source)

1. [`src/lib.rs`](src/lib.rs) — `run()` entry
2. [`src/config.rs`](src/config.rs) — environment
3. [`src/error/`](src/error/) — `ConfigError` → `StartupError` → `ServiceError`
4. [`src/state.rs`](src/state.rs) — wires [`OrderRepository`](src/repository/order_repository.rs), [`OrderCache`](src/cache/order_cache.rs), [`OrderMessaging`](src/messaging/order_messaging.rs)
5. [`src/http/handlers.rs`](src/http/handlers.rs) — Axum; uses [`OrderApiError`](src/http/api_error.rs) + `IntoResponse`
6. [`src/kafka_topic.rs`](src/kafka_topic.rs) — `KAFKA_TOPIC_ORDERS` (`orders`)

## Environment

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `DATABASE_URL` | yes | — | Postgres connection |
| `REDIS_URL` | yes | — | Redis cache |
| `KAFKA_BOOTSTRAP_SERVERS` | yes | — | Kafka (when not using outbox-only path for produce) |
| `PORT` | no | `3000` | Listen port |
| `FULFILLMENT_GRPC_URI` | no | — | Tonic plaintext URI (e.g. `http://fulfillment:50051`) for sync `Fulfill` RPC when handling `POST /orders/{id}/fulfill` |
| `OUTBOX_ENABLED` | no | `false` | `true` / `1` writes `outbox_event` instead of direct Kafka produce |

## Part V+ compatibility

Same HTTP contract and env names when tests swap standalone Postgres/Redis/Kafka for **cluster** Shakedown resources; only connection strings and bootstrap URLs change.

See also: [`crates/examples/README.md`](../examples/README.md) (chapter ↔ evidence table).
