# Fulfillment Service

Minimal Axum service for **Shakedown book** Parts II–III: MongoDB fulfillments, RabbitMQ publish, **Kafka consumer** on `orders`, optional payment gateway mock.

## Reading order (source)

1. [`src/lib.rs`](src/lib.rs) — `run()`, spawns Kafka consumer task + **gRPC** server (tonic)
2. [`src/config.rs`](src/config.rs)
3. [`src/error/`](src/error/) — `ConfigError` → `StartupError` → `ServiceError`
4. [`src/state.rs`](src/state.rs) — [`FulfillmentRepository`](src/repository/fulfillment_repository.rs), Rabbit connection
5. [`src/fulfillment.rs`](src/fulfillment.rs) — idempotent create ([`idempotency`](../idempotency)), shared by gRPC + HTTP + Kafka
6. [`src/kafka_consumer.rs`](src/kafka_consumer.rs) — consumer loop (supervision: logs errors, does not crash HTTP)
7. [`src/http/handlers.rs`](src/http/handlers.rs) — `201` on create, `200` on idempotent replay

## Environment

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `MONGODB_URI` | yes | — | MongoDB connection |
| `RABBITMQ_URL` | yes | — | AMQP |
| `KAFKA_BOOTSTRAP_SERVERS` | yes | — | Consumer bootstrap |
| `PORT` | no | `3001` | HTTP (Axum) listen port |
| `GRPC_PORT` | no | `50051` | gRPC (`book.fulfillment.v1.Fulfillment`) listen port |
| `PAYMENT_GATEWAY_URL` | no | — | Optional mock charge URL |

## Kafka topic

Subscribes to **`orders`** ([`kafka_topic::KAFKA_TOPIC_ORDERS`](src/kafka_topic.rs)), matching the order service producer and chapter tests.

## Idempotency

At-least-once Kafka delivery: duplicate messages for the same `order_id` **do not** insert twice; gRPC `Fulfill` and HTTP `POST /fulfill` use the same path. Shared decision types live in [`idempotency`](../idempotency).

See also: [`crates/examples/README.md`](../examples/README.md).
