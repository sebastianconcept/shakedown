# `shared-grpc`

Tonic + prost **codegen** for gRPC contracts used across the book’s example services.

This crate is intentionally **not** named after Fulfillment: add more `.proto` files under
`proto/book/…`, list them in `build.rs`, and depend on `shared-grpc` from any service that
needs those types (Order, Fulfillment, or future examples).

| Package | Role |
|---------|------|
| `book.fulfillment.v1` | Unary sync handoff Order → Fulfillment (`Fulfill`). |
| `book.demo.v1` | Small second API (`Demo` / `Ping`) to show **multiple** contracts in one crate. |

Re-exports at the crate root include clients, servers, messages, and method path constants
such as [`FULFILL_METHOD_PATH`](src/lib.rs) and [`DEMO_PING_METHOD_PATH`](src/lib.rs) for mocks.
