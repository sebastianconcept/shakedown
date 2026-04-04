# `idempotency`

Small, dependency-free types for the **Shakedown book** examples. Encodes the idempotency **decision** (`Proceed` vs already handled) so **Kafka** and **HTTP** entry points in `fulfillment-service` share the same rule without duplicating logic.

- **Crate:** [`src/lib.rs`](src/lib.rs) — [`IdempotentDecision<T>`](src/lib.rs)
- **Consumers:** `fulfillment-service` (not the main `shakedown` harness crate)

Run unit tests:

```bash
cargo test -p idempotency
```
