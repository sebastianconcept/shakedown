# Walkthrough — Orders + Payments (Kafka producer assertion)

**Purpose:** One green scenario where **the proof is a Kafka record** — Orders **produced** the expected payload on `orders.events` after a successful flow. The test **consumes** from the topic to verify (the **test harness** is the consumer for this assertion).

**When to use:** Your team’s risk is **“nothing was published”** or **wrong payload on the bus** — not (yet) a separate downstream service’s processing.

**Prerequisites:** [Walkthrough overview](walkthrough-orders-payments.md), [Quickstart](quickstart.md), [Glossary](../glossary.md).

**Repository layout:** [Recommended repository layout](walkthrough-orders-payments.md#recommended-repository-layout) — same workspace rules; name test files e.g. `crates/orders-service/tests/orders_kafka.rs` or `integration-tests/tests/orders_kafka.rs`.

---

## Glance

| You start | You assert |
|-----------|------------|
| Kafka (container), DB if Orders needs it for the flow, Payments stub, Orders | **Exactly one** message on `orders.events` with expected `order_id` and `charge_id` |

This is **not** the doc where a **second** long-running consumer process writes to a table — see [Kafka consumer + database](walkthrough-orders-payments-kafka-consumer-database.md).

```mermaid
flowchart LR
    subgraph lab [Scenario]
        O[Orders]
        P[Payments stub]
        K[(Kafka)]
        D[(Database)]
    end
    O --> P
    O --> D
    O --> K
```

```mermaid
sequenceDiagram
    participant C as Client / test
    participant O as Orders
    participant S as Payments stub
    participant K as Kafka
    C->>O: POST /orders
    O->>S: POST /v1/charges
    S-->>O: 201 + charge_id
    O->>K: produce order.created
    Note over C,K: Test consumes topic to assert payload
```

---

## Step 1 — Contracts

**Event payload (JSON)**

```json
{
  "event": "order.created",
  "order_id": "ord_01HQXYZ",
  "charge_id": "chg_test_1",
  "occurred_at": "2025-03-21T12:00:00Z"
}
```

**Payments stub** — same as other walkthroughs: `POST /v1/charges` → `{ "charge_id": "chg_test_1", "status": "captured" }`.

---

## Step 2 — Config

```bash
export PAYMENTS_BASE_URL="http://127.0.0.1:9XXX"
export DATABASE_URL="postgres://orders:orders@127.0.0.1:5XXX/orders_m001"
export KAFKA_BOOTSTRAP="127.0.0.1:3XXX"
export ORDERS_HTTP_BIND="127.0.0.1:8080"
```

---

## Step 3 — Harness + test crate

**Resources:** Kafka + Payments stub + Postgres (if Orders persists before publish). Topic `orders.events` created or namespaced per scenario.

```toml
[dev-dependencies]
shakedown = { version = "0.1", features = ["kafka", "postgres", "stub-http"] }
shakedown-macros = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
serde_json = "1"
```

---

## Step 4 — Drive

Same `POST /orders` as [database walkthrough](walkthrough-orders-payments-database.md#step-5--drive); capture `order_id` from the response.

---

## Step 5 — Assertion (Kafka)

Consume from `orders.events` (or use harness helpers) and assert **exactly one** record with the expected fields.

```rust
let records = ctx
    .kafka()
    .consume_json("orders.events", /* bounds / timeout */)
    .await?;

assert_eq!(records.len(), 1, "expected exactly one order.created, got {records:?}");
let payload = &records[0].value;
assert_eq!(payload["order_id"].as_str(), Some(order_id));
assert_eq!(payload["charge_id"].as_str(), Some("chg_test_1"));
```

---

## Step 6 — Teardown + repeat

Stop Orders, tear down stub and containers. Run the test **twice**.

---

## Understand

- **Producer vs consumer:** Here **Orders** is the **producer**. The **consumer** in the assertion is **your test** (or harness helper) reading the topic. **Do not** confuse this with a separate **Fulfillment** worker — that is [Kafka consumer + database](walkthrough-orders-payments-kafka-consumer-database.md).
- **Database:** You may still need migrations so Orders can complete its flow; this scenario simply **does not assert** on SQL.

---

## Refresher card

- **Assert:** one Kafka message with expected `order_id` + `charge_id`.
- **Not covered:** downstream consumer process or DB writes after consume.
