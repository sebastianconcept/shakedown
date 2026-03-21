# Walkthrough — Orders + Payments (database assertion)

**Purpose:** One green scenario where **the proof is a database row** — e.g. `orders` with `status` and `external_charge_id` after `POST /orders` and a successful Payments charge.

**When to use:** Your team’s risk is **wrong or missing persistence** (transactions, migrations, outbox adjacent to the row, etc.). You are **not** proving Kafka content in this scenario.

**Prerequisites:** [Walkthrough overview](walkthrough-orders-payments.md), [Quickstart](quickstart.md), [Glossary](../glossary.md).

**Repository layout:** Follow [Recommended repository layout](walkthrough-orders-payments.md#recommended-repository-layout) on the hub — migrations under `crates/orders-service/migrations/`, integration tests either next to that crate or in a dedicated `integration-tests/` member.

---

## Glance

| You start | You assert |
|-----------|------------|
| Postgres or MariaDB (container), Payments **HTTP stub**, Orders process | **Exactly one** row in `orders` for the expected `order_id`, with `status` and `external_charge_id` matching the stub |

**Kafka:** If your Orders binary **requires** a broker to start, provision Kafka in the harness but **do not assert** on topics in this doc. If you can run Orders with Kafka disabled in config for this test, skip the Kafka container.

```mermaid
flowchart LR
    subgraph lab [Scenario]
        O[Orders]
        P[Payments stub]
        D[(Database)]
    end
    O --> P
    O --> D
```

```mermaid
sequenceDiagram
    participant C as Client / test
    participant O as Orders
    participant S as Payments stub
    participant D as Database
    C->>O: POST /orders
    O->>S: POST /v1/charges
    S-->>O: 201 + charge_id
    O->>D: INSERT / UPDATE orders
```

---

## Step 1 — Contracts

**Payments stub — request / response**

```http
POST /v1/charges HTTP/1.1
Content-Type: application/json

{ "order_id": "ord_01HQXYZ", "amount_cents": 1000 }
```

```json
{ "charge_id": "chg_test_1", "status": "captured" }
```

**Database — illustrative**

```sql
CREATE TABLE orders (
  id                TEXT PRIMARY KEY,
  status            TEXT NOT NULL,
  external_charge_id TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## Step 2 — Config

Orders must read **only** `PAYMENTS_BASE_URL` and `DATABASE_URL` from env (no `#[cfg(test)]` in domain code). Example:

```bash
export PAYMENTS_BASE_URL="http://127.0.0.1:9XXX"
export DATABASE_URL="postgres://orders:orders@127.0.0.1:5XXX/orders_m001"
export ORDERS_HTTP_BIND="127.0.0.1:8080"
```

---

## Step 3 — Harness + test crate

1. Integration test target (e.g. `tests/orders_db.rs`).
2. **Resources:** DB + Payments stub; Kafka **only if** required to boot Orders.
3. **Dependencies:** `shakedown`, `shakedown-macros`, `postgres` (or MariaDB) feature, `stub-http`, `sqlx` or your DB client.

```toml
[dev-dependencies]
shakedown = { version = "0.1", features = ["postgres", "stub-http"] }
shakedown-macros = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres"] }
```

---

## Step 4 — `#[system_test]` skeleton

1. Start DB + run migrations.
2. Start Payments stub (`chg_test_1`).
3. Spawn Orders with env from `TestContext`.
4. Wait for `/health`.
5. `POST /orders` (see [drive request](#step-5--drive) below).
6. **Assert SQL** (next section).

---

## Step 5 — Drive

```bash
curl -sS -X POST "http://127.0.0.1:8080/orders" \
  -H 'Content-Type: application/json' \
  -d '{"sku":"demo-sku","amount_cents":1000}'
```

```rust
let client = reqwest::Client::new();
let resp = client
    .post("http://127.0.0.1:8080/orders")
    .json(&serde_json::json!({ "sku": "demo-sku", "amount_cents": 1000 }))
    .send()
    .await?;
assert!(resp.status().is_success());
let created: serde_json::Value = resp.json().await?;
let order_id = created["order_id"].as_str().expect("order_id");
```

---

## Step 6 — Assertion (database)

```rust
let row: (String, String) = sqlx::query_as(
    "SELECT status, external_charge_id FROM orders WHERE id = $1",
)
.bind(order_id)
.fetch_one(ctx.postgres_pool())
.await?;

assert_eq!(row.0, "confirmed");
assert_eq!(row.1, "chg_test_1");
```

---

## Step 7 — Teardown + repeat

Stop Orders, tear down stub and DB. Run the test **twice** locally.

---

## Understand

- **Why not assert Kafka here:** This scenario isolates **relational** correctness. Add [Kafka producer](walkthrough-orders-payments-kafka.md) or [Kafka + consumer DB](walkthrough-orders-payments-kafka-consumer-database.md) when you need bus-level proof.

---

## Refresher card

- **Assert:** one row in `orders` with expected `status` + `external_charge_id`.
- **Kafka:** optional resource; not part of the assertion in this doc.
