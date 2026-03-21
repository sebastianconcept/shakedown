# Walkthrough — Orders + Payments (overview)

**Purpose:** One entry point for the **Orders + Payments HTTP stub** example. Each path is a **separate doc** so you read only what matches how you want to assert.

**Audience:** Backend engineer adopting Shakedown.

**When to read this:** After [Quickstart](quickstart.md). Pick a path from the table, then open that doc.

**Prerequisites:** Docker, Rust workspace, Shakedown crates. Vocabulary: [Glossary](../glossary.md) (**wire stub**). API names (`#[system_test]`, `TestContext`, …) follow the **released crate**.

---

## The example domain (all paths)

| Piece | Role |
|-------|------|
| **Orders** | Service under test — HTTP API, Payments client, persistence, optional Kafka. |
| **Payments** | **HTTP wire stub** in the lab — deterministic JSON for `POST /v1/charges` — not a full second service on day one. In production, Payments might be **another language** (Go, Python, …); the stub only needs to match what **Orders’ HTTP client** sends and expects on the wire. |

**Shared idea:** `POST /orders` → Orders calls Payments → on success Orders does its job (DB row, Kafka publish, or both). Paths differ only in **what you assert**.

---

## Pick your path

| You want to assert … | Doc |
|----------------------|-----|
| **Database only** — e.g. `orders` row with `status` and `charge_id` | [Database assertion](walkthrough-orders-payments-database.md) |
| **Kafka only** — Orders **produced** the expected record on `orders.events` (you consume in the **test** to verify) | [Kafka producer assertion](walkthrough-orders-payments-kafka.md) |
| **Kafka + database** — a **separate consumer** process reads the topic and **writes a downstream row** (e.g. `fulfillment_jobs`) | [Kafka consumer + database](walkthrough-orders-payments-kafka-consumer-database.md) |

---

## Recommended repository layout

Keep **production code**, **migrations**, and **Shakedown scenarios** in predictable places so new contributors and CI behave the same.

### Workspace shape (example)

Use a **Cargo workspace** with one crate per deployable binary (or library) you start in tests. Put **SQL migrations** in exactly one tree that both prod and the harness apply.

```text
your-repo/
├── Cargo.toml                    # [workspace] members = [...]
├── crates/
│   ├── orders-service/
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── main.rs           # or bin/ if multiple binaries
│   │   │   └── lib.rs            # shared if tests call handlers in-process
│   │   ├── migrations/           # source of truth — same files in prod and scenarios
│   │   └── tests/                # optional: integration tests next to this crate
│   │       └── orders_db.rs
│   └── fulfillment-worker/       # only for Kafka + consumer + DB walkthrough
│       ├── Cargo.toml
│       └── src/
│           └── main.rs
├── integration-tests/            # optional: dedicated workspace member for all system tests
│   ├── Cargo.toml
│   └── tests/
│       ├── orders_database.rs
│       ├── orders_kafka.rs
│       ├── orders_kafka_consumer_db.rs
│       └── support/              # shared helpers — not published
│           └── mod.rs
└── docs/                         # project docs (or symlink to external doc repo)
```

**Pick one pattern** and stick to it:

| Pattern | When it fits |
|---------|----------------|
| **`crates/<service>/tests/*.rs`** | Scenarios mostly about **one** service; shortest path from code to test. |
| **`integration-tests/` crate** | Many services, shared `support/`, one `cargo test -p integration-tests`; keeps app crates slimmer. |

Do **not** mix both for the same scenario without a team rule — duplicates confuse ownership.

### Rules of thumb

1. **Migrations** — live under the **owning** service (`crates/orders-service/migrations/`). The harness runs the **same** paths the binary uses in prod.
2. **Test-only helpers** — under `tests/support/` or `integration-tests/tests/support/` — never `src/` unless it is a real test-only crate.
3. **Binaries under test** — `target/release/orders-service` (or `cargo run -p orders-service`) from the same workspace; document the path in one README.
4. **Future YAML scenarios** — e.g. `integration-tests/scenarios/*.yaml` next to the runner, **or** `scenarios/` at repo root — one directory, referenced by CLI and docs.
5. **Secrets** — not in git; use CI secrets and local env only. Commit `.env.example` with **placeholder** keys if useful.

The three walkthroughs assume paths like `tests/orders_*.rs` — map those names to your chosen layout.

---

## Refresher

- Same **Payments stub** contract across paths; only **resources asserted** and **processes spawned** change.
- Start with **one** assertion surface per scenario; add paths as the team needs confidence.
- **Directories:** one migration tree per service; one integration-test layout (`crate-local` **or** `integration-tests/` crate) — see [Recommended repository layout](#recommended-repository-layout) above.
