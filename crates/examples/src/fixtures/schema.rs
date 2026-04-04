//! SQL DDL snippets shared by book tests (Postgres, SQLite) and outbox alignment notes.
//!
//! ## Which constant to use
//!
//! - **[`ORDERS_SCHEMA_SQL`]** — Parts II+ when the scenario uses **Postgres** (`postgres` feature) and seeds the same `orders` table as the `order-service` binary.
//! - **[`ORDERS_SCHEMA_SQL_SQLITE`]** — Part I ch01 (no Docker); SQLite file or memory.
//! - **[`OUTBOX_SCHEMA_SQL`]** — ch12 and outbox appendices; must stay aligned with `crates/lib` outbox DDL so `Outbox::setup` and row assertions match.
//! - **Appendices J / K (MariaDB / MySQL)** — use the engine-specific setup in those tests; the conceptual `orders` shape matches these DDLs where the book shows relational assertions.

/// Postgres `orders` table DDL (Parts II+).
pub const ORDERS_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS orders (
    id          TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    items       TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'created',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"#;

/// Postgres `outbox_event` DDL — must match `POSTGRES_SETUP_SQL` in `crates/lib/src/outbox/postgres.rs`.
pub const OUTBOX_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS outbox_event (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    type           TEXT NOT NULL,
    payload        JSONB NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    attempts       INT NOT NULL DEFAULT 0,
    next_retry_at  TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at        TIMESTAMPTZ
);
"#;

/// SQLite `orders` table for Chapter 1 (no Docker).
pub const ORDERS_SCHEMA_SQL_SQLITE: &str = r#"
CREATE TABLE IF NOT EXISTS orders (
    id          TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    items       TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'created',
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
"#;
