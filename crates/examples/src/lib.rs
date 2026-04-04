//! Shared fixtures for Shakedown example chapters — schemas, samples, and domain types.

use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Order {
    pub id: String,
    pub customer_id: String,
    pub items: Vec<OrderItem>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderItem {
    pub product_id: String,
    pub quantity: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Fulfillment {
    pub id: String,
    pub order_id: String,
    pub status: String,
}

/// Sample `POST /orders` JSON body for a customer id.
pub fn sample_order(customer_id: &str) -> serde_json::Value {
    serde_json::json!({
        "customer_id": customer_id,
        "items": [
            { "product_id": "widget-1", "quantity": 2 },
            { "product_id": "gadget-3", "quantity": 1 }
        ]
    })
}

/// Sample `POST /fulfill` JSON body for an order id.
pub fn sample_fulfillment(order_id: &str) -> serde_json::Value {
    serde_json::json!({
        "order_id": order_id,
        "items": [
            { "product_id": "widget-1", "quantity": 2 }
        ]
    })
}
