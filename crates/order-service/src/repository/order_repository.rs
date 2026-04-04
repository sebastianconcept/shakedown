//! [`OrderRepository`] — SQL for the `orders` table and transactional outbox inserts.

use std::sync::Arc;

use tokio_postgres::Client;

/// Relational persistence for order aggregates and outbox rows.
#[derive(Clone)]
pub struct OrderRepository {
    client: Arc<Client>,
}

impl OrderRepository {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    pub async fn insert_order(
        &self,
        order_id: &str,
        customer_id: &str,
        items_json: &str,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO orders (id, customer_id, items, status) VALUES ($1, $2, $3, $4)",
                &[&order_id, &customer_id, &items_json, &"created"],
            )
            .await?;
        Ok(())
    }

    pub async fn select_order_for_get(
        &self,
        order_id: &str,
    ) -> Result<Option<tokio_postgres::Row>, tokio_postgres::Error> {
        self.client
            .query_opt(
                "SELECT id, customer_id, items, status FROM orders WHERE id = $1",
                &[&order_id],
            )
            .await
    }

    pub async fn select_items_json(
        &self,
        order_id: &str,
    ) -> Result<Option<String>, tokio_postgres::Error> {
        let row = self
            .client
            .query_opt("SELECT items FROM orders WHERE id = $1", &[&order_id])
            .await?;
        Ok(row.map(|row| row.get(0)))
    }

    pub async fn insert_outbox_order_created(
        &self,
        aggregate_id: &str,
        payload: &str,
    ) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO outbox_event (aggregate_type, aggregate_id, type, payload) \
                 VALUES ($1, $2, $3, $4)",
                &[&"order", &aggregate_id, &"OrderCreated", &payload],
            )
            .await?;
        Ok(())
    }
}
