//! Order status cache — key shape `order:{id}:status` (asserted in book chapter tests).

use redis::AsyncCommands;

/// Redis cache for order status strings.
#[derive(Clone)]
pub struct OrderCache {
    connection: redis::aio::MultiplexedConnection,
}

impl OrderCache {
    pub fn new(connection: redis::aio::MultiplexedConnection) -> Self {
        Self { connection }
    }

    pub async fn set_status(&self, order_id: &str, status: &str) -> Result<(), redis::RedisError> {
        let mut connection = self.connection.clone();
        let cache_key = format!("order:{order_id}:status");
        connection.set::<_, _, ()>(&cache_key, status).await
    }

    pub async fn get_status(&self, order_id: &str) -> Result<Option<String>, redis::RedisError> {
        let mut connection = self.connection.clone();
        let cache_key = format!("order:{order_id}:status");
        connection.get(&cache_key).await
    }
}
