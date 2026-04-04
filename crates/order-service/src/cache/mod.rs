//! Redis cache for order status (read-through in [`crate::http::handlers`](crate::http::handlers)).

mod order_cache;

pub use order_cache::OrderCache;
