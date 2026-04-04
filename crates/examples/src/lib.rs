//! Shared fixtures for Shakedown example chapters — schemas, samples, and domain types.
//!
//! ## Layout
//!
//! - [`fixtures::schema`] — SQL DDL constants
//! - [`fixtures::domain`] — `Order`, `Fulfillment`, etc.
//! - [`fixtures::samples`] — `sample_order`, `sample_fulfillment`
//!
//! The stable API is re-exported at the crate root so tests use `shakedown_examples::…` paths.

pub mod fixtures;

#[doc(inline)]
pub use fixtures::domain::{Fulfillment, Order, OrderItem};
#[doc(inline)]
pub use fixtures::samples::{sample_fulfillment, sample_order};
#[doc(inline)]
pub use fixtures::schema::{ORDERS_SCHEMA_SQL, ORDERS_SCHEMA_SQL_SQLITE, OUTBOX_SCHEMA_SQL};
