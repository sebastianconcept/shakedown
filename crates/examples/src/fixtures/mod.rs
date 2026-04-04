//! Shared fixtures for Shakedown example chapters — schemas, domain types, and sample payloads.
//!
//! **What:** DDL constants, serializable domain shapes, and JSON builders used across `tests/`.
//!
//! **Why:** Keeps chapter tests aligned with companion services and the library’s outbox DDL.
//!
//! **How:** Import from the crate root (`shakedown_examples::ORDERS_SCHEMA_SQL`, …); see the crate `lib.rs` re-exports.

pub mod domain;
pub mod samples;
pub mod schema;
