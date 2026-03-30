//! Service-under-test lifecycle (`services` feature).
//!
//! Opt-in module for running real services in Docker during scenarios. Concrete types and
//! lifecycle wiring land in follow-up user stories; this module is the compile-time seam.

mod builder;
mod health;
mod types;

pub use builder::ServiceUnderTest;
pub use health::{HealthCheck, DEFAULT_HEALTH_CHECK_INTERVAL, DEFAULT_HEALTH_CHECK_TIMEOUT};
pub use types::{EnvMapping, PullPolicy, ServiceSpec};
