//! Docker `label` filters for Shakedown-managed containers.
//!
//! Re-exports [`crate::docker_labels`] so callers can keep using `shakedown::diagnostics::labels`.

pub use crate::docker_labels::*;
