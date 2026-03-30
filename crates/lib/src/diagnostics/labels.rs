//! Docker `label` filters for Shakedown-managed containers.
//!
//! Label constants and the Docker Engine `label=` filter string live in [`crate::docker_labels`];
//! this module re-exports them at [`crate::diagnostics::labels`] for diagnostics callers.
//!
//! - [`SHAKEDOWN_DOCKER_LABEL_KEY`] / [`SHAKEDOWN_DOCKER_LABEL_VALUE`] — all Shakedown-managed containers
//!   (infrastructure resources and service-under-test).
//! - [`SHAKEDOWN_SERVICE_LABEL_KEY`] — logical service name on service-under-test containers (US-010);
//!   bollard stats, logs, and `/events` correlation use this for [`correlation_name_from_container_summary`].

pub use crate::docker_labels::{
    shakedown_docker_label_filter, SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE,
    SHAKEDOWN_SERVICE_LABEL_KEY,
};

/// Prefer the logical service name from `io.shakedown.service` when present so stats, logs, and
/// [`crate::lifecycle::Context::assert_resource`] match the service registration name (US-010).
///
/// Public so integration tests and advanced callers can align assertions with the same naming
/// rules as the diagnostics pollers.
#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
pub fn correlation_name_from_container_summary(
    summary: &bollard::models::ContainerSummary,
) -> String {
    use crate::docker_labels::SHAKEDOWN_SERVICE_LABEL_KEY;
    if let Some(labels) = &summary.labels {
        if let Some(service) = labels.get(SHAKEDOWN_SERVICE_LABEL_KEY) {
            if !service.is_empty() {
                return service.clone();
            }
        }
    }
    summary
        .names
        .as_ref()
        .and_then(|names| names.first())
        .map(|name| name.trim_start_matches('/').to_string())
        .filter(|name| !name.is_empty())
        .or_else(|| summary.id.clone())
        .unwrap_or_else(|| "unknown".to_string())
}
