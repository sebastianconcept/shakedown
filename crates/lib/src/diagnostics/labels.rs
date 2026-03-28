//! Docker `label` filters for Shakedown-managed containers.
//!
//! **What:** stable key/value constants and a helper that builds the `key=value` fragment for Engine
//! API `filters` maps (list containers, events, stats).
//!
//! **Why:** diagnostics must select *our* containers without matching unrelated workloads on the same
//! daemon; we use an `io.shakedown.*` namespace rather than `org.testcontainers.*`, which
//! testcontainers reserves for its own metadata.
//!
//! **How:** resources that start containers set [`SHAKEDOWN_DOCKER_LABEL_KEY`] =
//! [`SHAKEDOWN_DOCKER_LABEL_VALUE`]; diagnostics pass [`shakedown_docker_label_filter`] into bollard.

/// Label key for containers started by Shakedown Docker-backed resources.
///
/// Paired with [`SHAKEDOWN_DOCKER_LABEL_VALUE`] on provisioned containers so list/events/stats filters
/// can target Shakedown workloads only.
pub const SHAKEDOWN_DOCKER_LABEL_KEY: &str = "io.shakedown.framework";

/// Label value paired with [`SHAKEDOWN_DOCKER_LABEL_KEY`].
pub const SHAKEDOWN_DOCKER_LABEL_VALUE: &str = "true";

/// `label=` filter fragment for Docker Engine APIs (`key=value`).
///
/// Combine with other filters (for example `status=running`) when building bollard options.
#[must_use]
pub fn shakedown_docker_label_filter() -> String {
    format!("{SHAKEDOWN_DOCKER_LABEL_KEY}={SHAKEDOWN_DOCKER_LABEL_VALUE}")
}
