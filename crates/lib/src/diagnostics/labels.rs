//! Labels applied to Shakedown-managed containers so Docker event filters can select them.

/// Label key for containers started by Shakedown Docker-backed resources.
///
/// Uses the `io.shakedown.*` namespace (not `org.testcontainers.*`, which testcontainers reserves).
pub const SHAKEDOWN_DOCKER_LABEL_KEY: &str = "io.shakedown.framework";

/// Label value paired with [`SHAKEDOWN_DOCKER_LABEL_KEY`].
pub const SHAKEDOWN_DOCKER_LABEL_VALUE: &str = "true";

/// `label=` filter fragment for the Docker Engine `/events` API (`key=value`).
#[must_use]
pub fn shakedown_docker_label_filter() -> String {
    format!("{SHAKEDOWN_DOCKER_LABEL_KEY}={SHAKEDOWN_DOCKER_LABEL_VALUE}")
}
