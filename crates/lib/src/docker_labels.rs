//! Docker `label` key/value constants for Shakedown-managed containers (resources + services).
//!
//! Lives at crate root so `services` and Docker-backed resources share one definition without
//! depending on the `diagnostics` feature module layout.

/// Label key for containers started by Shakedown Docker-backed resources and service-under-test.
pub const SHAKEDOWN_DOCKER_LABEL_KEY: &str = "io.shakedown.framework";

/// Label value paired with [`SHAKEDOWN_DOCKER_LABEL_KEY`].
pub const SHAKEDOWN_DOCKER_LABEL_VALUE: &str = "true";

/// Label key identifying a logical service-under-test name on its container.
pub const SHAKEDOWN_SERVICE_LABEL_KEY: &str = "io.shakedown.service";

/// `label=` filter fragment for Docker Engine APIs (`key=value`).
///
/// Used by diagnostics and tests; kept available even when the `diagnostics` module is not built.
#[allow(dead_code)]
#[must_use]
pub fn shakedown_docker_label_filter() -> String {
    format!("{SHAKEDOWN_DOCKER_LABEL_KEY}={SHAKEDOWN_DOCKER_LABEL_VALUE}")
}
