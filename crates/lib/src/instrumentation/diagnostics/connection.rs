//! Docker socket connection and process-wide handle for infrastructure diagnostics.
//!
//! [`try_connect`] probes the local Engine, [`suite_diagnostics_handle`] caches a
//! successful [`DiagnosticsHandle`] for the rest of the process, and exposes the
//! underlying [`bollard::Docker`] client for stats, logs, and events.
//!
//! Shakedown diagnostics must degrade gracefully when Docker is missing or unreachable (CI without
//! Docker, wrong `DOCKER_HOST`). By using a single client, we avoid connection storms across parallel tests.
//!
//! The shakedown lifecycle and integration tests call [`suite_diagnostics_handle`] before spawning
//! monitors; failures log a warning and yield [`None`] so scenarios still run without infra enrichment.

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
use std::sync::LazyLock;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
use tokio::sync::Mutex;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
use bollard::Docker;

/// Connected Docker Engine client used for read-only diagnostics (stats, logs, events).
///
/// Exists so the shakedown reuses one [`bollard::Docker`] per process when the socket is available,
/// instead of reconnecting for every list or stream call. When the socket is not available,
/// we return [`None`] so scenarios still run without infra enrichment.
///
/// Obtained from [`try_connect`] or the cached [`suite_diagnostics_handle`]. When `no-diagnostics` is enabled,
/// this type is a zero-sized placeholder and is never constructed with a real client.
pub struct DiagnosticsHandle {
    #[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
    docker: bollard::Docker,
    #[cfg(not(all(feature = "diagnostics", not(feature = "no-diagnostics"))))]
    _disabled: (),
}

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
impl Clone for DiagnosticsHandle {
    fn clone(&self) -> Self {
        Self {
            docker: self.docker.clone(),
        }
    }
}

#[cfg(not(all(feature = "diagnostics", not(feature = "no-diagnostics"))))]
impl Clone for DiagnosticsHandle {
    fn clone(&self) -> Self {
        Self { _disabled: () }
    }
}

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
impl std::fmt::Debug for DiagnosticsHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiagnosticsHandle")
            .finish_non_exhaustive()
    }
}

#[cfg(not(all(feature = "diagnostics", not(feature = "no-diagnostics"))))]
impl std::fmt::Debug for DiagnosticsHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiagnosticsHandle")
            .finish_non_exhaustive()
    }
}

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
impl DiagnosticsHandle {
    /// Borrow the underlying bollard Docker client.
    pub fn docker(&self) -> &Docker {
        &self.docker
    }

    pub(crate) fn new(docker: Docker) -> Self {
        Self { docker }
    }
}

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
static SUITE_DIAGNOSTICS: LazyLock<Mutex<Option<Option<DiagnosticsHandle>>>> =
    LazyLock::new(|| Mutex::new(None));

/// Connect to the local Docker socket and verify the daemon responds (`ping`).
///
/// Exists as the narrow entry point so callers distinguish “no Docker” from provision failures.
///
/// On failure (missing socket, permission error, ping failure), logs a one-line warning and returns
/// [`None`] — callers must not treat this as a hard error.
pub async fn try_connect() -> Option<DiagnosticsHandle> {
    #[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
    {
        // Use `connect_with_defaults` (not `connect_with_local_defaults`) so `DOCKER_HOST` matches
        // testcontainers and other tooling: `tcp://` / `http://` point at the same engine as
        // `unix://` when unset (matches testcontainers DOCKER_HOST resolution).
        match Docker::connect_with_defaults() {
            Ok(docker) => match docker.ping().await {
                Ok(_) => Some(DiagnosticsHandle::new(docker)),
                Err(error) => {
                    tracing::warn!(
                        target: "shakedown::harness",
                        error = %error,
                        "shakedown diagnostics: Docker engine reachable but ping failed; infrastructure diagnostics disabled for this run"
                    );
                    None
                }
            },
            Err(error) => {
                tracing::warn!(
                    target: "shakedown::harness",
                    error = %error,
                    "shakedown diagnostics: Docker engine unavailable; infrastructure diagnostics disabled for this run"
                );
                None
            }
        }
    }
    #[cfg(not(all(feature = "diagnostics", not(feature = "no-diagnostics"))))]
    {
        None
    }
}

/// Lazily initialized suite-level diagnostics handle (one Docker client per process when enabled).
///
/// Exists so every scenario and background monitor shares the same connection pool settings and
/// avoids repeating socket setup.
///
/// First call runs [`try_connect`] and caches the result for the process lifetime; subsequent calls
/// return the cached [`DiagnosticsHandle`] or [`None`].
pub async fn suite_diagnostics_handle() -> Option<DiagnosticsHandle> {
    #[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
    {
        let mut guard = SUITE_DIAGNOSTICS.lock().await;
        if guard.is_none() {
            *guard = Some(try_connect().await);
        }
        guard.as_ref().and_then(|inner| inner.as_ref()).cloned()
    }
    #[cfg(not(all(feature = "diagnostics", not(feature = "no-diagnostics"))))]
    {
        None
    }
}
