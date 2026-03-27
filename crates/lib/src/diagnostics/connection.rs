//! Docker socket connection for infrastructure diagnostics.

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
use std::sync::LazyLock;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
use tokio::sync::Mutex;

#[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
use bollard::Docker;

/// Handle wrapping a connected Docker Engine client (bollard).
///
/// When diagnostics are disabled via the `no-diagnostics` feature, this type is a placeholder and
/// is never constructed by [`try_connect`].
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

/// Connect to the local Docker socket and verify the daemon responds.
///
/// On failure (missing socket, permission error, ping failure), logs a one-line warning and returns
/// [`None`] — callers must not treat this as a hard error.
pub async fn try_connect() -> Option<DiagnosticsHandle> {
    #[cfg(all(feature = "diagnostics", not(feature = "no-diagnostics")))]
    {
        match Docker::connect_with_local_defaults() {
            Ok(docker) => match docker.ping().await {
                Ok(_) => Some(DiagnosticsHandle::new(docker)),
                Err(error) => {
                    tracing::warn!(
                        target: "shakedown::harness",
                        error = %error,
                        "shakedown diagnostics: Docker socket reachable but ping failed; infrastructure diagnostics disabled for this run"
                    );
                    None
                }
            },
            Err(error) => {
                tracing::warn!(
                    target: "shakedown::harness",
                    error = %error,
                    "shakedown diagnostics: Docker socket unavailable; infrastructure diagnostics disabled for this run"
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
/// The result is stored for the lifetime of the process so later US can attach background tasks.
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
