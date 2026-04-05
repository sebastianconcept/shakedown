//! [`InternalError`] — framework bugs and invariant violations.

use std::error::Error;

use displaydoc::Display;

/// Error types related to internal framework bugs or invariant violations.
///
/// These errors indicate problems with the Shakedown framework itself,
/// not with user scenarios or infrastructure.
///
/// [`Display`] is from [`displaydoc`] (not `thiserror`) because [`InternalError::WithBacktrace`]
/// carries a [`std::backtrace::Backtrace`]; `thiserror` would enable unstable error-provider
/// integration for that field type on stable Rust.
#[derive(Debug, Display)]
#[ignore_extra_doc_attributes]
#[allow(missing_docs)]
pub enum InternalError {
    /// Internal error: Invariant violation: {0}
    ///
    /// Cause: a harness invariant was broken (should not happen in normal runs — report as a bug).
    InvariantViolation(String),
    /// Internal error: Invalid configuration: {0}
    ///
    /// Cause: lifecycle or resource wiring is inconsistent (e.g. missing prerequisite before run).
    InvalidConfiguration(String),
    /// Internal error: Unexpected state: {0}
    ///
    /// Cause: harness state machine saw an illegal transition or stale handle.
    UnexpectedState(String),
    /// Internal error: Framework bug: {0}
    ///
    /// Cause: explicit framework defect path; please report with a minimal repro.
    Bug(String),
    /// Internal error: Load artifact I/O: {operation} — {detail}
    ///
    /// Cause: reading or writing load reports/baselines under `target/shakedown/` (missing file, permissions, disk full, etc.).
    LoadArtifactIo {
        operation: &'static str,
        detail: String,
    },
    /// Internal error: {msg}
    ///
    /// Cause: internal failure with captured backtrace (not shown in this message).
    WithBacktrace {
        msg: String,
        #[doc(hidden)]
        backtrace: std::backtrace::Backtrace,
    },
    /// Internal error: Poisoned mutex ({0}): another thread panicked while holding the lock
    ///
    /// Cause: another thread panicked while holding a mutex used by the harness; the lock is poisoned.
    PoisonedMutex(String),
}

impl Error for InternalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            InternalError::InvariantViolation(_) => None,
            InternalError::InvalidConfiguration(_) => None,
            InternalError::UnexpectedState(_) => None,
            InternalError::Bug(_) => None,
            InternalError::LoadArtifactIo { .. } => None,
            InternalError::WithBacktrace { .. } => None,
            InternalError::PoisonedMutex(_) => None,
        }
    }
}
