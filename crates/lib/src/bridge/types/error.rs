//! [`BridgeError`] — failures from bridge request helpers and response builders.

use displaydoc::Display;
use thiserror::Error;

/// Errors surfaced by bridge request helpers and response builders.
#[derive(Debug, Display, Error)]
pub enum BridgeError {
    /// JSON deserialization failed: {source}
    Deserialization {
        #[source]
        source: serde_json::Error,
    },
    /// encoding failed: {message}
    Encoding {
        /// Human-readable detail (UTF-8, JSON serialization, …).
        message: String,
    },
    /// bridge server failed to start: {source}
    ServerStart {
        /// Underlying I/O error (bind/listen, …).
        #[source]
        source: std::io::Error,
    },
    /// bridge `{name}` is not registered
    NotRegistered {
        /// Logical bridge name passed to harness `Context` APIs.
        name: String,
    },
    /// response header name is not a valid HTTP token: {name}
    InvalidResponseHeaderName {
        /// Header name supplied by the bridge or stub.
        name: String,
    },
    /// failed to build HTTP response: {message}
    HttpResponseBuild {
        /// Builder or framework detail.
        message: String,
    },
}
