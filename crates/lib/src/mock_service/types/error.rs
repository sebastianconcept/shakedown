//! [`MockServiceError`] — failures from mock service request helpers and response builders.

use displaydoc::Display;
use thiserror::Error;

/// Errors surfaced by mock service request helpers and response builders.
#[derive(Debug, Display, Error)]
pub enum MockServiceError {
    /// JSON deserialization failed: {source}
    Deserialization {
        /// Underlying serde_json error.
        #[source]
        source: serde_json::Error,
    },
    /// encoding failed: {message}
    Encoding {
        /// Human-readable detail (UTF-8, JSON serialization, …).
        message: String,
    },
    /// mock service server failed to start: {source}
    ServerStart {
        /// Underlying I/O error (bind/listen, …).
        #[source]
        source: std::io::Error,
    },
    /// mock service `{name}` is not registered
    NotRegistered {
        /// Logical mock service name passed to harness `Context` APIs.
        name: String,
    },
    /// response header name is not a valid HTTP token: {name}
    InvalidResponseHeaderName {
        /// Header name supplied by the mock service or stub.
        name: String,
    },
    /// failed to build HTTP response: {message}
    HttpResponseBuild {
        /// Builder or framework detail.
        message: String,
    },
    /// HTTP status code {code} is not valid (see `http::StatusCode`)
    InvalidHttpStatus {
        /// Raw value that `http::StatusCode::from_u16` rejected
        code: u16,
    },
}
