//! Per-mock-service call recording and assertion helpers.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use time::OffsetDateTime;

use crate::error::{AssertionError, ShakedownError};

use super::types::MockServiceError;

/// One recorded inbound HTTP request and its response (mock service server wrapper).
#[derive(Debug, Clone)]
pub struct RecordedCall {
    /// When the request was received by the mock server.
    pub timestamp: OffsetDateTime,
    /// HTTP method (e.g. `GET`, `POST`).
    pub method: String,
    /// URL path component of the inbound request.
    pub path: String,
    /// Raw query string without the leading `?`, if present.
    pub query: Option<String>,
    /// Request headers as `(name, value)` pairs (names lowercased by the harness).
    pub request_headers: Vec<(String, String)>,
    /// Raw request body bytes (may be truncated; see [`RecordedCall::body_truncated`]).
    pub request_body: Bytes,
    /// HTTP status code returned by the mock route handler.
    pub response_status: u16,
    /// Raw response body bytes (may be truncated; see [`RecordedCall::body_truncated`]).
    pub response_body: Bytes,
    /// Time elapsed between request receipt and response sent.
    pub duration: Duration,
    /// True if request or response body was truncated when recording (see [`MockServiceHandle::max_recorded_body_size`]).
    pub body_truncated: bool,
}

impl RecordedCall {
    /// HTTP method as a string slice.
    pub fn method(&self) -> &str {
        self.method.as_str()
    }

    /// URL path as a string slice.
    pub fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Optional query string (without leading `?`).
    pub fn query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// Request headers (name lowercased when produced by the harness).
    pub fn headers(&self) -> &[(String, String)] {
        self.request_headers.as_slice()
    }

    /// Raw request body (may be truncated; see [`RecordedCall::body_truncated`]).
    pub fn request_body(&self) -> &Bytes {
        &self.request_body
    }

    /// HTTP status code returned by the mock route handler.
    pub fn response_status(&self) -> u16 {
        self.response_status
    }

    /// Raw response body (may be truncated; see [`RecordedCall::body_truncated`]).
    pub fn response_body(&self) -> &Bytes {
        &self.response_body
    }

    /// When this request was received by the mock server.
    pub fn timestamp(&self) -> OffsetDateTime {
        self.timestamp
    }

    /// Time elapsed between request receipt and response sent.
    pub fn duration(&self) -> Duration {
        self.duration
    }

    /// Deserialize the **request** body as JSON.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, MockServiceError> {
        serde_json::from_slice(self.request_body.as_ref())
            .map_err(|source| MockServiceError::Deserialization { source })
    }
}

/// Full request/response snapshot passed to [`MockServiceHandle::push_recorded_exchange`].
#[derive(Debug)]
pub struct RecordedExchange {
    /// When the request was received by the mock server.
    pub timestamp: OffsetDateTime,
    /// HTTP method (e.g. `GET`, `POST`).
    pub method: String,
    /// URL path component of the inbound request.
    pub path: String,
    /// Raw query string without the leading `?`, if present.
    pub query: Option<String>,
    /// Request headers as `(name, value)` pairs (names lowercased by the harness).
    pub request_headers: Vec<(String, String)>,
    /// Raw request body bytes.
    pub request_body: Bytes,
    /// HTTP status code returned by the mock route handler.
    pub response_status: u16,
    /// Raw response body bytes.
    pub response_body: Bytes,
    /// Time elapsed between request receipt and response sent.
    pub duration: Duration,
}

/// Handle for one mock service instance: bind URL, client URL, and recorded traffic.
pub struct MockServiceHandle {
    /// Logical name of this mock service (used in assertion messages and registration lookup).
    pub name: String,
    /// Actual socket address the server bound to (e.g. `127.0.0.1:54321`).
    pub bind_address: String,
    /// URL the service-under-test should use (may differ from bind when proxied).
    pub client_url: String,
    calls_storage: Arc<Mutex<Vec<RecordedCall>>>,
    /// When set, request/response bodies stored on [`RecordedCall`] are capped; see [`RecordedCall::body_truncated`].
    pub max_recorded_body_size: Option<usize>,
}

/// Locks the recorded-calls buffer. If the mutex is poisoned (a panicking thread held the lock),
/// recovers the inner data via [`std::sync::PoisonError::into_inner`] so teardown and assertions
/// can still run after a panic elsewhere.
fn lock_mock_service_calls(
    storage: &Arc<Mutex<Vec<RecordedCall>>>,
) -> std::sync::MutexGuard<'_, Vec<RecordedCall>> {
    storage
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl fmt::Debug for MockServiceHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = lock_mock_service_calls(&self.calls_storage).len();
        f.debug_struct("MockServiceHandle")
            .field("name", &self.name)
            .field("bind_address", &self.bind_address)
            .field("client_url", &self.client_url)
            .field("call_count", &count)
            .field("max_recorded_body_size", &self.max_recorded_body_size)
            .finish()
    }
}

impl MockServiceHandle {
    /// Create a handle (typically by the harness after binding a listener).
    pub fn new(
        name: impl Into<String>,
        bind_address: impl Into<String>,
        client_url: impl Into<String>,
        max_recorded_body_size: Option<usize>,
    ) -> Self {
        Self {
            name: name.into(),
            bind_address: bind_address.into(),
            client_url: client_url.into(),
            calls_storage: Arc::new(Mutex::new(Vec::new())),
            max_recorded_body_size,
        }
    }

    /// Append a call, applying [`Self::max_recorded_body_size`] to request/response bodies.
    pub fn push_recorded_exchange(&self, exchange: RecordedExchange) {
        let (request_body, rb_trunc) =
            truncate_bytes(exchange.request_body, self.max_recorded_body_size);
        let (response_body, resp_trunc) =
            truncate_bytes(exchange.response_body, self.max_recorded_body_size);
        let recorded = RecordedCall {
            timestamp: exchange.timestamp,
            method: exchange.method,
            path: exchange.path,
            query: exchange.query,
            request_headers: exchange.request_headers,
            request_body,
            response_status: exchange.response_status,
            response_body,
            duration: exchange.duration,
            body_truncated: rb_trunc || resp_trunc,
        };
        let mut calls = lock_mock_service_calls(&self.calls_storage);
        calls.push(recorded);
    }

    /// Returns a clone of all recorded calls.
    pub fn calls(&self) -> Vec<RecordedCall> {
        lock_mock_service_calls(&self.calls_storage).clone()
    }

    /// Calls whose method and path match exactly (case-sensitive for method).
    pub fn calls_matching(&self, method: &str, path: &str) -> Vec<RecordedCall> {
        let calls = lock_mock_service_calls(&self.calls_storage);
        calls
            .iter()
            .filter(|call| call.method() == method && call.path() == path)
            .cloned()
            .collect()
    }

    /// On mismatch returns [`ShakedownError::Scenario`].
    pub fn assert_called_times(&self, expected: usize) -> Result<(), ShakedownError> {
        let calls = lock_mock_service_calls(&self.calls_storage);
        let actual = calls.len();
        if actual == expected {
            return Ok(());
        }
        Err(AssertionError::MockServiceCallCountMismatch {
            mock_service_name: self.name.clone(),
            expected,
            actual,
            recorded_calls_diagnostic: format_calls_diagnostic(&calls),
        }
        .into())
    }

    /// See [`Self::assert_called_times`] for error semantics.
    pub fn assert_path_called_times(
        &self,
        method: &str,
        path: &str,
        expected: usize,
    ) -> Result<(), ShakedownError> {
        let calls = lock_mock_service_calls(&self.calls_storage);
        let actual = calls
            .iter()
            .filter(|call| call.method() == method && call.path() == path)
            .count();
        if actual == expected {
            return Ok(());
        }
        Err(AssertionError::MockServicePathCallCountMismatch {
            mock_service_name: self.name.clone(),
            method: method.to_string(),
            path: path.to_string(),
            expected,
            actual,
            recorded_calls_diagnostic: format_calls_diagnostic(&calls),
        }
        .into())
    }

    /// See [`Self::assert_called_times`] for error semantics.
    pub fn assert_called_with(
        &self,
        method: &str,
        path: &str,
        predicate: impl Fn(&RecordedCall) -> bool,
    ) -> Result<(), ShakedownError> {
        let calls = lock_mock_service_calls(&self.calls_storage);
        let matched = calls
            .iter()
            .filter(|call| call.method() == method && call.path() == path)
            .any(predicate);
        if matched {
            return Ok(());
        }
        Err(AssertionError::MockServicePredicateUnmatched {
            mock_service_name: self.name.clone(),
            method: method.to_string(),
            path: path.to_string(),
            recorded_calls_diagnostic: format_calls_diagnostic(&calls),
        }
        .into())
    }

    /// See [`Self::assert_called_times`] for error semantics.
    pub fn assert_not_called(&self) -> Result<(), ShakedownError> {
        let calls = lock_mock_service_calls(&self.calls_storage);
        if calls.is_empty() {
            return Ok(());
        }
        Err(AssertionError::MockServiceUnexpectedCalls {
            mock_service_name: self.name.clone(),
            actual: calls.len(),
            recorded_calls_diagnostic: format_calls_diagnostic(&calls),
        }
        .into())
    }

    /// See [`Self::assert_called_times`] for error semantics.
    pub fn assert_path_not_called(&self, method: &str, path: &str) -> Result<(), ShakedownError> {
        let calls = lock_mock_service_calls(&self.calls_storage);
        let matching = calls
            .iter()
            .filter(|call| call.method() == method && call.path() == path)
            .count();
        if matching == 0 {
            return Ok(());
        }
        Err(AssertionError::MockServicePathUnexpectedCalls {
            mock_service_name: self.name.clone(),
            method: method.to_string(),
            path: path.to_string(),
            matching_call_count: matching,
            recorded_calls_diagnostic: format_calls_diagnostic(&calls),
        }
        .into())
    }
}

fn truncate_bytes(body: Bytes, limit: Option<usize>) -> (Bytes, bool) {
    let Some(max) = limit else {
        return (body, false);
    };
    if body.len() <= max {
        return (body, false);
    }
    (body.slice(0..max), true)
}

fn format_calls_diagnostic(calls: &[RecordedCall]) -> String {
    if calls.is_empty() {
        return String::from("(none)");
    }
    let mut parts = Vec::with_capacity(calls.len());
    for call in calls {
        parts.push(format!(
            "`{} {}` @ {}",
            call.method(),
            call.path(),
            format_timestamp(call.timestamp())
        ));
    }
    format!("[{}]", parts.join(", "))
}

fn format_timestamp(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| timestamp.to_string())
}

#[cfg(all(test, feature = "mock-services"))]
mod tests;
