//! Per-bridge call recording and assertion helpers.

use std::time::Duration;

use bytes::Bytes;
use time::OffsetDateTime;

use crate::error::ScenarioError;

use super::types::BridgeError;

/// One recorded inbound HTTP request and its response (bridge server wrapper).
#[derive(Debug, Clone)]
pub struct RecordedCall {
    pub timestamp: OffsetDateTime,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub request_headers: Vec<(String, String)>,
    pub request_body: Bytes,
    pub response_status: u16,
    pub response_body: Bytes,
    pub duration: Duration,
    /// True if request or response body was truncated when recording (see [`BridgeHandle::max_recorded_body_size`]).
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

    pub fn response_status(&self) -> u16 {
        self.response_status
    }

    /// Raw response body (may be truncated; see [`RecordedCall::body_truncated`]).
    pub fn response_body(&self) -> &Bytes {
        &self.response_body
    }

    pub fn timestamp(&self) -> OffsetDateTime {
        self.timestamp
    }

    pub fn duration(&self) -> Duration {
        self.duration
    }

    /// Deserialize the **request** body as JSON.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, BridgeError> {
        serde_json::from_slice(self.request_body.as_ref()).map_err(BridgeError::from)
    }
}

/// Full request/response snapshot passed to [`BridgeHandle::push_recorded_exchange`].
#[derive(Debug)]
pub struct RecordedExchange {
    pub timestamp: OffsetDateTime,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub request_headers: Vec<(String, String)>,
    pub request_body: Bytes,
    pub response_status: u16,
    pub response_body: Bytes,
    pub duration: Duration,
}

/// Handle for one bridge instance: bind URL, client URL, and recorded traffic.
#[derive(Debug)]
pub struct BridgeHandle {
    pub name: String,
    /// Actual socket address the server bound to (e.g. `127.0.0.1:54321`).
    pub bind_address: String,
    /// URL the service-under-test should use (may differ from bind when proxied).
    pub client_url: String,
    pub calls: Vec<RecordedCall>,
    /// When set, request/response bodies stored on [`RecordedCall`] are capped; see [`RecordedCall::body_truncated`].
    pub max_recorded_body_size: Option<usize>,
}

impl BridgeHandle {
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
            calls: Vec::new(),
            max_recorded_body_size,
        }
    }

    /// Append a call, applying [`Self::max_recorded_body_size`] to request/response bodies.
    pub fn push_recorded_exchange(&mut self, exchange: RecordedExchange) {
        let (request_body, rb_trunc) =
            truncate_bytes(exchange.request_body, self.max_recorded_body_size);
        let (response_body, resp_trunc) =
            truncate_bytes(exchange.response_body, self.max_recorded_body_size);
        self.calls.push(RecordedCall {
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
        });
    }

    pub fn calls(&self) -> &[RecordedCall] {
        self.calls.as_slice()
    }

    /// Calls whose method and path match exactly (case-sensitive for method).
    pub fn calls_matching(&self, method: &str, path: &str) -> Vec<&RecordedCall> {
        self.calls
            .iter()
            .filter(|call| call.method() == method && call.path() == path)
            .collect()
    }

    pub fn assert_called_times(&self, expected: usize) -> Result<(), ScenarioError> {
        let actual = self.calls.len();
        if actual == expected {
            return Ok(());
        }
        Err(ScenarioError::Assertion(format!(
            "bridge '{}': expected {} total calls, got {}; recorded: {}",
            self.name,
            expected,
            actual,
            format_calls_diagnostic(&self.calls)
        )))
    }

    pub fn assert_path_called_times(
        &self,
        method: &str,
        path: &str,
        expected: usize,
    ) -> Result<(), ScenarioError> {
        let matching = self.calls_matching(method, path);
        let actual = matching.len();
        if actual == expected {
            return Ok(());
        }
        Err(ScenarioError::Assertion(format!(
            "bridge '{}': expected {} calls to `{} {}`, got {}; recorded: {}",
            self.name,
            expected,
            method,
            path,
            actual,
            format_calls_diagnostic(&self.calls)
        )))
    }

    pub fn assert_called_with(
        &self,
        method: &str,
        path: &str,
        predicate: impl Fn(&RecordedCall) -> bool,
    ) -> Result<(), ScenarioError> {
        let matched: Vec<&RecordedCall> = self
            .calls_matching(method, path)
            .into_iter()
            .filter(|call| predicate(call))
            .collect();
        if !matched.is_empty() {
            return Ok(());
        }
        Err(ScenarioError::Assertion(format!(
            "bridge '{}': no call to `{} {}` satisfied the predicate; recorded: {}",
            self.name,
            method,
            path,
            format_calls_diagnostic(&self.calls)
        )))
    }

    pub fn assert_not_called(&self) -> Result<(), ScenarioError> {
        if self.calls.is_empty() {
            return Ok(());
        }
        Err(ScenarioError::Assertion(format!(
            "bridge '{}': expected no calls, got {}; recorded: {}",
            self.name,
            self.calls.len(),
            format_calls_diagnostic(&self.calls)
        )))
    }

    pub fn assert_path_not_called(&self, method: &str, path: &str) -> Result<(), ScenarioError> {
        let matching = self.calls_matching(method, path);
        if matching.is_empty() {
            return Ok(());
        }
        Err(ScenarioError::Assertion(format!(
            "bridge '{}': expected no calls to `{} {}`, got {}; recorded: {}",
            self.name,
            method,
            path,
            matching.len(),
            format_calls_diagnostic(&self.calls)
        )))
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

#[cfg(all(test, feature = "bridges"))]
mod tests {
    use super::*;

    fn sample_time() -> OffsetDateTime {
        let date = time::Date::from_calendar_date(2026, time::Month::March, 28).unwrap();
        let time_of_day = time::Time::from_hms(12, 0, 0).unwrap();
        time::PrimitiveDateTime::new(date, time_of_day).assume_utc()
    }

    fn make_call(
        method: &str,
        path: &str,
        request_body: &[u8],
        status: u16,
        response_body: &[u8],
    ) -> RecordedCall {
        RecordedCall {
            timestamp: sample_time(),
            method: method.to_owned(),
            path: path.to_owned(),
            query: None,
            request_headers: Vec::new(),
            request_body: Bytes::copy_from_slice(request_body),
            response_status: status,
            response_body: Bytes::copy_from_slice(response_body),
            duration: Duration::from_millis(2),
            body_truncated: false,
        }
    }

    #[test]
    fn recorded_call_json_and_accessors() {
        let call = make_call("POST", "/v1/echo", br#"{"x":1}"#, 200, br#"{"ok":true}"#);
        assert_eq!(call.method(), "POST");
        assert_eq!(call.path(), "/v1/echo");
        assert_eq!(call.query(), None);
        assert!(call.headers().is_empty());
        assert_eq!(call.request_body().as_ref(), br#"{"x":1}"#);
        assert_eq!(call.response_status(), 200);
        assert_eq!(call.response_body().as_ref(), br#"{"ok":true}"#);
        assert_eq!(call.timestamp(), sample_time());
        assert_eq!(call.duration(), Duration::from_millis(2));
        let parsed: serde_json::Value = call.json().expect("json");
        assert_eq!(parsed["x"], 1);
    }

    #[test]
    fn calls_matching_filters_method_and_path() {
        let mut handle = BridgeHandle::new("pay", "127.0.0.1:1", "http://127.0.0.1:1", None);
        handle
            .calls
            .push(make_call("GET", "/health", b"", 200, b""));
        handle
            .calls
            .push(make_call("POST", "/charge", b"{}", 201, b""));
        handle
            .calls
            .push(make_call("POST", "/charge", b"[]", 400, b""));
        let matched = handle.calls_matching("POST", "/charge");
        assert_eq!(matched.len(), 2);
    }

    #[test]
    fn assert_called_times_pass_and_fail() {
        let mut handle = BridgeHandle::new("a", "127.0.0.1:1", "http://127.0.0.1:1", None);
        handle.calls.push(make_call("GET", "/", b"", 200, b""));
        assert!(handle.assert_called_times(1).is_ok());
        let err = handle.assert_called_times(0).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("expected 0 total calls"));
        assert!(msg.contains("`GET /`"));
    }

    #[test]
    fn assert_path_called_times_pass_and_fail() {
        let mut handle = BridgeHandle::new("a", "127.0.0.1:1", "http://127.0.0.1:1", None);
        handle.calls.push(make_call("POST", "/v1/x", b"", 200, b""));
        assert!(handle.assert_path_called_times("POST", "/v1/x", 1).is_ok());
        let err = handle
            .assert_path_called_times("POST", "/v1/y", 1)
            .unwrap_err();
        assert!(err.to_string().contains("POST /v1/y"));
    }

    #[test]
    fn assert_called_with_pass_and_fail() {
        let mut handle = BridgeHandle::new("a", "127.0.0.1:1", "http://127.0.0.1:1", None);
        handle.calls.push(make_call(
            "POST",
            "/v1/charge",
            br#"{"amount":10}"#,
            200,
            b"",
        ));
        assert!(handle
            .assert_called_with("POST", "/v1/charge", |call| {
                call.json::<serde_json::Value>()
                    .ok()
                    .and_then(|value| value.get("amount").and_then(|v| v.as_u64()))
                    == Some(10)
            })
            .is_ok());
        let err = handle
            .assert_called_with("POST", "/v1/charge", |_| false)
            .unwrap_err();
        assert!(err.to_string().contains("no call"));
    }

    #[test]
    fn assert_not_called_pass_and_fail() {
        let empty = BridgeHandle::new("e", "127.0.0.1:1", "http://127.0.0.1:1", None);
        assert!(empty.assert_not_called().is_ok());
        let mut handle = BridgeHandle::new("e", "127.0.0.1:1", "http://127.0.0.1:1", None);
        handle.calls.push(make_call("GET", "/", b"", 200, b""));
        let err = handle.assert_not_called().unwrap_err();
        assert!(err.to_string().contains("expected no calls"));
    }

    #[test]
    fn assert_path_not_called_pass_and_fail() {
        let mut handle = BridgeHandle::new("a", "127.0.0.1:1", "http://127.0.0.1:1", None);
        handle
            .calls
            .push(make_call("DELETE", "/v1/x", b"", 204, b""));
        assert!(handle.assert_path_not_called("GET", "/v1/x").is_ok());
        let err = handle
            .assert_path_not_called("DELETE", "/v1/x")
            .unwrap_err();
        assert!(err.to_string().contains("expected no calls"));
    }

    #[test]
    fn push_recorded_exchange_truncates_and_sets_flag() {
        let mut handle = BridgeHandle::new("t", "127.0.0.1:1", "http://proxy.example:9", Some(4));
        handle.push_recorded_exchange(RecordedExchange {
            timestamp: sample_time(),
            method: String::from("POST"),
            path: String::from("/up"),
            query: None,
            request_headers: Vec::new(),
            request_body: Bytes::from_static(b"abcdef"),
            response_status: 200,
            response_body: Bytes::from_static(b"ghijkl"),
            duration: Duration::from_millis(1),
        });
        assert_eq!(handle.calls.len(), 1);
        let call = &handle.calls[0];
        assert!(call.body_truncated);
        assert_eq!(call.request_body().len(), 4);
        assert_eq!(call.response_body().len(), 4);
        assert_eq!(handle.client_url, "http://proxy.example:9");
        assert_eq!(handle.bind_address, "127.0.0.1:1");
    }

    #[test]
    fn push_recorded_exchange_no_limit_no_truncation() {
        let mut handle = BridgeHandle::new("t", "127.0.0.1:1", "http://127.0.0.1:1", None);
        handle.push_recorded_exchange(RecordedExchange {
            timestamp: sample_time(),
            method: String::from("GET"),
            path: String::from("/"),
            query: Some(String::from("q=1")),
            request_headers: vec![(String::from("x-foo"), String::from("bar"))],
            request_body: Bytes::from_static(b"large-body"),
            response_status: 500,
            response_body: Bytes::from_static(b"err"),
            duration: Duration::from_micros(9),
        });
        let call = &handle.calls[0];
        assert!(!call.body_truncated);
        assert_eq!(call.query(), Some("q=1"));
        assert_eq!(call.headers().len(), 1);
    }
}
