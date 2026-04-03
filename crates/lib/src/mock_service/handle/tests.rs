use std::time::Duration;

use bytes::Bytes;
use time::OffsetDateTime;

use super::{MockServiceHandle, RecordedExchange};

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
) -> RecordedExchange {
    RecordedExchange {
        timestamp: sample_time(),
        method: method.to_owned(),
        path: path.to_owned(),
        query: None,
        request_headers: Vec::new(),
        request_body: Bytes::copy_from_slice(request_body),
        response_status: status,
        response_body: Bytes::copy_from_slice(response_body),
        duration: Duration::from_millis(2),
    }
}

#[test]
fn recorded_call_json_and_accessors() {
    let handle = MockServiceHandle::new("h", "127.0.0.1:1", "http://127.0.0.1:1", None);
    handle.push_recorded_exchange(RecordedExchange {
        timestamp: sample_time(),
        method: String::from("POST"),
        path: String::from("/v1/echo"),
        query: None,
        request_headers: Vec::new(),
        request_body: Bytes::from(r#"{"x":1}"#),
        response_status: 200,
        response_body: Bytes::from(r#"{"ok":true}"#),
        duration: Duration::from_millis(2),
    });
    let call = &handle.calls()[0];
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
    let handle = MockServiceHandle::new("pay", "127.0.0.1:1", "http://127.0.0.1:1", None);
    handle.push_recorded_exchange(make_call("GET", "/health", b"", 200, b""));
    handle.push_recorded_exchange(make_call("POST", "/charge", b"{}", 201, b""));
    handle.push_recorded_exchange(make_call("POST", "/charge", b"[]", 400, b""));
    let matched = handle.calls_matching("POST", "/charge");
    assert_eq!(matched.len(), 2);
}

#[test]
fn assert_called_times_pass_and_fail() {
    let handle = MockServiceHandle::new("a", "127.0.0.1:1", "http://127.0.0.1:1", None);
    handle.push_recorded_exchange(make_call("GET", "/", b"", 200, b""));
    assert!(handle.assert_called_times(1).is_ok());
    let err = handle.assert_called_times(0).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("expected 0 total calls"));
    assert!(msg.contains("`GET /`"));
}

#[test]
fn assert_path_called_times_pass_and_fail() {
    let handle = MockServiceHandle::new("a", "127.0.0.1:1", "http://127.0.0.1:1", None);
    handle.push_recorded_exchange(make_call("POST", "/v1/x", b"", 200, b""));
    assert!(handle.assert_path_called_times("POST", "/v1/x", 1).is_ok());
    let err = handle
        .assert_path_called_times("POST", "/v1/y", 1)
        .unwrap_err();
    assert!(err.to_string().contains("POST /v1/y"));
}

#[test]
fn assert_called_with_pass_and_fail() {
    let handle = MockServiceHandle::new("a", "127.0.0.1:1", "http://127.0.0.1:1", None);
    handle.push_recorded_exchange(make_call(
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
    let empty = MockServiceHandle::new("e", "127.0.0.1:1", "http://127.0.0.1:1", None);
    assert!(empty.assert_not_called().is_ok());
    let handle = MockServiceHandle::new("e", "127.0.0.1:1", "http://127.0.0.1:1", None);
    handle.push_recorded_exchange(make_call("GET", "/", b"", 200, b""));
    let err = handle.assert_not_called().unwrap_err();
    assert!(err.to_string().contains("expected no calls"));
}

#[test]
fn assert_path_not_called_pass_and_fail() {
    let handle = MockServiceHandle::new("a", "127.0.0.1:1", "http://127.0.0.1:1", None);
    handle.push_recorded_exchange(make_call("DELETE", "/v1/x", b"", 204, b""));
    assert!(handle.assert_path_not_called("GET", "/v1/x").is_ok());
    let err = handle
        .assert_path_not_called("DELETE", "/v1/x")
        .unwrap_err();
    assert!(err.to_string().contains("expected no calls"));
}

#[test]
fn push_recorded_exchange_truncates_and_sets_flag() {
    let handle = MockServiceHandle::new("t", "127.0.0.1:1", "http://proxy.example:9", Some(4));
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
    assert_eq!(handle.calls().len(), 1);
    let call = &handle.calls()[0];
    assert!(call.body_truncated);
    assert_eq!(call.request_body().len(), 4);
    assert_eq!(call.response_body().len(), 4);
    assert_eq!(handle.client_url, "http://proxy.example:9");
    assert_eq!(handle.bind_address, "127.0.0.1:1");
}

#[test]
fn push_recorded_exchange_no_limit_no_truncation() {
    let handle = MockServiceHandle::new("t", "127.0.0.1:1", "http://127.0.0.1:1", None);
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
    let call = &handle.calls()[0];
    assert!(!call.body_truncated);
    assert_eq!(call.query(), Some("q=1"));
    assert_eq!(call.headers().len(), 1);
}
