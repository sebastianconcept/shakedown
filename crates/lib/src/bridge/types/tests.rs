//! Unit tests for bridge request/response types.

use bytes::Bytes;

use super::{BridgeError, BridgeRequest, BridgeResponse};

#[test]
fn bridge_request_json_roundtrip() {
    // Given a POST with a JSON body
    let request = BridgeRequest {
        method: String::from("POST"),
        path: String::from("/v1/echo"),
        query: None,
        headers: Vec::new(),
        body: Bytes::from(r#"{"hello":"world"}"#),
    };
    // When we deserialize JSON from the body
    let parsed: serde_json::Value = request.json().expect("json");
    // Then the payload matches
    assert_eq!(parsed["hello"], "world");
}

#[test]
fn bridge_response_builder_json_sets_content_type() {
    // Given a builder with status and a custom header
    let response = BridgeResponse::builder()
        .status(201)
        .header("X-Test", "1")
        .json(&serde_json::json!({ "id": 42 }))
        .expect("json");
    // When we inspect headers and status
    // Then JSON content-type and custom header are present (names lowercased)
    assert_eq!(response.status, 201);
    assert!(response
        .headers
        .iter()
        .any(|(key, value)| key == "content-type" && value == "application/json"));
    assert!(response
        .headers
        .iter()
        .any(|(key, value)| key == "x-test" && value == "1"));
}

#[test]
fn bridge_request_from_http_request() {
    // Given an HTTP GET with query and Accept header
    let http_request = http::Request::builder()
        .method("GET")
        .uri("/items?limit=10")
        .header("Accept", "application/json")
        .body(Bytes::new())
        .expect("request");
    // When we convert to a bridge request
    let bridge_request = BridgeRequest::from_http_request(http_request);
    // Then method, path, query, and headers are preserved (header names lowercased)
    assert_eq!(bridge_request.method(), "GET");
    assert_eq!(bridge_request.path(), "/items");
    assert_eq!(bridge_request.query(), Some("limit=10"));
    assert!(bridge_request
        .headers()
        .iter()
        .any(|(key, value)| key == "accept" && value == "application/json"));
}

#[test]
fn bridge_request_text_valid_utf8() {
    // Given a request body that is valid UTF-8
    let request = BridgeRequest {
        method: String::from("POST"),
        path: String::from("/t"),
        query: None,
        headers: Vec::new(),
        body: Bytes::from_static(b"hello world"),
    };
    // When we decode as text
    let text = request.text().expect("text");
    // Then the string matches
    assert_eq!(text, "hello world");
}

#[test]
fn bridge_request_text_invalid_utf8_returns_encoding_error() {
    // Given a body that is not valid UTF-8
    let request = BridgeRequest {
        method: String::from("POST"),
        path: String::from("/t"),
        query: None,
        headers: Vec::new(),
        body: Bytes::from_static(&[0xff, 0xfe, 0xfd]),
    };
    // When we decode as text
    let result = request.text();
    // Then we get an encoding error
    assert!(matches!(result, Err(BridgeError::Encoding { .. })));
}

#[test]
fn bridge_response_builder_body_empty_preserves_status_and_headers() {
    // Given a builder with 204 and a custom header
    let response = BridgeResponse::builder()
        .status(204)
        .header("X-Empty", "1")
        .body_empty();
    // When we inspect the built response
    // Then status and header are preserved and body is empty
    assert_eq!(response.status, 204);
    assert!(response
        .headers
        .iter()
        .any(|(key, value)| key == "x-empty" && value == "1"));
    assert!(response.body.is_empty());
}
