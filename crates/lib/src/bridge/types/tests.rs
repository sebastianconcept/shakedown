//! Unit tests for bridge request/response types.

use bytes::Bytes;

use super::{BridgeRequest, BridgeResponse};

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
