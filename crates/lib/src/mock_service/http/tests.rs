use std::sync::Arc;

use bytes::Bytes;

use super::service::{HttpMockService, RouteMatch};
use super::HttpMockServiceRequest;
use super::HttpMockServiceResponse;
use super::HttpMockServiceTrait;
use crate::mock_service::types::{MockServiceContext, MockServiceResources};

fn ctx() -> MockServiceContext {
    MockServiceContext::new("s1", "stub", Arc::new(MockServiceResources::default()))
}

#[tokio::test]
async fn exact_match_returns_registered_response() {
    let mock_service = HttpMockService::new("api")
        .on("GET", "/health")
        .respond(204);
    let request = HttpMockServiceRequest {
        method: String::from("GET"),
        path: String::from("/health"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    let response = mock_service.handle(request, &ctx()).await;
    assert_eq!(response.status, 204);
}

#[tokio::test]
async fn glob_matches_single_segment() {
    let mock_service = HttpMockService::new("users")
        .on("GET", "/users/*/profile")
        .respond(200);
    let request = HttpMockServiceRequest {
        method: String::from("GET"),
        path: String::from("/users/42/profile"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    let response = mock_service.handle(request, &ctx()).await;
    assert_eq!(response.status, 200);
}

#[tokio::test]
async fn unmatched_uses_default_404() {
    let mock_service = HttpMockService::new("api").on("GET", "/known").respond(200);
    let request = HttpMockServiceRequest {
        method: String::from("GET"),
        path: String::from("/other"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    let response = mock_service.handle(request, &ctx()).await;
    assert_eq!(response.status, 404);
}

#[tokio::test]
async fn unmatched_uses_configured_default() {
    let mock_service = HttpMockService::new("api")
        .on("GET", "/known")
        .respond(200)
        .default_response(503);
    let request = HttpMockServiceRequest {
        method: String::from("GET"),
        path: String::from("/missing"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    let response = mock_service.handle(request, &ctx()).await;
    assert_eq!(response.status, 503);
}

#[tokio::test]
async fn registration_order_first_match_wins() {
    let mock_service = HttpMockService::new("order")
        .on("GET", "/users/*")
        .respond(111)
        .on("GET", "/users/42")
        .respond(222);
    let request = HttpMockServiceRequest {
        method: String::from("GET"),
        path: String::from("/users/42"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    let response = mock_service.handle(request, &ctx()).await;
    assert_eq!(response.status, 111);
}

#[tokio::test]
async fn registration_order_exact_before_glob() {
    let mock_service = HttpMockService::new("order")
        .on("GET", "/users/42")
        .respond(222)
        .on("GET", "/users/*")
        .respond(111);
    let request = HttpMockServiceRequest {
        method: String::from("GET"),
        path: String::from("/users/42"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    let response = mock_service.handle(request, &ctx()).await;
    assert_eq!(response.status, 222);
}

#[tokio::test]
async fn respond_json_sets_content_type() {
    let mock_service = HttpMockService::new("json")
        .on("POST", "/v1/validate")
        .respond_json(&serde_json::json!({ "valid": true }))
        .expect("json");
    let request = HttpMockServiceRequest {
        method: String::from("POST"),
        path: String::from("/v1/validate"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    let response = mock_service.handle(request, &ctx()).await;
    assert_eq!(response.status, 200);
    assert!(response
        .headers
        .iter()
        .any(|(key, value)| { key == "content-type" && value == "application/json" }));
}

#[test]
fn route_match_glob_star_is_single_segment_only() {
    assert!(RouteMatch::Glob(String::from("/users/*/profile")).matches("/users/42/profile"));
    assert!(!RouteMatch::Glob(String::from("/users/*/profile")).matches("/users/a/b/profile"));
}

#[tokio::test]
async fn respond_with_declarative_header_set_cookie() {
    let mock_service = HttpMockService::new("api")
        .on("GET", "/session")
        .respond_with(
            HttpMockServiceResponse::builder()
                .header("set-cookie", "A=B")
                .body_empty(),
        );
    let request = HttpMockServiceRequest {
        method: String::from("GET"),
        path: String::from("/session"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    let response = mock_service.handle(request, &ctx()).await;
    assert_eq!(response.status, 200);
    assert!(response
        .headers
        .iter()
        .any(|(name, value)| { name == "set-cookie" && value == "A=B" }));
}

#[tokio::test]
async fn mixing_respond_respond_json_and_respond_with() {
    let mock_service = HttpMockService::new("mix")
        .on("GET", "/plain")
        .respond(418)
        .on("POST", "/json")
        .respond_json(&serde_json::json!({ "k": 2 }))
        .expect("json")
        .on("GET", "/custom")
        .respond_with(
            HttpMockServiceResponse::builder()
                .status(203)
                .header("x-custom", "yes")
                .body_empty(),
        );

    let plain = mock_service
        .handle(
            HttpMockServiceRequest {
                method: String::from("GET"),
                path: String::from("/plain"),
                query: None,
                headers: Vec::new(),
                body: Bytes::new(),
            },
            &ctx(),
        )
        .await;
    assert_eq!(plain.status, 418);

    let json_route = mock_service
        .handle(
            HttpMockServiceRequest {
                method: String::from("POST"),
                path: String::from("/json"),
                query: None,
                headers: Vec::new(),
                body: Bytes::new(),
            },
            &ctx(),
        )
        .await;
    assert_eq!(json_route.status, 200);
    assert!(json_route
        .headers
        .iter()
        .any(|(key, _)| key == "content-type"));

    let custom = mock_service
        .handle(
            HttpMockServiceRequest {
                method: String::from("GET"),
                path: String::from("/custom"),
                query: None,
                headers: Vec::new(),
                body: Bytes::new(),
            },
            &ctx(),
        )
        .await;
    assert_eq!(custom.status, 203);
    assert!(custom
        .headers
        .iter()
        .any(|(name, value)| { name == "x-custom" && value == "yes" }));
}
