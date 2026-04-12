use std::sync::Arc;

use bytes::Bytes;
use http::{Method, StatusCode};

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
    // Given a GET /health route that responds with 204.
    let mock_service = HttpMockService::new("api")
        .on("GET", "/health")
        .respond(StatusCode::NO_CONTENT.into());
    let request = HttpMockServiceRequest {
        method: Method::GET,
        path: String::from("/health"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    // When the mock handles that exact path and method.
    let response = mock_service.handle(request, &ctx()).await;
    // Then the registered status is returned.
    assert_eq!(response.status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn glob_matches_single_segment() {
    // Given a glob route with one `*` segment.
    let mock_service = HttpMockService::new("users")
        .on("GET", "/users/*/profile")
        .respond(StatusCode::OK.into());
    let request = HttpMockServiceRequest {
        method: Method::GET,
        path: String::from("/users/42/profile"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    // When the path matches the single glob segment.
    let response = mock_service.handle(request, &ctx()).await;
    // Then the route matches and returns 200.
    assert_eq!(response.status, StatusCode::OK);
}

#[tokio::test]
async fn unmatched_uses_default_404() {
    // Given a mock with only GET /known and the default fallback.
    let mock_service = HttpMockService::new("api")
        .on("GET", "/known")
        .respond(StatusCode::OK.into());
    let request = HttpMockServiceRequest {
        method: Method::GET,
        path: String::from("/other"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    // When the request path does not match any route.
    let response = mock_service.handle(request, &ctx()).await;
    // Then the built-in default 404 applies.
    assert_eq!(response.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn unmatched_uses_configured_default() {
    // Given a mock with a custom default_response for unmatched paths.
    let mock_service = HttpMockService::new("api")
        .on("GET", "/known")
        .respond(StatusCode::OK.into())
        .default_response(StatusCode::SERVICE_UNAVAILABLE.into());
    let request = HttpMockServiceRequest {
        method: Method::GET,
        path: String::from("/missing"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    // When no route matches.
    let response = mock_service.handle(request, &ctx()).await;
    // Then the configured default status is returned.
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn registration_order_first_match_wins() {
    // Given overlapping routes where the glob is registered before the exact path.
    let mock_service = HttpMockService::new("order")
        .on("GET", "/users/*")
        .respond(StatusCode::SWITCHING_PROTOCOLS.into())
        .on("GET", "/users/42")
        .respond(StatusCode::PROCESSING.into());
    let request = HttpMockServiceRequest {
        method: Method::GET,
        path: String::from("/users/42"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    // When both could match.
    let response = mock_service.handle(request, &ctx()).await;
    // Then the first registered matching route wins.
    assert_eq!(response.status, StatusCode::SWITCHING_PROTOCOLS);
}

#[tokio::test]
async fn registration_order_exact_before_glob() {
    // Given overlapping routes where the exact path is registered before the glob.
    let mock_service = HttpMockService::new("order")
        .on("GET", "/users/42")
        .respond(StatusCode::PROCESSING.into())
        .on("GET", "/users/*")
        .respond(StatusCode::SWITCHING_PROTOCOLS.into());
    let request = HttpMockServiceRequest {
        method: Method::GET,
        path: String::from("/users/42"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    // When the path matches the exact route and the glob.
    let response = mock_service.handle(request, &ctx()).await;
    // Then the earlier exact route takes precedence over the later glob.
    assert_eq!(response.status, StatusCode::PROCESSING);
}

#[tokio::test]
async fn respond_json_sets_content_type() {
    // Given a POST route built with respond_json.
    let mock_service = HttpMockService::new("json")
        .on("POST", "/v1/validate")
        .respond_json(&serde_json::json!({ "valid": true }))
        .expect("json");
    let request = HttpMockServiceRequest {
        method: Method::POST,
        path: String::from("/v1/validate"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    // When the mock handles the request.
    let response = mock_service.handle(request, &ctx()).await;
    // Then status is 200 and Content-Type is application/json.
    assert_eq!(response.status, StatusCode::OK);
    assert!(response
        .headers
        .iter()
        .any(|(key, value)| { key == "content-type" && value == "application/json" }));
}

#[test]
fn route_match_glob_star_is_single_segment_only() {
    // Given a glob pattern with one `*` segment.
    let pattern = RouteMatch::Glob(String::from("/users/*/profile"));
    // When paths are matched.
    // Then one segment matches and multiple segments do not.
    assert!(pattern.matches("/users/42/profile"));
    assert!(!pattern.matches("/users/a/b/profile"));
}

#[test]
fn route_match_for_path_trailing_glob_star_star_yields_prefix() {
    // Given a path pattern ending in `/**`.
    // When it is classified for routing.
    // Then it becomes a Prefix match on the parent segment.
    assert_eq!(
        RouteMatch::for_path("/api/**"),
        RouteMatch::Prefix("/api".to_string())
    );
}

#[test]
fn route_match_prefix_d4_exact_and_continuation() {
    // Given a Prefix route matcher.
    let prefix = RouteMatch::Prefix("/v8/finance/chart".to_string());
    // When the request path is exact or continues under the prefix.
    // Then exact and longer paths match; similar-but-wrong paths do not.
    assert!(prefix.matches("/v8/finance/chart"));
    assert!(prefix.matches("/v8/finance/chart/AAPL"));
    assert!(!prefix.matches("/v8/finance/charter"));
}

#[test]
fn route_match_prefix_on_get_api_glob_matches_nested_path() {
    // Given `/api/**` parsed for routing.
    let route_match = RouteMatch::for_path("/api/**");
    // When it is used to match a nested path.
    // Then it is a Prefix and matches deep paths under /api.
    assert!(matches!(route_match, RouteMatch::Prefix(_)));
    assert!(route_match.matches("/api/foo/bar/baz"));
}

#[tokio::test]
async fn on_get_api_glob_responds_for_nested_path() {
    // Given GET /api/** registered as 204.
    let mock_service = HttpMockService::new("api")
        .on("GET", "/api/**")
        .respond(StatusCode::NO_CONTENT.into());
    let request = HttpMockServiceRequest {
        method: Method::GET,
        path: String::from("/api/foo/bar/baz"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    // When a deeply nested path under /api is requested.
    let response = mock_service.handle(request, &ctx()).await;
    // Then the prefix route responds.
    assert_eq!(response.status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn on_prefix_matches_nested_path() {
    // Given on_prefix("GET", "/api").
    let mock_service = HttpMockService::new("api")
        .on_prefix("GET", "/api")
        .respond(StatusCode::OK.into());
    let request = HttpMockServiceRequest {
        method: Method::GET,
        path: String::from("/api/foo/bar/baz"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    // When a nested path under that prefix is requested.
    let response = mock_service.handle(request, &ctx()).await;
    // Then the route matches.
    assert_eq!(response.status, StatusCode::OK);
}

#[test]
fn route_match_for_path_root_double_star_is_catch_all_not_empty_prefix() {
    // Given the literal pattern `"/**"` (e.g. `on_prefix("", …)` → `"/**"`)
    // When it is classified for routing
    // Then we use CatchAll instead of Prefix("") so the stub does not match almost every path
    assert_eq!(RouteMatch::for_path("/**"), RouteMatch::CatchAll);
}

#[test]
fn route_match_catch_all_matches_any_path() {
    // Given a CatchAll route (root wildcard)
    let catch_all = RouteMatch::CatchAll;
    // When various request paths are matched
    // Then every path matches
    assert!(catch_all.matches(""));
    assert!(catch_all.matches("/"));
    assert!(catch_all.matches("/a/b/c"));
}

#[tokio::test]
async fn on_get_root_glob_responds_for_any_path() {
    // Given a declarative route registered as `GET /**`
    let mock_service = HttpMockService::new("root")
        .on("GET", "/**")
        .respond(StatusCode::PROCESSING.into());
    // When a nested path is requested
    let response = mock_service
        .handle(
            HttpMockServiceRequest {
                method: Method::GET,
                path: String::from("/nested/path"),
                query: None,
                headers: Vec::new(),
                body: Bytes::new(),
            },
            &ctx(),
        )
        .await;
    // Then the catch-all route handles it
    assert_eq!(response.status, StatusCode::PROCESSING);
}

#[tokio::test]
async fn on_prefix_empty_string_registers_root_catch_all() {
    // Given `on_prefix` with an empty prefix (pattern becomes `"/**"`).
    let mock_service = HttpMockService::new("root")
        .on_prefix("GET", "")
        .respond(StatusCode::CREATED.into());
    // When any path is requested.
    let response = mock_service
        .handle(
            HttpMockServiceRequest {
                method: Method::GET,
                path: String::from("/v1/anything"),
                query: None,
                headers: Vec::new(),
                body: Bytes::new(),
            },
            &ctx(),
        )
        .await;
    // Then the catch-all responds with the registered status.
    assert_eq!(response.status, StatusCode::CREATED);
}

#[tokio::test]
async fn respond_with_declarative_header_set_cookie() {
    // Given respond_with a Set-Cookie header and empty body.
    let mock_service = HttpMockService::new("api")
        .on("GET", "/session")
        .respond_with(
            HttpMockServiceResponse::builder()
                .header("set-cookie", "A=B")
                .body_empty(),
        );
    let request = HttpMockServiceRequest {
        method: Method::GET,
        path: String::from("/session"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    // When the mock handles the request.
    let response = mock_service.handle(request, &ctx()).await;
    // Then status is 200 and Set-Cookie is present.
    assert_eq!(response.status, StatusCode::OK);
    assert!(response
        .headers
        .iter()
        .any(|(name, value)| { name == "set-cookie" && value == "A=B" }));
}

#[tokio::test]
async fn respond_with_headers_sets_status_headers_body() {
    // Given a declarative mock route with status, headers, and raw JSON body.
    let mock_service = HttpMockService::new("api")
        .on("GET", "/v1/data")
        .respond_with_headers(
            StatusCode::CREATED.into(),
            &[
                ("content-type", "application/json"),
                ("x-request-id", "abc"),
            ],
            br#"{"ok":true}"#,
        );
    let request = HttpMockServiceRequest {
        method: Method::GET,
        path: String::from("/v1/data"),
        query: None,
        headers: Vec::new(),
        body: Bytes::new(),
    };
    // When the mock handles a matching GET.
    let response = mock_service.handle(request, &ctx()).await;
    // Then status, headers, and body match the route.
    assert_eq!(response.status, StatusCode::CREATED);
    assert!(response
        .headers
        .iter()
        .any(|(key, value)| { key == "content-type" && value == "application/json" }));
    assert!(response
        .headers
        .iter()
        .any(|(key, value)| { key == "x-request-id" && value == "abc" }));
    assert_eq!(response.body.as_ref(), br#"{"ok":true}"#);
}

#[tokio::test]
async fn respond_with_headers_does_not_affect_other_routes() {
    // Given one route built with respond_with_headers and another with respond only.
    let mock_service = HttpMockService::new("api")
        .on("GET", "/with-headers")
        .respond_with_headers(StatusCode::OK.into(), &[("x-mode", "short")], b"short")
        .on("GET", "/plain")
        .respond(StatusCode::IM_A_TEAPOT.into());

    // When we handle GET /with-headers.
    let short = mock_service
        .handle(
            HttpMockServiceRequest {
                method: Method::GET,
                path: String::from("/with-headers"),
                query: None,
                headers: Vec::new(),
                body: Bytes::new(),
            },
            &ctx(),
        )
        .await;
    // Then that route returns the configured headers and body.
    assert_eq!(short.status, StatusCode::OK);
    assert!(short
        .headers
        .iter()
        .any(|(name, value)| { name == "x-mode" && value == "short" }));
    assert_eq!(short.body.as_ref(), b"short");

    // When we handle GET /plain.
    let plain = mock_service
        .handle(
            HttpMockServiceRequest {
                method: Method::GET,
                path: String::from("/plain"),
                query: None,
                headers: Vec::new(),
                body: Bytes::new(),
            },
            &ctx(),
        )
        .await;
    // Then the other route is unchanged (no extra headers from respond_with_headers).
    assert_eq!(plain.status, StatusCode::IM_A_TEAPOT);
    assert!(plain.headers.is_empty());
}

#[tokio::test]
async fn mixing_respond_respond_json_and_respond_with() {
    // Given one mock with respond, respond_json, and respond_with routes.
    let mock_service = HttpMockService::new("mix")
        .on("GET", "/plain")
        .respond(StatusCode::IM_A_TEAPOT.into())
        .on("POST", "/json")
        .respond_json(&serde_json::json!({ "k": 2 }))
        .expect("json")
        .on("GET", "/custom")
        .respond_with(
            HttpMockServiceResponse::builder()
                .status(StatusCode::NON_AUTHORITATIVE_INFORMATION)
                .header("x-custom", "yes")
                .body_empty(),
        );

    // When GET /plain is handled.
    let plain = mock_service
        .handle(
            HttpMockServiceRequest {
                method: Method::GET,
                path: String::from("/plain"),
                query: None,
                headers: Vec::new(),
                body: Bytes::new(),
            },
            &ctx(),
        )
        .await;
    // Then respond()'s status is used.
    assert_eq!(plain.status, StatusCode::IM_A_TEAPOT);

    // When POST /json is handled.
    let json_route = mock_service
        .handle(
            HttpMockServiceRequest {
                method: Method::POST,
                path: String::from("/json"),
                query: None,
                headers: Vec::new(),
                body: Bytes::new(),
            },
            &ctx(),
        )
        .await;
    // Then JSON defaults apply including Content-Type.
    assert_eq!(json_route.status, StatusCode::OK);
    assert!(json_route
        .headers
        .iter()
        .any(|(key, _)| key == "content-type"));

    // When GET /custom is handled.
    let custom = mock_service
        .handle(
            HttpMockServiceRequest {
                method: Method::GET,
                path: String::from("/custom"),
                query: None,
                headers: Vec::new(),
                body: Bytes::new(),
            },
            &ctx(),
        )
        .await;
    // Then the custom builder status and headers are returned.
    assert_eq!(custom.status, StatusCode::NON_AUTHORITATIVE_INFORMATION);
    assert!(custom
        .headers
        .iter()
        .any(|(name, value)| { name == "x-custom" && value == "yes" }));
}
