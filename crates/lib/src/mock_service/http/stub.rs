//! Declarative HTTP mock service (`HttpMockService`) implementing [`HttpMockServiceTrait`](super::super::HttpMockServiceTrait).

use async_trait::async_trait;
use serde::Serialize;

use super::r#trait::HttpMockServiceTrait;
use super::super::types::{MockServiceContext, MockServiceError};
use super::request::HttpMockServiceRequest;
use super::response::HttpMockServiceResponse;

/// Path matching mode for a stub route (D-brief-6: enum-based, registration order, not a map).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteMatch {
    /// Path must match exactly (including leading `/` if present in the pattern).
    Exact(String),
    /// Path pattern where `*` matches exactly one non-empty path segment (see [`RouteMatch::matches`]).
    Glob(String),
}

impl RouteMatch {
    /// Classify a path string: any `*` enables glob semantics for the whole pattern.
    pub(crate) fn for_path(path: impl Into<String>) -> Self {
        let path_string = path.into();
        if path_string.contains('*') {
            RouteMatch::Glob(path_string)
        } else {
            RouteMatch::Exact(path_string)
        }
    }

    /// Whether an inbound request path matches this route pattern for the same HTTP method.
    pub fn matches(&self, path: &str) -> bool {
        match self {
            RouteMatch::Exact(expected) => expected == path,
            RouteMatch::Glob(pattern) => glob_matches_single_segment(pattern, path),
        }
    }
}

fn path_segments(path: &str) -> Vec<&str> {
    path.trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect()
}

fn glob_matches_single_segment(pattern: &str, path: &str) -> bool {
    let pattern_segments = path_segments(pattern);
    let path_segments = path_segments(path);
    if pattern_segments.len() != path_segments.len() {
        return false;
    }
    pattern_segments
        .iter()
        .zip(path_segments.iter())
        .all(|(pattern_segment, path_segment)| {
            *pattern_segment == "*" || pattern_segment == path_segment
        })
}

fn normalize_method(method: &str) -> String {
    method.to_ascii_uppercase()
}

/// Builder for route → response mappings; implements [`HttpMockServiceTrait`](super::HttpMockServiceTrait).
#[derive(Debug, Clone)]
pub struct HttpMockService {
    name: String,
    routes: Vec<(String, RouteMatch, HttpMockServiceResponse)>,
    default_response: HttpMockServiceResponse,
}

/// Fluent step after [`HttpMockService::on`]: finish the route with a status or JSON body.
#[derive(Debug)]
pub struct HttpMockServiceRoutePending {
    mock_service: HttpMockService,
    method: String,
    path: String,
}

impl HttpMockService {
    /// Starts a mock service builder. Default response for unmatched routes is **404** until
    /// [`HttpMockService::default_response`] is called.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            routes: Vec::new(),
            default_response: HttpMockServiceResponse::status(404),
        }
    }

    /// Logical mock service name (for diagnostics; routing does not use this field).
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Sets the fallback response when no route matches (default **404**).
    pub fn default_response(mut self, status: u16) -> Self {
        self.default_response = HttpMockServiceResponse::status(status);
        self
    }

    /// Registers a route for the given HTTP method and path. Path may use `*` for a single-segment
    /// glob (see [`RouteMatch::Glob`]).
    pub fn on(self, method: impl AsRef<str>, path: impl Into<String>) -> HttpMockServiceRoutePending {
        HttpMockServiceRoutePending {
            mock_service: self,
            method: normalize_method(method.as_ref()),
            path: path.into(),
        }
    }
}

impl HttpMockServiceRoutePending {
    /// Empty body with the given status.
    pub fn respond(self, status: u16) -> HttpMockService {
        let route_match = RouteMatch::for_path(self.path);
        let mut mock_service = self.mock_service;
        mock_service.routes
            .push((self.method, route_match, HttpMockServiceResponse::status(status)));
        mock_service
    }

    /// JSON body with `Content-Type: application/json` (and status **200** unless set in a later builder).
    pub fn respond_json(self, value: &impl Serialize) -> Result<HttpMockService, MockServiceError> {
        let response = HttpMockServiceResponse::json(value)?;
        let route_match = RouteMatch::for_path(self.path);
        let mut mock_service = self.mock_service;
        mock_service.routes.push((self.method, route_match, response));
        Ok(mock_service)
    }
}

#[async_trait]
impl HttpMockServiceTrait for HttpMockService {
    async fn handle(&self, request: HttpMockServiceRequest, _context: &MockServiceContext) -> HttpMockServiceResponse {
        let method = normalize_method(request.method());
        let path = request.path();
        for (registered_method, route_match, response) in &self.routes {
            if registered_method != &method {
                continue;
            }
            if route_match.matches(path) {
                return response.clone();
            }
        }
        self.default_response.clone()
    }
}

#[cfg(all(test, feature = "mock-services"))]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::super::super::types::MockServiceResources;
    use super::*;

    fn ctx() -> MockServiceContext {
        MockServiceContext::new("s1", "stub", Arc::new(MockServiceResources::default()))
    }

    #[tokio::test]
    async fn exact_match_returns_registered_response() {
        let mock_service = HttpMockService::new("api").on("GET", "/health").respond(204);
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
}
