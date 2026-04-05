//! Outbound HTTP response from an [`super::super::HttpMockServiceTrait`].

use bytes::Bytes;
use http::StatusCode;
use http_body_util::Full;
use serde::Serialize;

use super::super::types::MockServiceError;
use super::response_builder::HttpMockServiceResponseBuilder;

/// Outbound HTTP response from an [`super::super::HttpMockServiceTrait`].
#[derive(Debug, Clone)]
pub struct HttpMockServiceResponse {
    /// HTTP status code.
    pub status: u16,
    /// Response headers as `(name, value)` pairs.
    pub headers: Vec<(String, String)>,
    /// Raw response body bytes.
    pub body: Bytes,
}

impl HttpMockServiceResponse {
    /// `200 OK` with an empty body.
    pub fn ok() -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            body: Bytes::new(),
        }
    }

    /// Empty body with the given status code.
    pub fn status(status_code: u16) -> Self {
        Self {
            status: status_code,
            headers: Vec::new(),
            body: Bytes::new(),
        }
    }

    /// `200 OK` with a JSON body and `Content-Type: application/json`.
    pub fn json(value: &impl Serialize) -> Result<Self, MockServiceError> {
        let raw = serde_json::to_vec(value).map_err(|source| MockServiceError::Encoding {
            message: source.to_string(),
        })?;
        Ok(Self {
            status: 200,
            headers: vec![(
                String::from("content-type"),
                String::from("application/json"),
            )],
            body: Bytes::from(raw),
        })
    }

    /// Starts a fluent builder.
    pub fn builder() -> HttpMockServiceResponseBuilder {
        HttpMockServiceResponseBuilder::new()
    }

    /// Converts into an [`http::Response`] with a [`Full`] body for serving via `hyper`.
    ///
    /// Header names must be valid HTTP tokens (use lowercase names from [`HttpMockServiceResponse::builder`]).
    pub fn into_http_response(self) -> Result<http::Response<Full<Bytes>>, MockServiceError> {
        let mut response_builder = http::Response::builder().status(self.status);
        for (name, value) in &self.headers {
            let header_name = http::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| MockServiceError::InvalidResponseHeaderName { name: name.clone() })?;
            response_builder = response_builder.header(header_name, value.as_str());
        }
        response_builder
            .body(Full::new(self.body))
            .map_err(|error| MockServiceError::HttpResponseBuild {
                message: error.to_string(),
            })
    }
}

/// Empty body, status 500 — used when [`HttpMockServiceResponse::into_http_response`] fails in the mock service server.
pub(crate) fn internal_server_error_http_response() -> http::Response<Full<Bytes>> {
    let mut response = http::Response::new(Full::new(Bytes::new()));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response
}
