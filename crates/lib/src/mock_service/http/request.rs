//! Inbound HTTP request presented to an [`super::super::HttpMockServiceTrait`].

use bytes::Bytes;

use super::super::types::MockServiceError;

/// Inbound HTTP request presented to an [`super::super::HttpMockServiceTrait`].
#[derive(Debug, Clone)]
pub struct HttpMockServiceRequest {
    /// HTTP method (uppercase by convention, e.g. `GET`).
    pub method: String,
    /// Path without query string (e.g. `/v1/orders`).
    pub path: String,
    /// Raw query string without `?`, if any.
    pub query: Option<String>,
    /// Header names are lowercased; values are lossy UTF-8 from the wire.
    pub headers: Vec<(String, String)>,
    /// Raw request body bytes.
    pub body: Bytes,
}

impl HttpMockServiceRequest {
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

    /// Request headers (name lowercased).
    pub fn headers(&self) -> &[(String, String)] {
        self.headers.as_slice()
    }

    /// Raw request body.
    pub fn bytes(&self) -> &Bytes {
        &self.body
    }

    /// Decode the body as UTF-8 text.
    pub fn text(&self) -> Result<String, MockServiceError> {
        String::from_utf8(self.body.to_vec()).map_err(|error| MockServiceError::Encoding {
            message: error.to_string(),
        })
    }

    /// Deserialize a JSON body.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, MockServiceError> {
        serde_json::from_slice(self.body.as_ref())
            .map_err(|source| MockServiceError::Deserialization { source })
    }

    /// Builds a [`HttpMockServiceRequest`] from `http` crate parts and body (optional interop, D-brief-8).
    pub fn from_http_parts(parts: http::request::Parts, body: Bytes) -> Self {
        let method = parts.method.as_str().to_owned();
        let path = parts.uri.path().to_owned();
        let query = parts.uri.query().map(str::to_owned);
        let mut headers = Vec::new();
        for (name, value) in parts.headers.iter() {
            if let Ok(value_str) = value.to_str() {
                headers.push((name.as_str().to_ascii_lowercase(), value_str.to_owned()));
            }
        }
        HttpMockServiceRequest {
            method,
            path,
            query,
            headers,
            body,
        }
    }

    /// Converts an owned [`http::Request`] with a [`Bytes`] body into a [`HttpMockServiceRequest`].
    pub fn from_http_request(request: http::Request<Bytes>) -> Self {
        let (parts, body) = request.into_parts();
        Self::from_http_parts(parts, body)
    }
}
