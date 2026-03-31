//! Fluent builder for [`super::response::HttpMockServiceResponse`].

use bytes::Bytes;
use serde::Serialize;

use super::super::types::MockServiceError;
use super::response::HttpMockServiceResponse;

/// Fluent builder for [`HttpMockServiceResponse`].
#[derive(Debug)]
pub struct HttpMockServiceResponseBuilder {
    status: u16,
    headers: Vec<(String, String)>,
}

impl HttpMockServiceResponseBuilder {
    pub(crate) fn new() -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
        }
    }

    /// Set the HTTP status code.
    pub fn status(mut self, status_code: u16) -> Self {
        self.status = status_code;
        self
    }

    /// Append a header (name is stored lowercased).
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let mut name_string = name.into();
        name_string.make_ascii_lowercase();
        self.headers.push((name_string, value.into()));
        self
    }

    /// Finish with an empty body.
    pub fn body_empty(self) -> HttpMockServiceResponse {
        HttpMockServiceResponse {
            status: self.status,
            headers: self.headers,
            body: Bytes::new(),
        }
    }

    /// Finish with a raw body.
    pub fn body_bytes(self, body: impl Into<Bytes>) -> HttpMockServiceResponse {
        HttpMockServiceResponse {
            status: self.status,
            headers: self.headers,
            body: body.into(),
        }
    }

    /// Serialize JSON as the body and set `Content-Type: application/json` unless already present.
    pub fn json(
        mut self,
        value: &impl Serialize,
    ) -> Result<HttpMockServiceResponse, MockServiceError> {
        let raw = serde_json::to_vec(value).map_err(|source| MockServiceError::Encoding {
            message: source.to_string(),
        })?;
        let has_json = self.headers.iter().any(|(key, _)| key == "content-type");
        if !has_json {
            self.headers.push((
                String::from("content-type"),
                String::from("application/json"),
            ));
        }
        Ok(HttpMockServiceResponse {
            status: self.status,
            headers: self.headers,
            body: Bytes::from(raw),
        })
    }
}
