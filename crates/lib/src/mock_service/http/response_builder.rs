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

impl Default for HttpMockServiceResponseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpMockServiceResponseBuilder {
    /// Starts a builder with status **200** and no headers.
    pub fn new() -> Self {
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

    /// Finish with a raw body (copied). Accepts `AsRef<[u8]>`, including `b"…"` literals.
    pub fn body_bytes(self, body: impl AsRef<[u8]>) -> HttpMockServiceResponse {
        HttpMockServiceResponse {
            status: self.status,
            headers: self.headers,
            body: Bytes::copy_from_slice(body.as_ref()),
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

#[cfg(test)]
mod body_bytes_tests {
    use bytes::Bytes;

    use super::HttpMockServiceResponseBuilder;

    #[test]
    fn body_bytes_accepts_byte_string_literal() {
        let response = HttpMockServiceResponseBuilder::new().body_bytes(b"hello");
        assert_eq!(response.body.as_ref(), b"hello");
    }

    #[test]
    fn body_bytes_accepts_vec_slice_and_bytes() {
        let from_vec = HttpMockServiceResponseBuilder::new().body_bytes(vec![1u8, 2, 3]);
        assert_eq!(from_vec.body.as_ref(), &[1, 2, 3]);

        let slice: &[u8] = &[4, 5];
        let from_slice = HttpMockServiceResponseBuilder::new().body_bytes(slice);
        assert_eq!(from_slice.body.as_ref(), slice);

        let from_bytes =
            HttpMockServiceResponseBuilder::new().body_bytes(Bytes::from_static(b"ok"));
        assert_eq!(from_bytes.body.as_ref(), b"ok");
    }
}
