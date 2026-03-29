//! Bridge request/response types, context, resources, and errors.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use displaydoc::Display;
use http_body_util::Full;
use serde::Serialize;
use thiserror::Error;

#[cfg(feature = "kafka")]
use rdkafka::producer::FutureProducer;

#[cfg(feature = "rabbitmq")]
use lapin::Channel;

#[cfg(feature = "kafka")]
use crate::resources::KafkaBindings;

#[cfg(feature = "mariadb")]
use crate::resources::MariaDBBindings;
#[cfg(feature = "postgres")]
use crate::resources::PostgresBindings;
#[cfg(feature = "rabbitmq")]
use crate::resources::RabbitBindings;

/// Errors surfaced by bridge request helpers and response builders.
#[derive(Debug, Display, Error)]
pub enum BridgeError {
    /// JSON deserialization failed: {source}
    Deserialization {
        #[from]
        source: serde_json::Error,
    },
    /// encoding failed: {message}
    Encoding {
        /// Human-readable detail (UTF-8, JSON serialization, …).
        message: String,
    },
    /// bridge server failed to start: {source}
    ServerStart {
        /// Underlying I/O error (bind/listen, …).
        #[source]
        source: std::io::Error,
    },
    /// bridge `{name}` is not registered
    NotRegistered {
        /// Logical bridge name passed to harness `Context` APIs.
        name: String,
    },
}

/// Inbound HTTP request presented to an [`super::HttpBridge`].
#[derive(Debug, Clone)]
pub struct BridgeRequest {
    /// HTTP method (uppercase by convention, e.g. `GET`).
    pub method: String,
    /// Path without query string (e.g. `/v1/orders`).
    pub path: String,
    /// Raw query string without `?`, if any.
    pub query: Option<String>,
    /// Header names are lowercased; values are lossy UTF-8 from the wire.
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

impl BridgeRequest {
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
    pub fn text(&self) -> Result<String, BridgeError> {
        String::from_utf8(self.body.to_vec()).map_err(|error| BridgeError::Encoding {
            message: error.to_string(),
        })
    }

    /// Deserialize a JSON body.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, BridgeError> {
        serde_json::from_slice(self.body.as_ref()).map_err(BridgeError::from)
    }

    /// Builds a [`BridgeRequest`] from `http` crate parts and body (optional interop, D-brief-8).
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
        BridgeRequest {
            method,
            path,
            query,
            headers,
            body,
        }
    }

    /// Converts an owned [`http::Request`] with a [`Bytes`] body into a [`BridgeRequest`].
    pub fn from_http_request(request: http::Request<Bytes>) -> Self {
        let (parts, body) = request.into_parts();
        Self::from_http_parts(parts, body)
    }
}

/// Outbound HTTP response from an [`super::HttpBridge`].
#[derive(Debug, Clone)]
pub struct BridgeResponse {
    /// HTTP status code.
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

impl BridgeResponse {
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
    pub fn json(value: &impl Serialize) -> Result<Self, BridgeError> {
        let raw = serde_json::to_vec(value).map_err(|source| BridgeError::Encoding {
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
    pub fn builder() -> BridgeResponseBuilder {
        BridgeResponseBuilder::new()
    }

    /// Converts into an [`http::Response`] with a [`Full`] body for serving via `hyper`.
    ///
    /// Header names must be valid HTTP tokens (use lowercase names from [`BridgeResponse::builder`]).
    pub fn into_http_response(self) -> http::Response<Full<Bytes>> {
        let mut response_builder = http::Response::builder().status(self.status);
        for (name, value) in &self.headers {
            let header_name = http::HeaderName::from_bytes(name.as_bytes())
                .expect("bridge response header names must be valid HTTP tokens");
            response_builder = response_builder.header(header_name, value.as_str());
        }
        response_builder
            .body(Full::new(self.body))
            .expect("valid bridge HTTP response")
    }
}

/// Fluent builder for [`BridgeResponse`].
#[derive(Debug)]
pub struct BridgeResponseBuilder {
    status: u16,
    headers: Vec<(String, String)>,
}

impl BridgeResponseBuilder {
    fn new() -> Self {
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
    pub fn body_empty(self) -> BridgeResponse {
        BridgeResponse {
            status: self.status,
            headers: self.headers,
            body: Bytes::new(),
        }
    }

    /// Finish with a raw body.
    pub fn body_bytes(self, body: impl Into<Bytes>) -> BridgeResponse {
        BridgeResponse {
            status: self.status,
            headers: self.headers,
            body: body.into(),
        }
    }

    /// Serialize JSON as the body and set `Content-Type: application/json` unless already present.
    pub fn json(mut self, value: &impl Serialize) -> Result<BridgeResponse, BridgeError> {
        let raw = serde_json::to_vec(value).map_err(|source| BridgeError::Encoding {
            message: source.to_string(),
        })?;
        let has_json = self.headers.iter().any(|(key, _)| key == "content-type");
        if !has_json {
            self.headers.push((
                String::from("content-type"),
                String::from("application/json"),
            ));
        }
        Ok(BridgeResponse {
            status: self.status,
            headers: self.headers,
            body: Bytes::from(raw),
        })
    }
}

/// Shared infrastructure handles available to bridge implementations while handling a request.
///
/// Populated by the harness after resource provision; use [`BridgeResources::default`] for unit tests
/// when no infrastructure is wired.
#[derive(Default)]
pub struct BridgeResources {
    #[cfg(feature = "kafka")]
    pub kafka_bindings: Option<KafkaBindings>,
    #[cfg(feature = "kafka")]
    pub kafka_producer: Option<FutureProducer>,
    #[cfg(feature = "rabbitmq")]
    pub rabbit_bindings: Option<RabbitBindings>,
    #[cfg(feature = "rabbitmq")]
    pub rabbit_channel: Option<Channel>,
    #[cfg(feature = "postgres")]
    pub postgres_bindings: Option<PostgresBindings>,
    #[cfg(feature = "mariadb")]
    pub mariadb_bindings: Option<MariaDBBindings>,
}

impl fmt::Debug for BridgeResources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("BridgeResources");
        #[cfg(feature = "kafka")]
        {
            s.field("kafka_bindings", &self.kafka_bindings);
            s.field(
                "kafka_producer",
                &self.kafka_producer.as_ref().map(|_| "<FutureProducer>"),
            );
        }
        #[cfg(feature = "rabbitmq")]
        {
            s.field("rabbit_bindings", &self.rabbit_bindings);
            s.field(
                "rabbit_channel",
                &self.rabbit_channel.as_ref().map(|_| "<lapin::Channel>"),
            );
        }
        #[cfg(feature = "postgres")]
        {
            s.field("postgres_bindings", &self.postgres_bindings);
        }
        #[cfg(feature = "mariadb")]
        {
            s.field("mariadb_bindings", &self.mariadb_bindings);
        }
        s.finish()
    }
}

/// Read-only context passed into [`super::HttpBridge::handle`].
#[derive(Debug)]
pub struct BridgeContext {
    /// Active scenario identifier.
    pub scenario_id: String,
    /// Logical name of this bridge instance.
    pub bridge_name: String,
    /// Shared resource handles (Kafka, AMQP, …).
    pub resources: Arc<BridgeResources>,
}

impl BridgeContext {
    /// Construct a context for tests or harness wiring.
    pub fn new(
        scenario_id: impl Into<String>,
        bridge_name: impl Into<String>,
        resources: Arc<BridgeResources>,
    ) -> Self {
        Self {
            scenario_id: scenario_id.into(),
            bridge_name: bridge_name.into(),
            resources,
        }
    }
}

#[cfg(all(test, feature = "bridges"))]
mod tests {
    use super::*;

    #[test]
    fn bridge_request_json_roundtrip() {
        let request = BridgeRequest {
            method: String::from("POST"),
            path: String::from("/v1/echo"),
            query: None,
            headers: Vec::new(),
            body: Bytes::from(r#"{"hello":"world"}"#),
        };
        let parsed: serde_json::Value = request.json().expect("json");
        assert_eq!(parsed["hello"], "world");
    }

    #[test]
    fn bridge_response_builder_json_sets_content_type() {
        let response = BridgeResponse::builder()
            .status(201)
            .header("X-Test", "1")
            .json(&serde_json::json!({ "id": 42 }))
            .expect("json");
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
        let http_request = http::Request::builder()
            .method("GET")
            .uri("/items?limit=10")
            .header("Accept", "application/json")
            .body(Bytes::new())
            .expect("request");
        let bridge_request = BridgeRequest::from_http_request(http_request);
        assert_eq!(bridge_request.method(), "GET");
        assert_eq!(bridge_request.path(), "/items");
        assert_eq!(bridge_request.query(), Some("limit=10"));
        assert!(bridge_request
            .headers()
            .iter()
            .any(|(key, value)| key == "accept" && value == "application/json"));
    }
}
