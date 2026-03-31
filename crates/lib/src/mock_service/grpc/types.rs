//! gRPC mock service request and response types (`grpc-mock-services` feature).

use bytes::Bytes;
use prost::Message;
use tonic::Code;

use super::super::types::MockServiceError;

/// Inbound unary gRPC request presented to a [`super::r#trait::GrpcMockService`].
#[derive(Debug, Clone)]
pub struct GrpcMockServiceRequest {
    /// gRPC service name (e.g. `inventory.InventoryService`).
    pub service: String,
    /// RPC method name (e.g. `CheckStock`).
    pub method: String,
    /// Request metadata (HTTP/2 headers excluding pseudo-headers).
    pub metadata: Vec<(String, String)>,
    /// Protobuf-encoded message payload (single message, compression flag not set).
    pub body: Bytes,
}

impl GrpcMockServiceRequest {
    /// Full gRPC method path: `/Service/Method` (leading slash).
    pub fn grpc_path(&self) -> String {
        format!("/{}/{}", self.service, self.method)
    }
}

/// Outbound unary gRPC response from a [`super::r#trait::GrpcMockService`].
#[derive(Debug, Clone)]
pub struct GrpcMockServiceResponse {
    /// gRPC status code (`Code::Ok` = success). Use [`Self::status`] for non-OK replies.
    pub status_code: Code,
    /// gRPC status message (non-OK).
    pub message: Option<String>,
    /// Trailing metadata entries (merged into gRPC trailers where valid).
    pub metadata: Vec<(String, String)>,
    /// Protobuf-encoded OK response body (ignored when `status_code != Code::Ok`).
    pub body: Bytes,
}

impl GrpcMockServiceResponse {
    /// Successful response with a raw protobuf body (will be length-prefixed by the server).
    pub fn ok(message: Bytes) -> Self {
        Self {
            status_code: Code::Ok,
            message: None,
            metadata: Vec::new(),
            body: message,
        }
    }

    /// Successful response encoding a [`prost::Message`].
    pub fn ok_proto(message: &impl Message) -> Result<Self, MockServiceError> {
        let mut encoded = Vec::new();
        message
            .encode(&mut encoded)
            .map_err(|error| MockServiceError::Encoding {
                message: format!("prost encode: {error}"),
            })?;
        Ok(Self::ok(Bytes::from(encoded)))
    }

    /// Error response with the given gRPC status code and message.
    pub fn status(code: Code, message: &str) -> Self {
        Self {
            status_code: code,
            message: Some(message.to_owned()),
            metadata: Vec::new(),
            body: Bytes::new(),
        }
    }
}
