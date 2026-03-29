//! gRPC bridge request and response types (`grpc-bridges` feature).

use bytes::Bytes;
use prost::Message;
use tonic::Code;

use super::types::BridgeError;

/// Inbound unary gRPC request presented to a [`super::traits::GrpcBridge`].
#[derive(Debug, Clone)]
pub struct GrpcBridgeRequest {
    /// gRPC service name (e.g. `inventory.InventoryService`).
    pub service: String,
    /// RPC method name (e.g. `CheckStock`).
    pub method: String,
    /// Request metadata (HTTP/2 headers excluding pseudo-headers).
    pub metadata: Vec<(String, String)>,
    /// Protobuf-encoded message payload (single message, compression flag not set).
    pub body: Bytes,
}

impl GrpcBridgeRequest {
    /// Full gRPC method path: `/Service/Method` (leading slash).
    pub fn grpc_path(&self) -> String {
        format!("/{}/{}", self.service, self.method)
    }
}

/// Outbound unary gRPC response from a [`super::traits::GrpcBridge`].
#[derive(Debug, Clone)]
pub struct GrpcBridgeResponse {
    /// gRPC status code (`0` = OK). Use [`tonic::Code`] when building via [`Self::status`].
    pub status_code: u32,
    /// gRPC status message (non-OK).
    pub message: Option<String>,
    /// Trailing metadata entries (merged into gRPC trailers where valid).
    pub metadata: Vec<(String, String)>,
    /// Protobuf-encoded OK response body (ignored when `status_code != 0`).
    pub body: Bytes,
}

impl GrpcBridgeResponse {
    /// Successful response with a raw protobuf body (will be length-prefixed by the server).
    pub fn ok(message: Bytes) -> Self {
        Self {
            status_code: Code::Ok as u32,
            message: None,
            metadata: Vec::new(),
            body: message,
        }
    }

    /// Successful response encoding a [`prost::Message`].
    pub fn ok_proto(message: &impl Message) -> Result<Self, BridgeError> {
        let mut encoded = Vec::new();
        message
            .encode(&mut encoded)
            .map_err(|error| BridgeError::Encoding {
                message: format!("prost encode: {error}"),
            })?;
        Ok(Self::ok(Bytes::from(encoded)))
    }

    /// Error response with the given gRPC status code and message.
    pub fn status(code: Code, message: &str) -> Self {
        Self {
            status_code: code as u32,
            message: Some(message.to_owned()),
            metadata: Vec::new(),
            body: Bytes::new(),
        }
    }
}
