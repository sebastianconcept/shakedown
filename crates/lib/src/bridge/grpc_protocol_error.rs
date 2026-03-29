//! Typed failures while decoding HTTP/2 unary gRPC frames in the bridge server (`grpc-bridges`).
//!
//! Each variant maps to a [`tonic::Code`] via [`GrpcBridgeProtocolError::status_code`] and becomes
//! the gRPC status message via [`std::fmt::Display`] (displaydoc).

use displaydoc::Display;
use thiserror::Error;
use tonic::Code;
use tonic::Status;

/// Protocol or transport failure in the gRPC bridge server request/response path.
#[derive(Debug, Display, Error)]
pub(crate) enum GrpcBridgeProtocolError {
    /// gRPC requests must use POST
    RequestMethodNotPost,
    /// failed to read gRPC request body
    RequestBodyReadFailed,
    /// gRPC path is not a valid `/Service/Method` URI
    InvalidGrpcPath,
    /// gRPC unary frame is shorter than the 5-byte gRPC length prefix header
    UnaryFrameTooShort,
    /// compressed gRPC request bodies are not supported
    CompressedUnaryPayloadNotSupported,
    /// gRPC length-prefix slice layout is invalid (invariant: frame length is at least 5 bytes)
    UnaryLengthPrefixLayoutInvalid,
    /// gRPC length-prefix declares more message bytes than present in the body
    UnaryPayloadLengthExceedsFrame,
    /// failed to build HTTP response for OK gRPC unary reply
    GrpcOkHttpResponseBuildFailed,
    /// trailing metadata name is not a valid HTTP header name
    TrailingMetadataHeaderNameInvalid,
    /// trailing metadata value is not a valid HTTP header value
    TrailingMetadataHeaderValueInvalid,
    /// failed to encode gRPC status into HTTP/2 trailer headers
    GrpcStatusTrailerEncodingFailed,
}

impl GrpcBridgeProtocolError {
    pub(crate) fn status_code(&self) -> Code {
        match self {
            GrpcBridgeProtocolError::RequestMethodNotPost
            | GrpcBridgeProtocolError::UnaryFrameTooShort
            | GrpcBridgeProtocolError::UnaryLengthPrefixLayoutInvalid
            | GrpcBridgeProtocolError::UnaryPayloadLengthExceedsFrame => Code::InvalidArgument,
            GrpcBridgeProtocolError::InvalidGrpcPath
            | GrpcBridgeProtocolError::CompressedUnaryPayloadNotSupported => Code::Unimplemented,
            GrpcBridgeProtocolError::RequestBodyReadFailed
            | GrpcBridgeProtocolError::GrpcOkHttpResponseBuildFailed
            | GrpcBridgeProtocolError::TrailingMetadataHeaderNameInvalid
            | GrpcBridgeProtocolError::TrailingMetadataHeaderValueInvalid
            | GrpcBridgeProtocolError::GrpcStatusTrailerEncodingFailed => Code::Internal,
        }
    }
}

impl From<GrpcBridgeProtocolError> for Status {
    fn from(error: GrpcBridgeProtocolError) -> Self {
        Status::new(error.status_code(), error.to_string())
    }
}
