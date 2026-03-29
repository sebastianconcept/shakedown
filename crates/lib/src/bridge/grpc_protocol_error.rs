//! Typed failures while decoding HTTP/2 unary gRPC frames in the bridge server (`grpc-bridges`).
//!
//! Each variant maps to a [`tonic::Code`] via [`GrpcBridgeProtocolError::status_code`] and becomes
//! the gRPC status message via [`std::fmt::Display`] (displaydoc).

use displaydoc::Display;
use thiserror::Error;
use tonic::Code;

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

#[cfg(test)]
mod tests {
    use super::GrpcBridgeProtocolError;
    use tonic::Code;

    #[test]
    fn status_code_maps_each_variant() {
        // Given each protocol error variant
        // When we read its tonic Code
        // Then it matches the gRPC semantics table
        assert_eq!(
            GrpcBridgeProtocolError::RequestMethodNotPost.status_code(),
            Code::InvalidArgument
        );
        assert_eq!(
            GrpcBridgeProtocolError::RequestBodyReadFailed.status_code(),
            Code::Internal
        );
        assert_eq!(
            GrpcBridgeProtocolError::InvalidGrpcPath.status_code(),
            Code::Unimplemented
        );
        assert_eq!(
            GrpcBridgeProtocolError::UnaryFrameTooShort.status_code(),
            Code::InvalidArgument
        );
        assert_eq!(
            GrpcBridgeProtocolError::CompressedUnaryPayloadNotSupported.status_code(),
            Code::Unimplemented
        );
        assert_eq!(
            GrpcBridgeProtocolError::UnaryLengthPrefixLayoutInvalid.status_code(),
            Code::InvalidArgument
        );
        assert_eq!(
            GrpcBridgeProtocolError::UnaryPayloadLengthExceedsFrame.status_code(),
            Code::InvalidArgument
        );
        assert_eq!(
            GrpcBridgeProtocolError::GrpcOkHttpResponseBuildFailed.status_code(),
            Code::Internal
        );
        assert_eq!(
            GrpcBridgeProtocolError::TrailingMetadataHeaderNameInvalid.status_code(),
            Code::Internal
        );
        assert_eq!(
            GrpcBridgeProtocolError::TrailingMetadataHeaderValueInvalid.status_code(),
            Code::Internal
        );
        assert_eq!(
            GrpcBridgeProtocolError::GrpcStatusTrailerEncodingFailed.status_code(),
            Code::Internal
        );
    }
}
