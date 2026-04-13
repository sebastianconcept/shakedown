//! [`TcpCodec`] — user-defined framing for TCP mock services.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::mock_service::types::MockServiceError;

/// User-implemented framing for TCP mock request/response pairs (v1: one in-flight message per
/// direction; sequential request/response on a connection).
///
/// Use [`bytes::BytesMut`] as the decode buffer so partial reads accumulate the same way as
/// `tokio_util::codec` patterns.
pub trait TcpCodec: Send + Sync + 'static {
    /// Decoded inbound request (e.g. RPC name + payload, or raw bytes).
    type Request: Send + 'static;
    /// Outbound response bytes after [`Self::encode`].
    type Response: Send + 'static;

    /// Consume bytes from `buffer` and return a complete request, or `None` if more bytes are
    /// needed.
    fn decode(&self, buffer: &mut BytesMut) -> Result<Option<Self::Request>, MockServiceError>;

    /// Append the wire-format response to `buffer`.
    fn encode(
        &self,
        response: Self::Response,
        buffer: &mut BytesMut,
    ) -> Result<(), MockServiceError>;

    /// Short label stored in [`crate::mock_service::RecordedCall::method`] (assert with
    /// [`crate::mock_service::MockServiceHandle::assert_path_called_times`] using `path == ""`).
    fn request_method_label(request: &Self::Request) -> String;

    /// Raw request bytes for the failure timeline / diagnostics (may differ from on-wire bytes).
    fn request_bytes_for_log(request: &Self::Request) -> Bytes;

    /// Raw response bytes for recordings.
    fn response_bytes_for_log(response: &Self::Response) -> Bytes;
}

/// Wire format for the declarative [`super::service::TcpMockService`] stub (documented, stable for tests):
///
/// - **Request:** `u32` big-endian `payload_len` (number of bytes after this field), then `u16`
///   big-endian method name length in bytes, then UTF-8 method name, then opaque body bytes.
/// - **Response:** `u32` big-endian body length, then body bytes.
#[derive(Debug, Default, Clone, Copy)]
pub struct DeclarativeTcpCodec;

/// Inbound request for [`DeclarativeTcpCodec`].
#[derive(Debug, Clone)]
pub struct DeclarativeTcpRequest {
    /// Logical RPC/method name (routed by the declarative stub).
    pub method: String,
    /// Payload bytes after the method name.
    pub body: Bytes,
}

impl TcpCodec for DeclarativeTcpCodec {
    type Request = DeclarativeTcpRequest;
    type Response = Bytes;

    fn decode(&self, buffer: &mut BytesMut) -> Result<Option<Self::Request>, MockServiceError> {
        if buffer.len() < 4 {
            return Ok(None);
        }
        let payload_len = u32::from_be_bytes(buffer[..4].try_into().map_err(|_| {
            MockServiceError::TcpCodecDecode {
                message: String::from("internal: four-byte length prefix"),
            }
        })?) as usize;
        if buffer.len() < 4 + payload_len {
            return Ok(None);
        }
        let mut frame = buffer.split_to(4 + payload_len);
        frame.advance(4);
        if frame.len() < 2 {
            return Err(MockServiceError::TcpCodecDecode {
                message: String::from("declarative TCP frame too short for method length"),
            });
        }
        let method_len = u16::from_be_bytes(frame[..2].try_into().map_err(|_| {
            MockServiceError::TcpCodecDecode {
                message: String::from("internal: two-byte method length"),
            }
        })?) as usize;
        frame.advance(2);
        if frame.len() < method_len {
            return Err(MockServiceError::TcpCodecDecode {
                message: String::from("declarative TCP frame truncated in method name"),
            });
        }
        let method_bytes = frame.split_to(method_len);
        let method = std::str::from_utf8(&method_bytes)
            .map_err(|_| MockServiceError::TcpCodecDecode {
                message: String::from("declarative TCP method name is not valid UTF-8"),
            })?
            .to_owned();
        let body = frame.copy_to_bytes(frame.remaining());
        Ok(Some(DeclarativeTcpRequest { method, body }))
    }

    fn encode(&self, response: Bytes, buffer: &mut BytesMut) -> Result<(), MockServiceError> {
        let len_u32 = u32::try_from(response.len()).map_err(|_| MockServiceError::Encoding {
            message: String::from("declarative TCP response length does not fit in u32"),
        })?;
        buffer.put_u32(len_u32);
        buffer.put_slice(response.as_ref());
        Ok(())
    }

    fn request_method_label(request: &Self::Request) -> String {
        request.method.clone()
    }

    fn request_bytes_for_log(request: &Self::Request) -> Bytes {
        let mut v = Vec::new();
        v.extend_from_slice(request.method.as_bytes());
        v.push(b':');
        v.extend_from_slice(&request.body);
        Bytes::from(v)
    }

    fn response_bytes_for_log(response: &Self::Response) -> Bytes {
        response.clone()
    }
}
