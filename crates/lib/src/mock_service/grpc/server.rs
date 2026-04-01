//! Tonic-hosted gRPC mock service server (`grpc-mock-services`): [`tower::Service`] over unary payloads.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::{BufMut, Bytes, BytesMut};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::{Request, Response};
use http_body::Frame;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio_stream::iter as stream_iter;
use tonic::body::Body;
use tonic::metadata::GRPC_CONTENT_TYPE;
use tonic::transport::server::TcpIncoming;
use tonic::Code;
use tonic::Status;
use tower::Service;

use super::super::handle::{MockServiceHandle, RecordedExchange};
use super::super::types::{MockServiceContext, MockServiceError, MockServiceResources};
use super::protocol_error::GrpcMockServiceProtocolError;
use super::types::{GrpcMockServiceRequest, GrpcMockServiceResponse};
use super::GrpcMockServiceTrait;

/// Join handle and shutdown signal for one gRPC mock service server task.
pub(crate) struct GrpcMockServiceServer {
    pub shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    pub join: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

/// Bind `127.0.0.1:0`, spawn [`tonic::transport::Server`], return [`MockServiceHandle`] and
/// [`GrpcMockServiceServer`] (shutdown signal + background task join handle).
pub(crate) async fn spawn_grpc_mock_service_server(
    mock_service_name: String,
    mock_service: Arc<dyn GrpcMockServiceTrait>,
    scenario_id: String,
    resources: Arc<MockServiceResources>,
    max_recorded_body_size: Option<usize>,
) -> Result<(Arc<MockServiceHandle>, GrpcMockServiceServer), MockServiceError> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|source| MockServiceError::ServerStart { source })?;
    let bind_addr = listener
        .local_addr()
        .map_err(|source| MockServiceError::ServerStart { source })?;
    let bind_display = bind_addr.to_string();
    let client_url = format!("http://{bind_display}");
    let incoming = TcpIncoming::from(listener);

    let handle = Arc::new(MockServiceHandle::new(
        mock_service_name.clone(),
        bind_display,
        client_url,
        max_recorded_body_size,
    ));

    let service = GrpcMockServiceTowerService {
        mock_service,
        mock_service_label: mock_service_name.clone(),
        scenario_id,
        resources,
        recorder: Arc::clone(&handle),
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let shutdown_signal = async move {
            let _ = shutdown_rx.await;
        };
        tonic::transport::Server::builder()
            .serve_with_incoming_shutdown(service, incoming, shutdown_signal)
            .await
    });

    Ok((
        handle,
        GrpcMockServiceServer {
            shutdown_tx: Some(shutdown_tx),
            join,
        },
    ))
}

/// Per-request [`tower::Service`] that decodes unary gRPC frames and delegates to [`GrpcMockServiceTrait`].
#[derive(Clone)]
pub(crate) struct GrpcMockServiceTowerService {
    mock_service: Arc<dyn GrpcMockServiceTrait>,
    mock_service_label: String,
    scenario_id: String,
    resources: Arc<MockServiceResources>,
    recorder: Arc<MockServiceHandle>,
}

impl GrpcMockServiceTowerService {
    async fn handle_request_inner(
        &self,
        request: Request<Body>,
    ) -> Result<Response<Body>, GrpcMockServiceProtocolError> {
        if request.method() != http::Method::POST {
            return Err(GrpcMockServiceProtocolError::RequestMethodNotPost);
        }

        let timestamp = OffsetDateTime::now_utc();
        let (parts, body) = request.into_parts();
        let grpc_path = parts.uri.path().to_owned();
        let request_headers = header_map_to_pairs(&parts.headers);

        let body_bytes = body
            .collect()
            .await
            .map_err(|_| GrpcMockServiceProtocolError::RequestBodyReadFailed)?
            .to_bytes();

        let (service_name, method_name) = parse_grpc_path(grpc_path.as_str())
            .ok_or(GrpcMockServiceProtocolError::InvalidGrpcPath)?;

        let payload = decode_unary_grpc_payload(body_bytes.as_ref())?;

        let grpc_request = GrpcMockServiceRequest {
            service: service_name,
            method: method_name,
            metadata: request_headers.clone(),
            body: Bytes::copy_from_slice(payload),
        };

        let mock_service_context = MockServiceContext::new(
            self.scenario_id.clone(),
            self.mock_service_label.clone(),
            Arc::clone(&self.resources),
        );

        let started = Instant::now();
        let mock_service_response = self
            .mock_service
            .handle(grpc_request, &mock_service_context)
            .await;
        let duration = started.elapsed();

        let http_response = grpc_mock_service_response_to_http(&mock_service_response)?;

        let response_status = 200u16;
        let response_body = if mock_service_response.status_code == Code::Ok {
            encode_grpc_frame(mock_service_response.body.as_ref())
        } else {
            Bytes::new()
        };

        self.recorder.push_recorded_exchange(RecordedExchange {
            timestamp,
            method: grpc_path.clone(),
            path: grpc_path,
            query: None,
            request_headers,
            request_body: Bytes::copy_from_slice(payload),
            response_status,
            response_body,
            duration,
        });

        Ok(http_response)
    }
}

impl Service<Request<Body>> for GrpcMockServiceTowerService {
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            let outcome = service.handle_request_inner(request).await;
            Ok(match outcome {
                Ok(response) => response,
                Err(error) => {
                    let status = Status::new(error.status_code(), error.to_string());
                    status_to_http_response(status)
                }
            })
        })
    }
}

fn parse_grpc_path(path: &str) -> Option<(String, String)> {
    let trimmed = path.strip_prefix('/')?;
    let (service, method) = trimmed.rsplit_once('/')?;
    if service.is_empty() || method.is_empty() {
        return None;
    }
    Some((service.to_owned(), method.to_owned()))
}

fn header_map_to_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str();
        if name_str.starts_with(':') {
            continue;
        }
        if let Ok(value_str) = value.to_str() {
            pairs.push((name_str.to_ascii_lowercase(), value_str.to_owned()));
        }
    }
    pairs
}

fn decode_unary_grpc_payload(input: &[u8]) -> Result<&[u8], GrpcMockServiceProtocolError> {
    if input.len() < 5 {
        return Err(GrpcMockServiceProtocolError::UnaryFrameTooShort);
    }
    let compressed = input[0];
    if compressed != 0 {
        return Err(GrpcMockServiceProtocolError::CompressedUnaryPayloadNotSupported);
    }
    let length_prefix: [u8; 4] = input[1..5]
        .try_into()
        .map_err(|_| GrpcMockServiceProtocolError::UnaryLengthPrefixLayoutInvalid)?;
    let message_length = u32::from_be_bytes(length_prefix) as usize;
    if input.len() < 5 + message_length {
        return Err(GrpcMockServiceProtocolError::UnaryPayloadLengthExceedsFrame);
    }
    Ok(&input[5..5 + message_length])
}

fn encode_grpc_frame(message: &[u8]) -> Bytes {
    debug_assert!(
        message.len() <= u32::MAX as usize,
        "gRPC frame payload exceeds u32::MAX"
    );
    let mut buffer = BytesMut::with_capacity(5 + message.len());
    buffer.put_u8(0);
    buffer.put_u32(message.len() as u32);
    buffer.put_slice(message);
    buffer.freeze()
}

fn status_from_mock_service_response(response: &GrpcMockServiceResponse) -> Status {
    let message = response.message.clone().unwrap_or_default();
    Status::new(response.status_code, message)
}

fn grpc_mock_service_response_to_http(
    response: &GrpcMockServiceResponse,
) -> Result<Response<Body>, GrpcMockServiceProtocolError> {
    if response.status_code == Code::Ok {
        let framed = encode_grpc_frame(response.body.as_ref());
        let mut trailers = HeaderMap::new();
        trailers.insert(
            HeaderName::from_static("grpc-status"),
            HeaderValue::from_static("0"),
        );
        merge_trailing_metadata(response, &mut trailers)?;
        let stream = stream_iter(vec![
            Ok::<_, Infallible>(Frame::data(framed)),
            Ok(Frame::trailers(trailers)),
        ]);
        Response::builder()
            .status(http::StatusCode::OK)
            .header(http::header::CONTENT_TYPE, GRPC_CONTENT_TYPE)
            .body(Body::new(StreamBody::new(stream)))
            .map_err(|_| GrpcMockServiceProtocolError::GrpcOkHttpResponseBuildFailed)
    } else {
        Ok(status_to_http_response(status_from_mock_service_response(
            response,
        )))
    }
}

fn merge_trailing_metadata(
    response: &GrpcMockServiceResponse,
    trailers: &mut HeaderMap,
) -> Result<(), GrpcMockServiceProtocolError> {
    for (key, value) in &response.metadata {
        let name = HeaderName::try_from(key.as_str())
            .map_err(|_| GrpcMockServiceProtocolError::TrailingMetadataHeaderNameInvalid)?;
        let header_value = HeaderValue::try_from(value.as_str())
            .map_err(|_| GrpcMockServiceProtocolError::TrailingMetadataHeaderValueInvalid)?;
        trailers.insert(name, header_value);
    }
    Ok(())
}

fn status_to_http_response(status: Status) -> Response<Body> {
    let mut trailers = HeaderMap::new();
    if status.add_header(&mut trailers).is_err() {
        trailers.clear();
        let fallback = GrpcMockServiceProtocolError::GrpcStatusTrailerEncodingFailed;
        let _ = Status::new(fallback.status_code(), fallback.to_string()).add_header(&mut trailers);
    }
    let stream = stream_iter(vec![Ok::<_, Infallible>(Frame::trailers(trailers))]);
    match Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, GRPC_CONTENT_TYPE)
        .body(Body::new(StreamBody::new(stream)))
    {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(
                target: "shakedown::mock_service",
                "failed to build gRPC error HTTP response: {}",
                error
            );
            let mut response = Response::new(Body::empty());
            *response.status_mut() = http::StatusCode::INTERNAL_SERVER_ERROR;
            response
        }
    }
}

#[cfg(all(test, feature = "grpc-mock-services"))]
mod tests {
    use http::header::{HeaderMap, HeaderName, HeaderValue};

    use super::super::protocol_error::GrpcMockServiceProtocolError;
    use super::*;

    #[test]
    fn parse_grpc_path_valid() {
        // Given a standard gRPC path `/Service/Method`
        // When we parse it
        let parsed = parse_grpc_path("/echo.EchoService/SayHello");
        // Then service and method split on the last `/`
        assert_eq!(
            parsed,
            Some((String::from("echo.EchoService"), String::from("SayHello")))
        );
    }

    #[test]
    fn parse_grpc_path_missing_leading_slash() {
        // Given a path without a leading slash
        // When we parse
        // Then parsing fails
        assert_eq!(parse_grpc_path("echo.Service/Method"), None);
    }

    #[test]
    fn parse_grpc_path_empty_method_segment() {
        // Given a path ending with `/` so the method segment is empty
        // When we parse
        // Then parsing fails
        assert_eq!(parse_grpc_path("/service/"), None);
    }

    #[test]
    fn parse_grpc_path_no_method_separator() {
        // Given a single path segment after the leading slash
        // When we parse
        // Then there is no `service/method` pair
        assert_eq!(parse_grpc_path("/NoInnerSlash"), None);
    }

    #[test]
    fn decode_unary_grpc_payload_too_short() {
        // Given fewer than 5 bytes (no full length prefix)
        // When we decode
        // Then we get UnaryFrameTooShort
        assert!(matches!(
            decode_unary_grpc_payload(&[0, 1, 2]),
            Err(GrpcMockServiceProtocolError::UnaryFrameTooShort)
        ));
    }

    #[test]
    fn decode_unary_grpc_payload_compressed_not_supported() {
        // Given the compression flag byte set to non-zero
        let frame = [1u8, 0, 0, 0, 0];
        // When we decode
        // Then compressed payloads are rejected
        assert!(matches!(
            decode_unary_grpc_payload(&frame),
            Err(GrpcMockServiceProtocolError::CompressedUnaryPayloadNotSupported)
        ));
    }

    #[test]
    fn decode_unary_grpc_payload_valid() {
        // Given a valid 5-byte prefix and one message byte
        let frame = [0u8, 0, 0, 0, 1, 0x42];
        // When we decode
        // Then the payload slice is that byte
        assert_eq!(decode_unary_grpc_payload(&frame).unwrap(), &[0x42u8][..]);
    }

    #[test]
    fn decode_unary_grpc_payload_truncated_message() {
        // Given a length prefix claiming more bytes than follow
        let frame = [0u8, 0, 0, 0, 10, 1, 2, 3];
        // When we decode
        // Then we get UnaryPayloadLengthExceedsFrame
        assert!(matches!(
            decode_unary_grpc_payload(&frame),
            Err(GrpcMockServiceProtocolError::UnaryPayloadLengthExceedsFrame)
        ));
    }

    #[test]
    fn encode_grpc_frame_round_trips_decode() {
        // Given a protobuf payload
        let payload = b"hello";
        // When we encode then decode
        let framed = encode_grpc_frame(payload);
        let round_trip = decode_unary_grpc_payload(framed.as_ref()).unwrap();
        // Then the bytes match
        assert_eq!(round_trip, payload);
    }

    #[test]
    fn header_map_to_pairs_lowercases_header_names() {
        // Given headers with mixed-case names
        let mut map = HeaderMap::new();
        map.insert("X-Custom", HeaderValue::from_static("Value"));
        map.insert("ACCEPT", HeaderValue::from_static("application/json"));
        // When we convert to pairs
        let pairs = header_map_to_pairs(&map);
        // Then names are ASCII-lowercased
        assert!(pairs.iter().any(|(k, v)| k == "x-custom" && v == "Value"));
        assert!(pairs
            .iter()
            .any(|(k, v)| k == "accept" && v == "application/json"));
    }

    #[test]
    fn header_map_to_pairs_skips_non_utf8_header_values() {
        // Given one UTF-8 header and one opaque value that is not `to_str()`-able
        let mut map = HeaderMap::new();
        map.insert(
            HeaderName::from_static("x-ascii"),
            HeaderValue::from_static("ok"),
        );
        let opaque = HeaderValue::from_bytes(&[0xff]).expect("opaque header value");
        map.insert(HeaderName::from_static("x-bin"), opaque);
        // When we convert to pairs
        let pairs = header_map_to_pairs(&map);
        // Then only ASCII-decodable values appear
        assert!(pairs.iter().any(|(k, v)| k == "x-ascii" && v == "ok"));
        assert!(!pairs.iter().any(|(k, _)| k == "x-bin"));
    }
}
