//! Tonic-hosted gRPC bridge server (`grpc-bridges`): [`tower::Service`] over unary payloads.

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

use super::grpc_protocol_error::GrpcBridgeProtocolError;
use super::grpc_types::{GrpcBridgeRequest, GrpcBridgeResponse};
use super::handle::{BridgeHandle, RecordedExchange};
use super::traits::GrpcBridge;
use super::types::{BridgeContext, BridgeError, BridgeResources};

/// Join handle and shutdown signal for one gRPC bridge server task.
pub(crate) struct GrpcBridgeRuntime {
    pub shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    pub join: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

/// Bind `127.0.0.1:0`, spawn [`tonic::transport::Server`], return [`BridgeHandle`] and runtime.
pub(crate) async fn spawn_grpc_bridge_server(
    bridge_name: String,
    bridge: Arc<dyn GrpcBridge>,
    scenario_id: String,
    resources: Arc<BridgeResources>,
    max_recorded_body_size: Option<usize>,
) -> Result<(Arc<BridgeHandle>, GrpcBridgeRuntime), BridgeError> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|source| BridgeError::ServerStart { source })?;
    let bind_addr = listener
        .local_addr()
        .map_err(|source| BridgeError::ServerStart { source })?;
    let bind_display = bind_addr.to_string();
    let client_url = format!("http://{bind_display}");
    let incoming = TcpIncoming::from(listener);

    let handle = Arc::new(BridgeHandle::new(
        bridge_name.clone(),
        bind_display,
        client_url,
        max_recorded_body_size,
    ));

    let service = GrpcBridgeTowerService {
        bridge,
        bridge_label: bridge_name.clone(),
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
        GrpcBridgeRuntime {
            shutdown_tx: Some(shutdown_tx),
            join,
        },
    ))
}

/// Per-request [`tower::Service`] that decodes unary gRPC frames and delegates to [`GrpcBridge`].
#[derive(Clone)]
pub(crate) struct GrpcBridgeTowerService {
    bridge: Arc<dyn GrpcBridge>,
    bridge_label: String,
    scenario_id: String,
    resources: Arc<BridgeResources>,
    recorder: Arc<BridgeHandle>,
}

impl GrpcBridgeTowerService {
    async fn handle_request_inner(
        &self,
        request: Request<Body>,
    ) -> Result<Response<Body>, GrpcBridgeProtocolError> {
        if request.method() != http::Method::POST {
            return Err(GrpcBridgeProtocolError::RequestMethodNotPost);
        }

        let timestamp = OffsetDateTime::now_utc();
        let (parts, body) = request.into_parts();
        let path_owned = parts.uri.path().to_owned();
        let full_grpc_path = path_owned.clone();
        let request_headers = header_map_to_pairs(&parts.headers);

        let body_bytes = body
            .collect()
            .await
            .map_err(|_| GrpcBridgeProtocolError::RequestBodyReadFailed)?
            .to_bytes();

        let (service_name, method_name) =
            parse_grpc_path(path_owned.as_str()).ok_or(GrpcBridgeProtocolError::InvalidGrpcPath)?;

        let payload = decode_unary_grpc_payload(body_bytes.as_ref())?;

        let grpc_request = GrpcBridgeRequest {
            service: service_name,
            method: method_name,
            metadata: request_headers.clone(),
            body: Bytes::copy_from_slice(payload),
        };

        let bridge_context = BridgeContext::new(
            self.scenario_id.clone(),
            self.bridge_label.clone(),
            Arc::clone(&self.resources),
        );

        let started = Instant::now();
        let bridge_response = self.bridge.handle(grpc_request, &bridge_context).await;
        let duration = started.elapsed();

        let http_response = grpc_bridge_response_to_http(&bridge_response)?;

        let response_status = 200u16;
        let response_body = if bridge_response.status_code == Code::Ok as u32 {
            encode_grpc_frame(bridge_response.body.as_ref())
        } else {
            Bytes::new()
        };

        self.recorder.push_recorded_exchange(RecordedExchange {
            timestamp,
            method: full_grpc_path.clone(),
            path: full_grpc_path,
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

impl Service<Request<Body>> for GrpcBridgeTowerService {
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
                Err(error) => status_to_http_response(error.into()),
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

fn decode_unary_grpc_payload(input: &[u8]) -> Result<&[u8], GrpcBridgeProtocolError> {
    if input.len() < 5 {
        return Err(GrpcBridgeProtocolError::UnaryFrameTooShort);
    }
    let compressed = input[0];
    if compressed != 0 {
        return Err(GrpcBridgeProtocolError::CompressedUnaryPayloadNotSupported);
    }
    let length_prefix: [u8; 4] = input[1..5]
        .try_into()
        .map_err(|_| GrpcBridgeProtocolError::UnaryLengthPrefixLayoutInvalid)?;
    let message_length = u32::from_be_bytes(length_prefix) as usize;
    if input.len() < 5 + message_length {
        return Err(GrpcBridgeProtocolError::UnaryPayloadLengthExceedsFrame);
    }
    Ok(&input[5..5 + message_length])
}

fn encode_grpc_frame(message: &[u8]) -> Bytes {
    let mut buffer = BytesMut::with_capacity(5 + message.len());
    buffer.put_u8(0);
    buffer.put_u32(message.len() as u32);
    buffer.put_slice(message);
    buffer.freeze()
}

fn status_from_bridge_response(response: &GrpcBridgeResponse) -> Status {
    let code = Code::from_i32(response.status_code as i32);
    let message = response.message.clone().unwrap_or_default();
    Status::new(code, message)
}

fn grpc_bridge_response_to_http(
    response: &GrpcBridgeResponse,
) -> Result<Response<Body>, GrpcBridgeProtocolError> {
    if response.status_code == Code::Ok as u32 {
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
            .map_err(|_| GrpcBridgeProtocolError::GrpcOkHttpResponseBuildFailed)
    } else {
        Ok(status_to_http_response(status_from_bridge_response(
            response,
        )))
    }
}

fn merge_trailing_metadata(
    response: &GrpcBridgeResponse,
    trailers: &mut HeaderMap,
) -> Result<(), GrpcBridgeProtocolError> {
    for (key, value) in &response.metadata {
        let name = HeaderName::try_from(key.as_str())
            .map_err(|_| GrpcBridgeProtocolError::TrailingMetadataHeaderNameInvalid)?;
        let header_value = HeaderValue::try_from(value.as_str())
            .map_err(|_| GrpcBridgeProtocolError::TrailingMetadataHeaderValueInvalid)?;
        trailers.insert(name, header_value);
    }
    Ok(())
}

fn status_to_http_response(status: Status) -> Response<Body> {
    let mut trailers = HeaderMap::new();
    if status.add_header(&mut trailers).is_err() {
        trailers.clear();
        let _ = Status::from(GrpcBridgeProtocolError::GrpcStatusTrailerEncodingFailed)
            .add_header(&mut trailers);
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
                target: "shakedown::bridge",
                "failed to build gRPC error HTTP response: {}",
                error
            );
            let mut response = Response::new(Body::empty());
            *response.status_mut() = http::StatusCode::INTERNAL_SERVER_ERROR;
            response
        }
    }
}
