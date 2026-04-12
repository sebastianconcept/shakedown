//! Hyper HTTP/1 listener for [`super::HttpMockServiceTrait`].

use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use super::super::handle::{MockServiceHandle, RecordedExchange};
use super::super::types::{MockServiceContext, MockServiceError, MockServiceResources};
use http::StatusCode;

use super::request::HttpMockServiceRequest;
use super::response::{internal_server_error_http_response, HttpMockServiceResponse};
use super::HttpMockServiceTrait;

/// Join handle and shutdown signal for one HTTP mock service accept loop.
pub(crate) struct HttpMockServiceServer {
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    pub join: tokio::task::JoinHandle<()>,
}

/// Bind `bind_ip:0` (ephemeral port), spawn the accept loop, return the public handle (in an [`Arc`]) and
/// [`HttpMockServiceServer`] (shutdown signal + background task join handle).
pub(crate) async fn spawn_http_mock_service_server(
    mock_service_name: String,
    mock_service: Arc<dyn HttpMockServiceTrait>,
    scenario_id: String,
    resources: Arc<MockServiceResources>,
    max_recorded_body_size: Option<usize>,
    bind_ip: IpAddr,
) -> Result<(Arc<MockServiceHandle>, HttpMockServiceServer), MockServiceError> {
    let listener = TcpListener::bind(SocketAddr::new(bind_ip, 0))
        .await
        .map_err(|source| MockServiceError::ServerStart { source })?;
    let bind_addr = listener
        .local_addr()
        .map_err(|source| MockServiceError::ServerStart { source })?;
    let bind_display = bind_addr.to_string();
    let client_url = format!("http://{bind_display}");

    let handle = Arc::new(MockServiceHandle::new(
        mock_service_name.clone(),
        bind_display,
        client_url,
        max_recorded_body_size,
    ));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle_for_task = Arc::clone(&handle);
    let join = tokio::spawn(run_listener(
        listener,
        shutdown_rx,
        mock_service,
        mock_service_name,
        scenario_id,
        resources,
        handle_for_task,
    ));

    Ok((
        handle,
        HttpMockServiceServer {
            shutdown_tx: Some(shutdown_tx),
            join,
        },
    ))
}

async fn run_listener(
    listener: TcpListener,
    mut shutdown_rx: oneshot::Receiver<()>,
    mock_service: Arc<dyn HttpMockServiceTrait>,
    mock_service_name: String,
    scenario_id: String,
    resources: Arc<MockServiceResources>,
    handle: Arc<MockServiceHandle>,
) {
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                break;
            }
            accept_result = listener.accept() => {
                let (tcp, _) = match accept_result {
                    Ok(pair) => pair,
                    Err(error) => {
                        tracing::warn!(target: "shakedown::mock_service", "mock service '{}' accept error: {}", mock_service_name, error);
                        continue;
                    }
                };
                let io = TokioIo::new(tcp);
                let mock_service_for_connection = Arc::clone(&mock_service);
                let scenario_id_for_connection = scenario_id.clone();
                let mock_service_name_for_connection = mock_service_name.clone();
                let resources_for_connection = Arc::clone(&resources);
                let handle_for_connection = Arc::clone(&handle);
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let mock_service = Arc::clone(&mock_service_for_connection);
                        let mock_service_context = MockServiceContext::new(
                            scenario_id_for_connection.clone(),
                            mock_service_name_for_connection.clone(),
                            Arc::clone(&resources_for_connection),
                        );
                        let recorder = Arc::clone(&handle_for_connection);
                        async move { serve_one(mock_service, mock_service_context, recorder, request).await }
                    });
                    let connection = http1::Builder::new().serve_connection(io, service);
                    if let Err(error) = connection.await {
                        tracing::debug!(target: "shakedown::mock_service", "mock service connection ended: {}", error);
                    }
                });
            }
        }
    }
}

async fn serve_one(
    mock_service: Arc<dyn HttpMockServiceTrait>,
    mock_service_context: MockServiceContext,
    recorder: Arc<MockServiceHandle>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (parts, body) = request.into_parts();
    let body_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            return Ok(
                match HttpMockServiceResponse::status(StatusCode::BAD_REQUEST).into_http_response()
                {
                    Ok(response) => response,
                    Err(error) => {
                        tracing::error!(
                            target: "shakedown::mock_service",
                            "mock service '{}' failed to build 400 response: {}",
                            mock_service_context.mock_service_name,
                            error
                        );
                        internal_server_error_http_response()
                    }
                },
            );
        }
    };
    let mock_service_request = HttpMockServiceRequest::from_http_parts(parts, body_bytes);
    let timestamp = OffsetDateTime::now_utc();
    let method = mock_service_request.method().as_str().to_owned();
    let path = mock_service_request.path().to_owned();
    let query = mock_service_request.query().map(str::to_owned);
    let request_headers = mock_service_request.headers().to_vec();
    let request_body = mock_service_request.bytes().clone();

    let started = Instant::now();
    let mock_service_response = mock_service
        .handle(mock_service_request, &mock_service_context)
        .await;
    let duration = started.elapsed();

    let response_status = mock_service_response.status.as_u16();
    let response_body = mock_service_response.body.clone();
    let http_response = match mock_service_response.into_http_response() {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(
                target: "shakedown::mock_service",
                "mock service '{}' invalid HTTP response from stub: {}",
                mock_service_context.mock_service_name,
                error
            );
            internal_server_error_http_response()
        }
    };

    recorder.push_recorded_exchange(RecordedExchange {
        timestamp,
        method,
        path,
        query,
        request_headers,
        request_body,
        response_status,
        response_body,
        duration,
    });

    Ok(http_response)
}
