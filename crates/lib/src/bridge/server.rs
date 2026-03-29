//! Hyper HTTP/1 listener for [`super::HttpBridge`].

use std::convert::Infallible;
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

use super::handle::{BridgeHandle, RecordedExchange};
use super::traits::HttpBridge;
use super::types::{
    internal_server_error_http_response, BridgeContext, BridgeError, BridgeRequest,
    BridgeResources, BridgeResponse,
};

/// Join handle and shutdown signal for one HTTP bridge accept loop.
pub(crate) struct HttpBridgeRuntime {
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    pub join: tokio::task::JoinHandle<()>,
}

/// Bind `127.0.0.1:0`, spawn the accept loop, return the public handle (in an [`Arc`]) and runtime.
pub(crate) async fn spawn_http_bridge_server(
    bridge_name: String,
    bridge: Arc<dyn HttpBridge>,
    scenario_id: String,
    resources: Arc<BridgeResources>,
    max_recorded_body_size: Option<usize>,
) -> Result<(Arc<BridgeHandle>, HttpBridgeRuntime), BridgeError> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|source| BridgeError::ServerStart { source })?;
    let bind_addr = listener
        .local_addr()
        .map_err(|source| BridgeError::ServerStart { source })?;
    let bind_display = bind_addr.to_string();
    let client_url = format!("http://{bind_display}");

    let handle = Arc::new(BridgeHandle::new(
        bridge_name.clone(),
        bind_display,
        client_url,
        max_recorded_body_size,
    ));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle_for_task = Arc::clone(&handle);
    let join = tokio::spawn(run_listener(
        listener,
        shutdown_rx,
        bridge,
        bridge_name,
        scenario_id,
        resources,
        handle_for_task,
    ));

    Ok((
        handle,
        HttpBridgeRuntime {
            shutdown_tx: Some(shutdown_tx),
            join,
        },
    ))
}

async fn run_listener(
    listener: TcpListener,
    mut shutdown_rx: oneshot::Receiver<()>,
    bridge: Arc<dyn HttpBridge>,
    bridge_name: String,
    scenario_id: String,
    resources: Arc<BridgeResources>,
    handle: Arc<BridgeHandle>,
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
                        tracing::warn!(target: "shakedown::bridge", "bridge '{}' accept error: {}", bridge_name, error);
                        continue;
                    }
                };
                let io = TokioIo::new(tcp);
                let bridge_c = Arc::clone(&bridge);
                let scenario_id_c = scenario_id.clone();
                let bridge_name_c = bridge_name.clone();
                let resources_c = Arc::clone(&resources);
                let handle_c = Arc::clone(&handle);
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let bridge = Arc::clone(&bridge_c);
                        let bridge_context = BridgeContext::new(
                            scenario_id_c.clone(),
                            bridge_name_c.clone(),
                            Arc::clone(&resources_c),
                        );
                        let recorder = Arc::clone(&handle_c);
                        async move { serve_one(bridge, bridge_context, recorder, request).await }
                    });
                    let connection = http1::Builder::new().serve_connection(io, service);
                    if let Err(error) = connection.await {
                        tracing::debug!(target: "shakedown::bridge", "bridge connection ended: {}", error);
                    }
                });
            }
        }
    }
}

async fn serve_one(
    bridge: Arc<dyn HttpBridge>,
    bridge_context: BridgeContext,
    recorder: Arc<BridgeHandle>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (parts, body) = request.into_parts();
    let body_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            return Ok(match BridgeResponse::status(400).into_http_response() {
                Ok(response) => response,
                Err(error) => {
                    tracing::error!(
                        target: "shakedown::bridge",
                        "bridge '{}' failed to build 400 response: {}",
                        bridge_context.bridge_name,
                        error
                    );
                    internal_server_error_http_response()
                }
            });
        }
    };
    let bridge_request = BridgeRequest::from_http_parts(parts, body_bytes);
    let timestamp = OffsetDateTime::now_utc();
    let method = bridge_request.method().to_owned();
    let path = bridge_request.path().to_owned();
    let query = bridge_request.query().map(str::to_owned);
    let request_headers = bridge_request.headers().to_vec();
    let request_body = bridge_request.bytes().clone();

    let started = Instant::now();
    let bridge_response = bridge.handle(bridge_request, &bridge_context).await;
    let duration = started.elapsed();

    let response_status = bridge_response.status;
    let response_body = bridge_response.body.clone();
    let http_response = match bridge_response.into_http_response() {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(
                target: "shakedown::bridge",
                "bridge '{}' invalid HTTP response from stub: {}",
                bridge_context.bridge_name,
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
