//! Tokio UDP socket for [`super::traits::UdpMockServiceTrait`].

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use time::OffsetDateTime;
use tokio::net::UdpSocket;
use tokio::sync::oneshot;

use crate::mock_service::handle::{MockServiceHandle, RecordedExchange};
use crate::mock_service::types::{MockServiceContext, MockServiceError, MockServiceResources};

use super::traits::UdpMockServiceTrait;

/// Shutdown signal and join handle for a UDP mock recv loop.
pub(crate) struct UdpMockServiceServer {
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    pub join: tokio::task::JoinHandle<()>,
}

/// Bind `bind_ip:0`, spawn recv loop, return [`MockServiceHandle`] and shutdown handle.
pub(crate) async fn spawn_udp_mock_service_server(
    mock_service_name: String,
    handler: Arc<dyn UdpMockServiceTrait>,
    scenario_id: String,
    resources: Arc<MockServiceResources>,
    max_recorded_body_size: Option<usize>,
    bind_ip: IpAddr,
) -> Result<(Arc<MockServiceHandle>, UdpMockServiceServer), MockServiceError> {
    let socket = UdpSocket::bind(SocketAddr::new(bind_ip, 0))
        .await
        .map_err(|source| MockServiceError::ServerStart { source })?;
    let bind_addr = socket
        .local_addr()
        .map_err(|source| MockServiceError::ServerStart { source })?;
    let bind_display = bind_addr.to_string();
    let client_url = format!("udp://{bind_display}");

    let handle = Arc::new(MockServiceHandle::new(
        mock_service_name.clone(),
        bind_display,
        client_url,
        max_recorded_body_size,
    ));

    let socket = Arc::new(socket);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle_for_task = Arc::clone(&handle);
    let join = tokio::spawn(run_udp_mock_loop(
        socket,
        shutdown_rx,
        handler,
        mock_service_name,
        scenario_id,
        resources,
        handle_for_task,
    ));

    Ok((
        handle,
        UdpMockServiceServer {
            shutdown_tx: Some(shutdown_tx),
            join,
        },
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_udp_mock_loop(
    socket: Arc<UdpSocket>,
    mut shutdown_rx: oneshot::Receiver<()>,
    handler: Arc<dyn UdpMockServiceTrait>,
    mock_service_name: String,
    scenario_id: String,
    resources: Arc<MockServiceResources>,
    recorder: Arc<MockServiceHandle>,
) {
    let mut buffer = vec![0u8; 65535];
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                break;
            }
            recv_result = socket.recv_from(&mut buffer) => {
                let (len, source) = match recv_result {
                    Ok(pair) => pair,
                    Err(error) => {
                        tracing::warn!(
                            target: "shakedown::mock_service",
                            "UDP mock service '{}' recv_from error: {}",
                            mock_service_name,
                            error
                        );
                        continue;
                    }
                };
                let datagram = &buffer[..len];
                let mock_service_context = MockServiceContext::new(
                    scenario_id.clone(),
                    mock_service_name.clone(),
                    Arc::clone(&resources),
                );
                let timestamp = OffsetDateTime::now_utc();
                let started = Instant::now();
                let handle_result = handler
                    .handle(datagram, source, &mock_service_context)
                    .await;
                let duration = started.elapsed();
                let request_body = Bytes::copy_from_slice(datagram);
                match handle_result {
                    Ok(Some(reply)) => {
                        let response_body_log = Bytes::copy_from_slice(&reply);
                        match socket.send_to(&reply, source).await {
                            Ok(_) => {
                                recorder.push_recorded_exchange(RecordedExchange {
                                    timestamp,
                                    method: String::from("datagram"),
                                    path: String::new(),
                                    query: Some(source.to_string()),
                                    request_headers: Vec::new(),
                                    request_body,
                                    response_status: 200,
                                    response_body: response_body_log,
                                    duration,
                                });
                            }
                            Err(source_err) => {
                                recorder.push_recorded_exchange(RecordedExchange {
                                    timestamp,
                                    method: String::from("datagram"),
                                    path: String::new(),
                                    query: Some(source.to_string()),
                                    request_headers: Vec::new(),
                                    request_body,
                                    response_status: 500,
                                    response_body: Bytes::copy_from_slice(
                                        source_err.to_string().as_bytes(),
                                    ),
                                    duration,
                                });
                            }
                        }
                    }
                    Ok(None) => {
                        recorder.push_recorded_exchange(RecordedExchange {
                            timestamp,
                            method: String::from("datagram"),
                            path: String::new(),
                            query: Some(source.to_string()),
                            request_headers: Vec::new(),
                            request_body,
                            response_status: 204,
                            response_body: Bytes::new(),
                            duration,
                        });
                    }
                    Err(error) => {
                        recorder.push_recorded_exchange(RecordedExchange {
                            timestamp,
                            method: String::from("datagram"),
                            path: String::new(),
                            query: Some(source.to_string()),
                            request_headers: Vec::new(),
                            request_body,
                            response_status: 500,
                            response_body: Bytes::copy_from_slice(error.to_string().as_bytes()),
                            duration,
                        });
                    }
                }
            }
        }
    }
}
