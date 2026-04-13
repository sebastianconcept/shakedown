//! Tokio TCP listener for [`super::traits::TcpMockServiceTrait`].

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

use crate::mock_service::handle::{MockServiceHandle, RecordedExchange};
use crate::mock_service::types::{MockServiceContext, MockServiceError, MockServiceResources};

use super::codec::TcpCodec;
use super::traits::TcpMockServiceTrait;

/// Shutdown signal and join handle for a TCP mock accept loop.
pub(crate) struct TcpMockServiceServer {
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    pub join: tokio::task::JoinHandle<()>,
}

/// Registration returned by [`super::service::TcpMockService::build`] and
/// [`super::service::TcpMockCustom::build`].
pub struct TcpMockServiceRegistration {
    pub(crate) connection: Arc<dyn TcpMockServiceConnection>,
    pub(crate) tls: Option<Arc<TlsAcceptor>>,
}

#[async_trait]
pub(crate) trait TcpMockServiceConnection: Send + Sync {
    async fn serve_connection(
        &self,
        tcp_stream: TcpStream,
        tls_acceptor: Option<Arc<TlsAcceptor>>,
        mock_service_context: MockServiceContext,
        recorder: Arc<MockServiceHandle>,
    ) -> Result<(), MockServiceError>;
}

pub(crate) struct TcpMockServiceSession<C, H> {
    pub(crate) codec: Arc<C>,
    pub(crate) handler: Arc<H>,
}

#[async_trait]
impl<C, H> TcpMockServiceConnection for TcpMockServiceSession<C, H>
where
    C: TcpCodec + Send + Sync,
    H: TcpMockServiceTrait<Codec = C> + Send + Sync,
{
    async fn serve_connection(
        &self,
        tcp_stream: TcpStream,
        tls_acceptor: Option<Arc<TlsAcceptor>>,
        mock_service_context: MockServiceContext,
        recorder: Arc<MockServiceHandle>,
    ) -> Result<(), MockServiceError> {
        match tls_acceptor {
            Some(acceptor) => {
                let tls_stream = acceptor
                    .accept(tcp_stream)
                    .await
                    .map_err(|source| MockServiceError::TlsHandshake { source })?;
                self.serve_io(tls_stream, mock_service_context, recorder)
                    .await
            }
            None => {
                self.serve_io(tcp_stream, mock_service_context, recorder)
                    .await
            }
        }
    }
}

impl<C, H> TcpMockServiceSession<C, H>
where
    C: TcpCodec + Send + Sync,
    H: TcpMockServiceTrait<Codec = C> + Send + Sync,
{
    async fn serve_io<S>(
        &self,
        mut stream: S,
        ctx: MockServiceContext,
        recorder: Arc<MockServiceHandle>,
    ) -> Result<(), MockServiceError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let codec = &self.codec;
        let handler = &self.handler;
        let mut read_buffer = BytesMut::with_capacity(8192);
        let mut scratch = [0u8; 4096];
        loop {
            let request = loop {
                match codec.decode(&mut read_buffer)? {
                    Some(request) => break request,
                    None => {
                        let read_count = stream
                            .read(&mut scratch)
                            .await
                            .map_err(|source| MockServiceError::ServerStart { source })?;
                        if read_count == 0 {
                            return Ok(());
                        }
                        read_buffer.extend_from_slice(&scratch[..read_count]);
                    }
                }
            };

            let method_label = C::request_method_label(&request);
            let request_body_log = C::request_bytes_for_log(&request);
            let timestamp = OffsetDateTime::now_utc();
            let started = Instant::now();

            let handle_result = handler.handle(request, &ctx).await;
            let duration = started.elapsed();

            match handle_result {
                Ok(response) => {
                    let response_body_log = C::response_bytes_for_log(&response);
                    let mut write_buffer = BytesMut::new();
                    codec.encode(response, &mut write_buffer)?;
                    stream
                        .write_all(&write_buffer)
                        .await
                        .map_err(|source| MockServiceError::ServerStart { source })?;
                    stream
                        .flush()
                        .await
                        .map_err(|source| MockServiceError::ServerStart { source })?;
                    recorder.push_recorded_exchange(RecordedExchange {
                        timestamp,
                        method: method_label,
                        path: String::new(),
                        query: None,
                        request_headers: Vec::new(),
                        request_body: request_body_log,
                        response_status: 200,
                        response_body: response_body_log,
                        duration,
                    });
                }
                Err(error) => {
                    recorder.push_recorded_exchange(RecordedExchange {
                        timestamp,
                        method: method_label,
                        path: String::new(),
                        query: None,
                        request_headers: Vec::new(),
                        request_body: request_body_log,
                        response_status: 500,
                        response_body: Bytes::copy_from_slice(error.to_string().as_bytes()),
                        duration,
                    });
                    return Err(error);
                }
            }
        }
    }
}

/// Bind `bind_ip:0`, spawn accept loop, return [`MockServiceHandle`] and shutdown handle.
pub(crate) async fn spawn_tcp_mock_service_server(
    mock_service_name: String,
    connection: Arc<dyn TcpMockServiceConnection>,
    tls_acceptor: Option<Arc<TlsAcceptor>>,
    scenario_id: String,
    resources: Arc<MockServiceResources>,
    max_recorded_body_size: Option<usize>,
    bind_ip: IpAddr,
) -> Result<(Arc<MockServiceHandle>, TcpMockServiceServer), MockServiceError> {
    let listener = TcpListener::bind(SocketAddr::new(bind_ip, 0))
        .await
        .map_err(|source| MockServiceError::ServerStart { source })?;
    let bind_addr = listener
        .local_addr()
        .map_err(|source| MockServiceError::ServerStart { source })?;
    let bind_display = bind_addr.to_string();
    let client_url = format!("tcp://{bind_display}");

    let handle = Arc::new(MockServiceHandle::new(
        mock_service_name.clone(),
        bind_display,
        client_url,
        max_recorded_body_size,
    ));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle_for_task = Arc::clone(&handle);
    let join = tokio::spawn(run_tcp_listener(
        listener,
        shutdown_rx,
        connection,
        tls_acceptor,
        mock_service_name,
        scenario_id,
        resources,
        handle_for_task,
    ));

    Ok((
        handle,
        TcpMockServiceServer {
            shutdown_tx: Some(shutdown_tx),
            join,
        },
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_tcp_listener(
    listener: TcpListener,
    mut shutdown_rx: oneshot::Receiver<()>,
    connection: Arc<dyn TcpMockServiceConnection>,
    tls_acceptor: Option<Arc<TlsAcceptor>>,
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
                let (tcp_stream, _) = match accept_result {
                    Ok(pair) => pair,
                    Err(error) => {
                        tracing::warn!(target: "shakedown::mock_service", "TCP mock service '{}' accept error: {}", mock_service_name, error);
                        continue;
                    }
                };
                let connection = Arc::clone(&connection);
                let tls = tls_acceptor.clone();
                let mock_service_name_for = mock_service_name.clone();
                let scenario_id_for = scenario_id.clone();
                let resources_for = Arc::clone(&resources);
                let recorder = Arc::clone(&handle);
                tokio::spawn(async move {
                    let mock_service_context = MockServiceContext::new(
                        scenario_id_for,
                        mock_service_name_for.clone(),
                        resources_for,
                    );
                    if let Err(error) = connection
                        .serve_connection(tcp_stream, tls, mock_service_context, recorder)
                        .await
                    {
                        tracing::debug!(
                            target: "shakedown::mock_service",
                            "TCP mock service '{}' connection ended: {}",
                            mock_service_name_for,
                            error
                        );
                    }
                });
            }
        }
    }
}
