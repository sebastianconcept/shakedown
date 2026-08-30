//! Start, probe, and shut down mock-service servers for one scenario run.
//!
//! This module is useful when mock listeners must bind, pass readiness, or shut down with the
//! scenario.
//! This module binds HTTP (and optional gRPC, TCP, UDP) mock listeners after seed.
//! This module does not provision first-party containers.

use crate::error::{InfrastructureError, ShakedownError};
use crate::event::{Event, EventLog};
#[cfg(all(feature = "mock-services", feature = "grpc"))]
use crate::mock_service::grpc::server::{spawn_grpc_mock_service_server, GrpcMockServiceServer};
use crate::mock_service::handle::MockServiceHandle;
use crate::mock_service::http::server::{spawn_http_mock_service_server, HttpMockServiceServer};
#[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
use crate::mock_service::tcp::{spawn_tcp_mock_service_server, TcpMockServiceServer};
use crate::mock_service::types::MockServiceResources;
#[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
use crate::mock_service::udp::{spawn_udp_mock_service_server, UdpMockServiceServer};
use std::collections::HashMap;
use std::sync::Arc;

use super::Context;

#[cfg(feature = "mock-services")]
#[allow(unused_variables)] // `ctx` unused when no resource features are enabled alongside mock services
pub(crate) fn mock_service_resources_from_context(ctx: &Context) -> Arc<MockServiceResources> {
    #[allow(unused_mut)]
    // assignments are feature-gated; with mock services alone the struct stays default
    let mut resources = MockServiceResources::default();
    #[cfg(feature = "kafka")]
    {
        resources.kafka_bindings = ctx.kafka_bindings().cloned();
    }
    #[cfg(feature = "rabbitmq")]
    {
        resources.rabbit_bindings = ctx.rabbit_bindings().cloned();
    }
    #[cfg(feature = "postgres")]
    {
        resources.postgres_bindings = ctx.postgres_bindings().cloned();
    }
    #[cfg(feature = "timescaledb")]
    {
        resources.timescaledb_bindings = ctx.timescaledb_bindings().cloned();
    }
    #[cfg(feature = "mariadb")]
    {
        resources.mariadb_bindings = ctx.mariadb_bindings().cloned();
    }
    Arc::new(resources)
}

#[cfg(feature = "mock-services")]
pub(crate) async fn reset_mock_services_at_run_start(ctx: &mut Context) {
    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    {
        let servers = std::mem::take(&mut ctx.grpc_mock_service_servers);
        for (_name, mut server) in servers {
            if let Some(sender) = server.shutdown_tx.take() {
                let _ = sender.send(());
            }
            let _ = server.join.await;
        }
    }
    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    {
        shutdown_tcp_mock_service_servers_silent(ctx).await;
    }
    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    {
        shutdown_udp_mock_service_servers_silent(ctx).await;
    }
    let servers = std::mem::take(&mut ctx.http_mock_service_servers);
    for (_name, mut server) in servers {
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
    }
    ctx.mock_service_handles.clear();
    ctx.mock_service_servers_phase_complete = false;
}

#[cfg(feature = "mock-services")]
pub(crate) async fn tcp_probe_local_bind_address(bind_address: &str) -> std::io::Result<()> {
    let socket_address: std::net::SocketAddr = bind_address.parse().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid HTTP mock service bind address",
        )
    })?;
    let connect_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::net::TcpStream::connect(socket_address),
    )
    .await;
    match connect_result {
        Ok(Ok(stream)) => {
            drop(stream);
            Ok(())
        }
        Ok(Err(source)) => Err(source),
        Err(_elapsed) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "mock service tcp probe timed out",
        )),
    }
}

#[cfg(feature = "mock-services")]
pub(crate) fn udp_probe_local_bind_address(bind_address: &str) -> std::io::Result<()> {
    use std::net::SocketAddr;
    // Parse only: the listener is already bound in `spawn_udp_mock_service_server`. Sending a
    // probe datagram would be handled by the mock and recorded as a spurious call.
    let _: SocketAddr = bind_address.parse().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid UDP mock service bind address",
        )
    })?;
    Ok(())
}

#[cfg(feature = "mock-services")]
pub(crate) async fn mock_service_bind_address_ready(
    client_url: &str,
    bind_address: &str,
) -> std::io::Result<()> {
    if client_url.starts_with("udp://") {
        udp_probe_local_bind_address(bind_address)
    } else {
        tcp_probe_local_bind_address(bind_address).await
    }
}

#[cfg(feature = "mock-services")]
pub(crate) fn recorded_mock_service_call_count(
    handles: &HashMap<String, Arc<MockServiceHandle>>,
    mock_service_name: &str,
) -> usize {
    handles
        .get(mock_service_name)
        .map(|handle| handle.calls().len())
        .unwrap_or(0)
}

#[cfg(all(feature = "mock-services", feature = "grpc"))]
pub(crate) async fn shutdown_grpc_mock_service_servers_with_log(
    ctx: &mut Context,
    event_log: &mut EventLog,
) {
    let servers = std::mem::take(&mut ctx.grpc_mock_service_servers);
    for (name, mut server) in servers {
        let recorded_count =
            recorded_mock_service_call_count(&ctx.mock_service_handles, name.as_str());
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
        event_log.append(Event::info(
            "mock-service",
            format!("mock service '{name}' stopped; {recorded_count} calls recorded"),
        ));
    }
}

#[cfg(all(feature = "mock-services", feature = "grpc"))]
pub(crate) async fn shutdown_grpc_mock_service_servers_silent(ctx: &mut Context) {
    let servers = std::mem::take(&mut ctx.grpc_mock_service_servers);
    for (_name, mut server) in servers {
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
    }
}

#[cfg(feature = "mock-services")]
pub(crate) async fn shutdown_http_mock_service_servers_with_log(
    ctx: &mut Context,
    event_log: &mut EventLog,
) {
    let servers = std::mem::take(&mut ctx.http_mock_service_servers);
    for (name, mut server) in servers {
        let recorded_count =
            recorded_mock_service_call_count(&ctx.mock_service_handles, name.as_str());
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
        event_log.append(Event::info(
            "mock-service",
            format!("mock service '{name}' stopped; {recorded_count} calls recorded"),
        ));
    }
}

#[cfg(feature = "mock-services")]
pub(crate) async fn shutdown_http_mock_service_servers_silent(ctx: &mut Context) {
    let servers = std::mem::take(&mut ctx.http_mock_service_servers);
    for (_name, mut server) in servers {
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
    }
}

#[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
pub(crate) async fn shutdown_tcp_mock_service_servers_with_log(
    ctx: &mut Context,
    event_log: &mut EventLog,
) {
    let servers = std::mem::take(&mut ctx.tcp_mock_service_servers);
    for (name, mut server) in servers {
        let recorded_count =
            recorded_mock_service_call_count(&ctx.mock_service_handles, name.as_str());
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
        event_log.append(Event::info(
            "mock-service",
            format!("mock service '{name}' stopped; {recorded_count} calls recorded"),
        ));
    }
}

#[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
pub(crate) async fn shutdown_tcp_mock_service_servers_silent(ctx: &mut Context) {
    let servers = std::mem::take(&mut ctx.tcp_mock_service_servers);
    for (_name, mut server) in servers {
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
    }
}

#[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
pub(crate) async fn shutdown_udp_mock_service_servers_with_log(
    ctx: &mut Context,
    event_log: &mut EventLog,
) {
    let servers = std::mem::take(&mut ctx.udp_mock_service_servers);
    for (name, mut server) in servers {
        let recorded_count =
            recorded_mock_service_call_count(&ctx.mock_service_handles, name.as_str());
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
        event_log.append(Event::info(
            "mock-service",
            format!("mock service '{name}' stopped; {recorded_count} calls recorded"),
        ));
    }
}

#[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
pub(crate) async fn shutdown_udp_mock_service_servers_silent(ctx: &mut Context) {
    let servers = std::mem::take(&mut ctx.udp_mock_service_servers);
    for (_name, mut server) in servers {
        if let Some(sender) = server.shutdown_tx.take() {
            let _ = sender.send(());
        }
        let _ = server.join.await;
    }
}

#[cfg(feature = "mock-services")]
pub(crate) async fn start_and_readiness_mock_services(
    ctx: &mut Context,
    event_log: &mut EventLog,
) -> Result<(), ShakedownError> {
    let pending_http = std::mem::take(&mut ctx.pending_http_mock_services);
    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    let pending_grpc = std::mem::take(&mut ctx.pending_grpc_mock_services);
    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    let pending_tcp = std::mem::take(&mut ctx.pending_tcp_mock_services);
    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    let pending_udp = std::mem::take(&mut ctx.pending_udp_mock_services);

    let nothing_to_start = pending_http.is_empty()
        && {
            #[cfg(all(feature = "mock-services", feature = "grpc"))]
            {
                pending_grpc.is_empty()
            }
            #[cfg(not(all(feature = "mock-services", feature = "grpc")))]
            {
                true
            }
        }
        && {
            #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
            {
                pending_tcp.is_empty()
            }
            #[cfg(not(all(feature = "mock-services", feature = "tcp-mock-services")))]
            {
                true
            }
        }
        && {
            #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
            {
                pending_udp.is_empty()
            }
            #[cfg(not(all(feature = "mock-services", feature = "udp-mock-services")))]
            {
                true
            }
        };

    if nothing_to_start {
        ctx.mock_service_servers_phase_complete = true;
        return Ok(());
    }

    let resources = mock_service_resources_from_context(ctx);
    let scenario_id_string = ctx.scenario_id().to_string();

    let mut started_http_mock_servers: Vec<(String, HttpMockServiceServer)> = Vec::new();
    for (mock_service_name, mock_service_impl) in pending_http {
        match spawn_http_mock_service_server(
            mock_service_name.clone(),
            mock_service_impl,
            scenario_id_string.clone(),
            Arc::clone(&resources),
            None,
            ctx.mock_service_bind_ip,
        )
        .await
        {
            Ok((handle, server)) => {
                let bind_display = handle.bind_address.clone();
                ctx.mock_service_handles
                    .insert(mock_service_name.clone(), handle);
                started_http_mock_servers.push((mock_service_name.clone(), server));
                event_log.append(Event::info(
                    "mock-service",
                    format!("mock service '{mock_service_name}' started on {bind_display}"),
                ));
            }
            Err(error) => {
                for (_rollback_name, mut server) in started_http_mock_servers {
                    if let Some(sender) = server.shutdown_tx.take() {
                        let _ = sender.send(());
                    }
                    let _ = server.join.await;
                }
                ctx.mock_service_handles.clear();
                return Err(InfrastructureError::Provision {
                    resource: format!("mock-service:{mock_service_name}"),
                    source: Box::new(error),
                }
                .into());
            }
        }
    }
    ctx.http_mock_service_servers = started_http_mock_servers;

    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    {
        let mut started_grpc_mock_servers: Vec<(String, GrpcMockServiceServer)> = Vec::new();
        for (mock_service_name, mock_service_impl) in pending_grpc {
            match spawn_grpc_mock_service_server(
                mock_service_name.clone(),
                mock_service_impl,
                scenario_id_string.clone(),
                Arc::clone(&resources),
                None,
                ctx.mock_service_bind_ip,
            )
            .await
            {
                Ok((handle, server)) => {
                    let bind_display = handle.bind_address.clone();
                    ctx.mock_service_handles
                        .insert(mock_service_name.clone(), handle);
                    started_grpc_mock_servers.push((mock_service_name.clone(), server));
                    event_log.append(Event::info(
                        "mock-service",
                        format!("mock service '{mock_service_name}' started on {bind_display}"),
                    ));
                }
                Err(error) => {
                    shutdown_http_mock_service_servers_silent(ctx).await;
                    for (_rollback_name, mut server) in started_grpc_mock_servers {
                        if let Some(sender) = server.shutdown_tx.take() {
                            let _ = sender.send(());
                        }
                        let _ = server.join.await;
                    }
                    ctx.mock_service_handles.clear();
                    return Err(InfrastructureError::Provision {
                        resource: format!("mock-service:{mock_service_name}"),
                        source: Box::new(error),
                    }
                    .into());
                }
            }
        }
        ctx.grpc_mock_service_servers = started_grpc_mock_servers;
    }

    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    {
        let mut started_tcp_mock_servers: Vec<(String, TcpMockServiceServer)> = Vec::new();
        for (mock_service_name, registration) in pending_tcp {
            match spawn_tcp_mock_service_server(
                mock_service_name.clone(),
                Arc::clone(&registration.connection),
                registration.tls.clone(),
                scenario_id_string.clone(),
                Arc::clone(&resources),
                None,
                ctx.mock_service_bind_ip,
            )
            .await
            {
                Ok((handle, server)) => {
                    let bind_display = handle.bind_address.clone();
                    ctx.mock_service_handles
                        .insert(mock_service_name.clone(), handle);
                    started_tcp_mock_servers.push((mock_service_name.clone(), server));
                    event_log.append(Event::info(
                        "mock-service",
                        format!("mock service '{mock_service_name}' started on {bind_display}"),
                    ));
                }
                Err(error) => {
                    shutdown_http_mock_service_servers_silent(ctx).await;
                    #[cfg(all(feature = "mock-services", feature = "grpc"))]
                    shutdown_grpc_mock_service_servers_silent(ctx).await;
                    for (_rollback_name, mut server) in started_tcp_mock_servers {
                        if let Some(sender) = server.shutdown_tx.take() {
                            let _ = sender.send(());
                        }
                        let _ = server.join.await;
                    }
                    ctx.mock_service_handles.clear();
                    return Err(InfrastructureError::Provision {
                        resource: format!("mock-service:{mock_service_name}"),
                        source: Box::new(error),
                    }
                    .into());
                }
            }
        }
        ctx.tcp_mock_service_servers = started_tcp_mock_servers;
    }

    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    {
        let mut started_udp_mock_servers: Vec<(String, UdpMockServiceServer)> = Vec::new();
        for (mock_service_name, registration) in pending_udp {
            match spawn_udp_mock_service_server(
                mock_service_name.clone(),
                Arc::clone(&registration.handler),
                scenario_id_string.clone(),
                Arc::clone(&resources),
                None,
                ctx.mock_service_bind_ip,
            )
            .await
            {
                Ok((handle, server)) => {
                    let bind_display = handle.bind_address.clone();
                    ctx.mock_service_handles
                        .insert(mock_service_name.clone(), handle);
                    started_udp_mock_servers.push((mock_service_name.clone(), server));
                    event_log.append(Event::info(
                        "mock-service",
                        format!("mock service '{mock_service_name}' started on {bind_display}"),
                    ));
                }
                Err(error) => {
                    shutdown_http_mock_service_servers_silent(ctx).await;
                    #[cfg(all(feature = "mock-services", feature = "grpc"))]
                    shutdown_grpc_mock_service_servers_silent(ctx).await;
                    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
                    shutdown_tcp_mock_service_servers_silent(ctx).await;
                    for (_rollback_name, mut server) in started_udp_mock_servers {
                        if let Some(sender) = server.shutdown_tx.take() {
                            let _ = sender.send(());
                        }
                        let _ = server.join.await;
                    }
                    ctx.mock_service_handles.clear();
                    return Err(InfrastructureError::Provision {
                        resource: format!("mock-service:{mock_service_name}"),
                        source: Box::new(error),
                    }
                    .into());
                }
            }
        }
        ctx.udp_mock_service_servers = started_udp_mock_servers;
    }

    let readiness_targets: Vec<(String, String, String)> = ctx
        .mock_service_handles
        .iter()
        .map(|(mock_service_name, handle)| {
            (
                mock_service_name.clone(),
                handle.bind_address.clone(),
                handle.client_url.clone(),
            )
        })
        .collect();
    for (mock_service_name, bind_address, client_url) in readiness_targets {
        if let Err(source) =
            mock_service_bind_address_ready(client_url.as_str(), bind_address.as_str()).await
        {
            shutdown_http_mock_service_servers_silent(ctx).await;
            #[cfg(all(feature = "mock-services", feature = "grpc"))]
            shutdown_grpc_mock_service_servers_silent(ctx).await;
            #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
            shutdown_tcp_mock_service_servers_silent(ctx).await;
            #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
            shutdown_udp_mock_service_servers_silent(ctx).await;
            ctx.mock_service_handles.clear();
            return Err(InfrastructureError::Readiness {
                resource: format!("mock-service:{mock_service_name}"),
                source: Box::new(source),
            }
            .into());
        }
    }
    ctx.mock_service_servers_phase_complete = true;
    Ok(())
}
