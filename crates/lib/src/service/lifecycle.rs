//! Start, readiness, and teardown for service-under-test containers (`services` feature).

use std::time::{Duration, Instant};

use testcontainers::core::ExecCommand;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use crate::docker_labels::{
    SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE, SHAKEDOWN_SERVICE_LABEL_KEY,
};
use crate::error::InfrastructureError;
use crate::event::{EventLevel, EventLog};
use crate::lifecycle::Context;
use crate::service::address::ensure_host_docker_internal_resolution;
use crate::service::dependency::service_startup_order;
use crate::service::handle::ServiceHandle;
use crate::service::health::HealthCheck;
use crate::service::injection::inject_service_env;
use crate::service::types::PullPolicy;

fn split_image_repository_and_tag(full_image: &str) -> (String, String) {
    if let Some((left, right)) = full_image.rsplit_once(':') {
        if !right.contains('/') && !right.is_empty() {
            return (left.to_string(), right.to_string());
        }
    }
    (full_image.to_string(), "latest".to_string())
}

fn map_service_provision_error(
    service_name: &str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> InfrastructureError {
    InfrastructureError::Provision {
        resource: format!("service:{service_name}"),
        source: Box::new(source),
    }
}

fn map_service_readiness_error(
    service_name: &str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> InfrastructureError {
    InfrastructureError::Readiness {
        resource: format!("service:{service_name}"),
        source: Box::new(source),
    }
}

fn map_service_pre_start_error(
    service_name: &str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> InfrastructureError {
    InfrastructureError::ServicePreStart {
        resource: format!("service:{service_name}"),
        source: Box::new(source),
    }
}

fn map_service_teardown_error(
    service_name: &str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> InfrastructureError {
    InfrastructureError::Teardown {
        resource: format!("service:{service_name}"),
        source: Box::new(source),
    }
}

/// Start registered service containers after mock services, run readiness probes, and attach handles on `Context`.
pub(crate) async fn start_and_readiness_services(
    context: &mut Context,
    event_log: &mut EventLog,
) -> Result<(), InfrastructureError> {
    if context.services.is_empty() {
        return Ok(());
    }

    event_log.append_raw(
        EventLevel::Info,
        "lifecycle",
        "Starting service container phase (provision + readiness)",
    );

    let order = service_startup_order(&context.services).map_err(|error| {
        InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
    })?;

    for service_name in order {
        if let Err(error) = start_one_service(context, event_log, service_name.as_str()).await {
            let _ = teardown_service_containers(context, event_log).await;
            return Err(error);
        }
    }

    Ok(())
}

async fn start_one_service(
    context: &mut Context,
    event_log: &mut EventLog,
    service_name: &str,
) -> Result<(), InfrastructureError> {
    let specification = context
        .services
        .get(service_name)
        .map(|state| state.spec.clone())
        .ok_or_else(|| {
            InfrastructureError::Backend(Box::new(std::io::Error::other(
                "service lifecycle: missing registered service",
            )))
        })?;

    let pre_start_total = specification.pre_start_callbacks.len();
    for (index, callback) in specification.pre_start_callbacks.iter().enumerate() {
        event_log.append_raw(
            EventLevel::Info,
            "lifecycle",
            format!(
                "service '{service_name}' pre_start ({}/{pre_start_total})",
                index + 1
            ),
        );
        crate::service::pre_start::ServicePreStart::invoke(callback.as_ref(), &*context)
            .await
            .map_err(|error| map_service_pre_start_error(service_name, error))?;
    }

    let environment = inject_service_env(context, &specification);

    let (repository, tag) = split_image_repository_and_tag(specification.image.as_str());
    let mut image = GenericImage::new(repository, tag);
    for port_mapping in &specification.ports {
        image = image.with_exposed_port(port_mapping.container_port.tcp());
    }
    let mut request = image
        .with_label(SHAKEDOWN_DOCKER_LABEL_KEY, SHAKEDOWN_DOCKER_LABEL_VALUE)
        .with_label(SHAKEDOWN_SERVICE_LABEL_KEY, service_name);
    for (key, value) in environment {
        request = request.with_env_var(key, value);
    }
    request = ensure_host_docker_internal_resolution(request);
    request = match specification.pull_policy {
        PullPolicy::Always => request
            .pull_image()
            .await
            .map_err(|error| map_service_provision_error(service_name, error))?,
        PullPolicy::IfNotPresent | PullPolicy::Never => request,
    };

    let container = request
        .start()
        .await
        .map_err(|error| map_service_provision_error(service_name, error))?;

    if let Some(state) = context.services.get_mut(service_name) {
        state.container = Some(container);
    }

    let container_ref = context
        .services
        .get(service_name)
        .and_then(|state| state.container.as_ref())
        .ok_or_else(|| {
            InfrastructureError::Backend(Box::new(std::io::Error::other(
                "service container missing after start",
            )))
        })?;

    let mut host_ports: Vec<(Option<String>, u16)> = Vec::new();
    for port_mapping in &specification.ports {
        let host_port = container_ref
            .get_host_port_ipv4(port_mapping.container_port.tcp())
            .await
            .map_err(|error| map_service_provision_error(service_name, error))?;
        host_ports.push((port_mapping.name.clone(), host_port));
    }

    if host_ports.is_empty() {
        return Err(map_service_provision_error(
            service_name,
            std::io::Error::other("service has no exposed ports after start"),
        ));
    }

    let primary_host_port = host_ports[0].1;
    let base_url = format!("http://127.0.0.1:{primary_host_port}");
    let mut named_urls = std::collections::HashMap::new();
    for (maybe_name, host_port) in &host_ports {
        if let Some(port_name) = maybe_name {
            named_urls.insert(port_name.clone(), format!("http://127.0.0.1:{host_port}"));
        }
    }

    let container_id = Some(container_ref.id().to_string());

    let last_probe_outcome = run_health_convergence(
        service_name,
        &specification.health_check,
        container_ref,
        &base_url,
        &host_ports,
    )
    .await
    .err();

    if let Some(message) = last_probe_outcome {
        if let Some(state) = context.services.get_mut(service_name) {
            if let Some(container) = state.container.take() {
                let _ = container.rm().await;
            }
            state.handle = None;
        }
        return Err(map_service_readiness_error(
            service_name,
            std::io::Error::other(format!(
                "HINT: service readiness failed (test infrastructure / accidental complexity): \
                 service '{service_name}' did not become healthy. {message}"
            )),
        ));
    }

    let handle = ServiceHandle::new(
        service_name.to_string(),
        base_url,
        named_urls,
        container_id,
        true,
    );

    context
        .set_service_handle(service_name, handle)
        .map_err(|error| {
            InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
        })?;

    event_log.append_raw(
        EventLevel::Info,
        "lifecycle",
        format!("service '{service_name}' is healthy"),
    );

    Ok(())
}

fn collect_leaf_health_checks<'a>(
    health_check: &'a HealthCheck,
    leaves: &mut Vec<&'a HealthCheck>,
) {
    match health_check {
        HealthCheck::Composite { checks, .. } => {
            for nested in checks {
                collect_leaf_health_checks(nested, leaves);
            }
        }
        _ => leaves.push(health_check),
    }
}

async fn run_health_convergence(
    _service_name: &str,
    health_check: &HealthCheck,
    container: &ContainerAsync<GenericImage>,
    base_url: &str,
    host_ports: &[(Option<String>, u16)],
) -> Result<(), String> {
    let mut leaves = Vec::new();
    collect_leaf_health_checks(health_check, &mut leaves);
    for leaf in leaves {
        run_leaf_health_check(leaf, container, base_url, host_ports).await?;
    }
    Ok(())
}

async fn run_leaf_health_check(
    health_check: &HealthCheck,
    container: &ContainerAsync<GenericImage>,
    base_url: &str,
    host_ports: &[(Option<String>, u16)],
) -> Result<(), String> {
    match health_check {
        HealthCheck::Http {
            path,
            expected_status,
            timeout,
            interval,
        } => {
            converge_poll(*timeout, *interval, || {
                http_probe_once(base_url, path.as_str(), *expected_status)
            })
            .await
        }
        HealthCheck::Tcp { timeout, interval } => {
            let primary_port = host_ports
                .first()
                .map(|(_, port)| *port)
                .ok_or_else(|| "no host port for TCP health check".to_string())?;
            converge_poll(*timeout, *interval, || tcp_probe_once(primary_port)).await
        }
        HealthCheck::Command {
            cmd,
            timeout,
            interval,
        } => {
            converge_poll(*timeout, *interval, || {
                command_probe_once(container, cmd.clone())
            })
            .await
        }
        #[cfg(feature = "grpc")]
        HealthCheck::Grpc {
            service,
            timeout,
            interval,
        } => {
            converge_poll(*timeout, *interval, || {
                let base_url_owned = base_url.to_string();
                let service = service.clone();
                async move { grpc_probe_once(&base_url_owned, &service).await }
            })
            .await
        }
        HealthCheck::Composite { .. } => {
            Err("internal error: composite health check leaked into leaf probe".to_string())
        }
    }
}

async fn converge_poll<F, Fut>(
    overall_timeout: Duration,
    interval: Duration,
    mut probe_one_attempt: F,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let deadline = Instant::now() + overall_timeout;
    let mut last = "no probe attempts".to_string();
    while Instant::now() < deadline {
        match probe_one_attempt().await {
            Ok(()) => return Ok(()),
            Err(message) => {
                last = message;
                tokio::time::sleep(interval).await;
            }
        }
    }
    Err(format!(
        "timed out after {:?}; last probe result: {last}",
        overall_timeout
    ))
}

async fn http_probe_once(base_url: &str, path: &str, expected_status: u16) -> Result<(), String> {
    let url = if path.starts_with('/') {
        format!("{}{}", base_url.trim_end_matches('/'), path)
    } else {
        format!("{}/{}", base_url.trim_end_matches('/'), path)
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|error| error.to_string())?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status = response.status().as_u16();
    if status == expected_status {
        Ok(())
    } else {
        Err(format!(
            "HTTP GET expected status {expected_status}, got {status}"
        ))
    }
}

async fn tcp_probe_once(host_port: u16) -> Result<(), String> {
    let socket_address: std::net::SocketAddr = format!("127.0.0.1:{host_port}")
        .parse()
        .map_err(|error: std::net::AddrParseError| error.to_string())?;
    let connect_outcome = tokio::time::timeout(
        Duration::from_secs(3),
        tokio::net::TcpStream::connect(socket_address),
    )
    .await;
    match connect_outcome {
        Ok(Ok(stream)) => {
            drop(stream);
            Ok(())
        }
        Ok(Err(source)) => Err(source.to_string()),
        Err(_elapsed) => Err("TCP connect timed out".to_string()),
    }
}

async fn command_probe_once(
    container: &ContainerAsync<GenericImage>,
    command: Vec<String>,
) -> Result<(), String> {
    if command.is_empty() {
        return Err("health check command argv is empty".to_string());
    }
    let exec_command = ExecCommand::new(command);
    let execution = container
        .exec(exec_command)
        .await
        .map_err(|error| error.to_string())?;
    let code = execution
        .exit_code()
        .await
        .map_err(|error| error.to_string())?;
    match code {
        Some(0) => Ok(()),
        Some(value) => Err(format!("exec exited with status {value}")),
        None => Err("exec exit code not available".to_string()),
    }
}

/// `grpc.health.v1.Health/Check` against `base_url` (HTTP origin, e.g. `http://127.0.0.1:8080`).
#[cfg(feature = "grpc")]
async fn grpc_probe_once(base_url: &str, service: &Option<String>) -> Result<(), String> {
    use tonic::transport::Endpoint;
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::HealthCheckRequest;

    let endpoint = Endpoint::from_shared(base_url.to_string())
        .map_err(|error| format!("invalid gRPC health endpoint URL: {error}"))?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|error| format!("gRPC health connect failed: {error}"))?;

    let mut client = HealthClient::new(channel);
    let request = tonic::Request::new(HealthCheckRequest {
        service: service.clone().unwrap_or_default(),
    });
    let response = client
        .check(request)
        .await
        .map_err(|error| format!("gRPC Health/Check request failed: {error}"))?;
    let status = response.into_inner().status();
    match status {
        ServingStatus::Serving => Ok(()),
        other => Err(format!(
            "gRPC health status is {other:?}, expected SERVING (probe grpc.health.v1.Health/Check)"
        )),
    }
}

/// Stop service containers in reverse startup order; append truncated logs to `event_log` before removal.
pub(crate) async fn teardown_service_containers(
    context: &mut Context,
    event_log: &mut EventLog,
) -> Result<(), InfrastructureError> {
    if context.services.is_empty() {
        return Ok(());
    }

    let order = service_startup_order(&context.services).map_err(|error| {
        InfrastructureError::Backend(Box::new(std::io::Error::other(error.to_string())))
    })?;

    event_log.append_raw(
        EventLevel::Info,
        "lifecycle",
        "Tearing down service containers",
    );

    for service_name in order.iter().rev() {
        teardown_one_service(context, event_log, service_name.as_str()).await?;
    }

    Ok(())
}

async fn teardown_one_service(
    context: &mut Context,
    event_log: &mut EventLog,
    service_name: &str,
) -> Result<(), InfrastructureError> {
    let Some(state) = context.services.get_mut(service_name) else {
        return Ok(());
    };
    let Some(container) = state.container.take() else {
        return Ok(());
    };

    let stdout = container
        .stdout_to_vec()
        .await
        .unwrap_or_else(|_| Vec::new());
    let stderr = container
        .stderr_to_vec()
        .await
        .unwrap_or_else(|_| Vec::new());
    let mut combined = String::new();
    let stdout_text = String::from_utf8_lossy(&stdout);
    let stderr_text = String::from_utf8_lossy(&stderr);
    let limit = 8000usize;
    if !stdout_text.is_empty() {
        let slice = trim_log_slice(stdout_text.as_ref(), limit);
        combined.push_str("--- stdout ---\n");
        combined.push_str(slice);
        combined.push('\n');
    }
    if !stderr_text.is_empty() {
        let slice = trim_log_slice(stderr_text.as_ref(), limit);
        combined.push_str("--- stderr ---\n");
        combined.push_str(slice);
        combined.push('\n');
    }
    if !combined.is_empty() {
        event_log.append_raw(
            EventLevel::Debug,
            "lifecycle",
            format!("service '{service_name}' container logs before removal:\n{combined}"),
        );
    }

    container
        .rm()
        .await
        .map_err(|error| map_service_teardown_error(service_name, error))?;

    state.handle = None;

    event_log.append_raw(
        EventLevel::Info,
        "lifecycle",
        format!("service '{service_name}' container removed"),
    );

    Ok(())
}

fn trim_log_slice(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    match text.char_indices().nth(max_bytes) {
        Some((byte_index, _)) => &text[..byte_index],
        None => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_image_basic_tag() {
        // Given a normal image:tag reference
        let (repository, tag) = split_image_repository_and_tag("traefik/whoami:v1.10");
        // Then repository and tag split on the last non-path colon
        assert_eq!(repository, "traefik/whoami");
        assert_eq!(tag, "v1.10");
    }

    #[test]
    fn split_image_registry_with_port_no_tag() {
        // Given registry host:port without a tag (colon is not a tag delimiter)
        let (repository, tag) = split_image_repository_and_tag("localhost:5000/foo");
        assert_eq!(repository, "localhost:5000/foo");
        assert_eq!(tag, "latest");
    }

    #[test]
    fn split_image_registry_with_port_and_tag() {
        // Given registry:port/repo:tag
        let (repository, tag) = split_image_repository_and_tag("localhost:5000/foo:1.0");
        assert_eq!(repository, "localhost:5000/foo");
        assert_eq!(tag, "1.0");
    }
}
