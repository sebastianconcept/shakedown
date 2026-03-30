//! Host ↔ container address resolution for service-under-test env injection (`services` feature).
//!
//! Infrastructure and bridges bind on the host loopback. Service containers must reach them via
//! [`HOST_DOCKER_INTERNAL`]. On Linux, Docker does not define that hostname unless
//! `--add-host host.docker.internal:host-gateway` is set — use
//! [`ensure_host_docker_internal_resolution`] when building the container request (US-009).

use testcontainers::core::Image;
use testcontainers::ContainerRequest;

/// Hostname Docker exposes (or that we map via `host-gateway`) for reaching the host from a container.
pub const HOST_DOCKER_INTERNAL: &str = "host.docker.internal";

/// Returns whether this process runs on Linux (`host.docker.internal` needs an explicit host mapping).
#[inline]
pub fn linux_host_gateway_mapping_required() -> bool {
    cfg!(target_os = "linux")
}

/// Apply Docker’s `host-gateway` mapping for [`HOST_DOCKER_INTERNAL`] on Linux only.
/// macOS and Windows Docker Desktop provide this name without extra configuration.
pub fn ensure_host_docker_internal_resolution<I: Image>(
    request: ContainerRequest<I>,
) -> ContainerRequest<I> {
    #[cfg(target_os = "linux")]
    {
        use testcontainers::core::Host;
        use testcontainers::ImageExt;
        request.with_host(HOST_DOCKER_INTERNAL, Host::HostGateway)
    }
    #[cfg(not(target_os = "linux"))]
    {
        request
    }
}

/// Rewrite loopback hostnames and IPs in injected env values so a **service container** can reach
/// the harness host. The scenario process should keep using `127.0.0.1` in [`Context`] bindings;
/// only values passed into the container’s environment are rewritten.
pub fn rewrite_injected_addresses_for_service_container(value: &str) -> String {
    let step_one = replace_ipv4_loopback(value);
    let step_two = replace_ipv6_loopback(&step_one);
    let step_three = replace_localhost_authority(&step_two);
    replace_bare_localhost_ports(&step_three)
}

fn replace_ipv4_loopback(input: &str) -> String {
    const NEEDLE: &str = "127.0.0.1";
    let mut output = String::with_capacity(input.len());
    let mut rest = input;
    while !rest.is_empty() {
        if let Some(index) = rest.find(NEEDLE) {
            output.push_str(&rest[..index]);
            let after = &rest[index + NEEDLE.len()..];
            let next_is_digit = after
                .chars()
                .next()
                .map(|character| character.is_ascii_digit())
                .unwrap_or(false);
            if next_is_digit {
                output.push_str(NEEDLE);
            } else {
                output.push_str(HOST_DOCKER_INTERNAL);
            }
            rest = after;
        } else {
            output.push_str(rest);
            break;
        }
    }
    output
}

fn replace_ipv6_loopback(input: &str) -> String {
    input.replace("[::1]", HOST_DOCKER_INTERNAL)
}

fn replace_localhost_authority(input: &str) -> String {
    let mut out = input.to_string();
    let pairs: [(&str, String); 6] = [
        (
            "http://localhost:",
            format!("http://{HOST_DOCKER_INTERNAL}:"),
        ),
        (
            "https://localhost:",
            format!("https://{HOST_DOCKER_INTERNAL}:"),
        ),
        (
            "http://localhost/",
            format!("http://{HOST_DOCKER_INTERNAL}/"),
        ),
        (
            "https://localhost/",
            format!("https://{HOST_DOCKER_INTERNAL}/"),
        ),
        (
            "http://localhost?",
            format!("http://{HOST_DOCKER_INTERNAL}?"),
        ),
        (
            "https://localhost?",
            format!("https://{HOST_DOCKER_INTERNAL}?"),
        ),
    ];
    for (from, to) in pairs {
        if out.contains(from) {
            out = out.replace(from, &to);
        }
    }
    if out.contains("@localhost:") {
        out = out.replace("@localhost:", &format!("@{HOST_DOCKER_INTERNAL}:"));
    }
    out
}

fn replace_bare_localhost_ports(input: &str) -> String {
    let mut out = input.to_string();
    if out.starts_with("localhost:") {
        out = format!("{}:{}", HOST_DOCKER_INTERNAL, &out["localhost:".len()..]);
    }
    let needle = ",localhost:";
    if out.contains(needle) {
        out = out.replace(needle, &format!(",{HOST_DOCKER_INTERNAL}:"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv4_rewrites_bootstrap_and_jdbc_urls() {
        // Given connection strings containing 127.0.0.1
        // When we rewrite for a service container
        // Then loopback becomes host.docker.internal
        assert_eq!(
            rewrite_injected_addresses_for_service_container("127.0.0.1:9092"),
            format!("{HOST_DOCKER_INTERNAL}:9092")
        );
        assert_eq!(
            rewrite_injected_addresses_for_service_container(
                "postgresql://u:p@127.0.0.1:5432/scenario_db"
            ),
            format!("postgresql://u:p@{HOST_DOCKER_INTERNAL}:5432/scenario_db")
        );
    }

    #[test]
    fn ipv4_does_not_corrupt_127_0_0_10() {
        // Given an IP that is not loopback but shares a prefix with 127.0.0.1
        assert_eq!(
            rewrite_injected_addresses_for_service_container("127.0.0.10:9092"),
            "127.0.0.10:9092"
        );
    }

    #[test]
    fn localhost_http_and_userinfo() {
        // Given localhost in authority and userinfo
        assert_eq!(
            rewrite_injected_addresses_for_service_container("http://localhost:8080/v1"),
            format!("http://{HOST_DOCKER_INTERNAL}:8080/v1")
        );
        assert_eq!(
            rewrite_injected_addresses_for_service_container("amqp://g:p@localhost:5672/%2f"),
            format!("amqp://g:p@{HOST_DOCKER_INTERNAL}:5672/%2f")
        );
    }

    #[test]
    fn ipv6_bracket_loopback() {
        // Given [::1] in a URL
        assert_eq!(
            rewrite_injected_addresses_for_service_container("http://[::1]:8080/"),
            format!("http://{HOST_DOCKER_INTERNAL}:8080/")
        );
    }

    #[test]
    fn kafka_comma_separated_bootstrap() {
        // Given comma-separated broker list
        assert_eq!(
            rewrite_injected_addresses_for_service_container("127.0.0.1:9092,localhost:9093"),
            format!("{HOST_DOCKER_INTERNAL}:9092,{HOST_DOCKER_INTERNAL}:9093")
        );
    }

    #[test]
    fn bare_localhost_bootstrap_includes_colon_before_port() {
        // Given bootstrap form without scheme
        assert_eq!(
            rewrite_injected_addresses_for_service_container("localhost:9092"),
            format!("{HOST_DOCKER_INTERNAL}:9092")
        );
    }

    #[test]
    fn linux_gate_mapping_flag_matches_platform() {
        // Given the compile-time OS
        // Then linux_host_gateway_mapping_required matches cfg!(target_os = "linux")
        assert_eq!(
            linux_host_gateway_mapping_required(),
            cfg!(target_os = "linux")
        );
    }
}
