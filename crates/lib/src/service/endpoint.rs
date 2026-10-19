//! Published address of a service port on the harness host (`services` feature).
//!
//! The variant follows the readiness probe. HTTP and gRPC probes publish an `http://127.0.0.1`
//! origin (tonic dials gRPC health on that origin). TCP and in-container command probes publish
//! a host and port with no scheme.

use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};

use super::health::HealthCheck;

/// IPv4 port on the harness host, mapped from a service container port.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MappedHostPort(u16);

impl MappedHostPort {
    pub(crate) fn new(port: u16) -> Self {
        Self(port)
    }

    /// Port number on `127.0.0.1`.
    pub fn as_u16(self) -> u16 {
        self.0
    }
}

/// `http://127.0.0.1:{port}` origin on the harness loopback.
///
/// HTTP clients request this origin. The gRPC health probe dials the same origin with HTTP/2.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceHttpOrigin {
    port: MappedHostPort,
    origin: String,
}

impl ServiceHttpOrigin {
    pub(crate) fn loopback(host_port: u16) -> Self {
        let port = MappedHostPort::new(host_port);
        let address = ServiceHostPort::from_mapped(port);
        Self {
            port,
            origin: format!("http://{address}"),
        }
    }

    /// Origin string, including the `http` scheme.
    pub fn as_str(&self) -> &str {
        self.origin.as_str()
    }

    /// Mapped host port inside the origin.
    pub fn port(&self) -> MappedHostPort {
        self.port
    }
}

/// Loopback host and port with no application scheme (`127.0.0.1:{port}`).
///
/// Published when readiness is a TCP connect or an in-container command.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ServiceHostPort {
    port: MappedHostPort,
}

impl ServiceHostPort {
    pub(crate) fn loopback(host_port: u16) -> Self {
        Self::from_mapped(MappedHostPort::new(host_port))
    }

    fn from_mapped(port: MappedHostPort) -> Self {
        Self { port }
    }

    /// Mapped host port.
    pub fn port(self) -> MappedHostPort {
        self.port
    }

    /// `127.0.0.1` socket for this port.
    pub fn socket_addr(self) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, self.port.as_u16()))
    }
}

impl fmt::Display for ServiceHostPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.socket_addr())
    }
}

/// Address the scenario uses for one published port of a running service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceEndpoint {
    /// HTTP origin. Readiness included an HTTP GET probe.
    Http(ServiceHttpOrigin),
    /// gRPC health origin. Tonic dials [`ServiceHttpOrigin::as_str`].
    #[cfg(feature = "grpc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "grpc")))]
    Grpc(ServiceHttpOrigin),
    /// Host and port. Readiness was TCP or an in-container command.
    HostPort(ServiceHostPort),
}

impl ServiceEndpoint {
    /// Address for `host_port` implied by `health_check`.
    ///
    /// A composite publishes [`ServiceEndpoint::Http`] when any leaf is HTTP. Otherwise it
    /// publishes [`ServiceEndpoint::Grpc`] when any leaf is a gRPC health probe. TCP and command
    /// leaves publish [`ServiceEndpoint::HostPort`].
    pub(crate) fn for_readiness(health_check: &HealthCheck, host_port: u16) -> Self {
        match published_scheme(health_check) {
            PublishedScheme::Http => Self::Http(ServiceHttpOrigin::loopback(host_port)),
            #[cfg(feature = "grpc")]
            PublishedScheme::Grpc => Self::Grpc(ServiceHttpOrigin::loopback(host_port)),
            PublishedScheme::HostPort => Self::HostPort(ServiceHostPort::loopback(host_port)),
        }
    }

    /// `http://` origin when this endpoint is HTTP or gRPC.
    pub fn http_url(&self) -> Option<&str> {
        match self {
            ServiceEndpoint::Http(origin) => Some(origin.as_str()),
            #[cfg(feature = "grpc")]
            ServiceEndpoint::Grpc(origin) => Some(origin.as_str()),
            ServiceEndpoint::HostPort(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublishedScheme {
    Http,
    #[cfg(feature = "grpc")]
    Grpc,
    HostPort,
}

fn published_scheme(health_check: &HealthCheck) -> PublishedScheme {
    match health_check {
        HealthCheck::Http { .. } => PublishedScheme::Http,
        #[cfg(feature = "grpc")]
        HealthCheck::Grpc { .. } => PublishedScheme::Grpc,
        HealthCheck::Tcp { .. } | HealthCheck::Command { .. } => PublishedScheme::HostPort,
        HealthCheck::Composite { checks, .. } => scheme_of_composite(checks),
    }
}

fn scheme_of_composite(checks: &[HealthCheck]) -> PublishedScheme {
    #[cfg(feature = "grpc")]
    let mut saw_grpc = false;
    for check in checks {
        match published_scheme(check) {
            PublishedScheme::Http => return PublishedScheme::Http,
            #[cfg(feature = "grpc")]
            PublishedScheme::Grpc => saw_grpc = true,
            PublishedScheme::HostPort => {}
        }
    }
    #[cfg(feature = "grpc")]
    if saw_grpc {
        return PublishedScheme::Grpc;
    }
    PublishedScheme::HostPort
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_readiness_publishes_loopback_origin() {
        // Given an HTTP probe and a mapped host port
        let endpoint = ServiceEndpoint::for_readiness(&HealthCheck::http("/ready"), 8080);
        // Then the scenario origin is http on loopback
        assert_eq!(endpoint.http_url(), Some("http://127.0.0.1:8080"));
        let ServiceEndpoint::Http(origin) = &endpoint else {
            panic!("expected Http");
        };
        assert_eq!(origin.as_str(), "http://127.0.0.1:8080");
        assert_eq!(origin.port().as_u16(), 8080);
    }

    #[test]
    #[cfg(feature = "grpc")]
    fn grpc_readiness_publishes_tonic_http_origin() {
        // Given a gRPC health probe
        let endpoint = ServiceEndpoint::for_readiness(
            &HealthCheck::grpc_service("payments.v1.Payments"),
            50051,
        );
        // Then the origin stays the http URL tonic dials
        assert_eq!(endpoint.http_url(), Some("http://127.0.0.1:50051"));
        let ServiceEndpoint::Grpc(origin) = &endpoint else {
            panic!("expected Grpc");
        };
        assert_eq!(origin.as_str(), "http://127.0.0.1:50051");
    }

    #[test]
    fn tcp_readiness_publishes_host_port() {
        // Given a TCP probe
        let endpoint = ServiceEndpoint::for_readiness(&HealthCheck::tcp(), 5432);
        // Then the address is loopback host and port, with no scheme
        assert!(endpoint.http_url().is_none());
        let ServiceEndpoint::HostPort(address) = endpoint else {
            panic!("expected HostPort");
        };
        assert_eq!(address.port().as_u16(), 5432);
        assert_eq!(
            address.socket_addr(),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 5432))
        );
        assert_eq!(address.to_string(), "127.0.0.1:5432");
    }

    #[test]
    fn command_readiness_publishes_host_port() {
        // Given an in-container command probe
        let endpoint = ServiceEndpoint::for_readiness(&HealthCheck::command(["/bin/true"]), 6400);
        assert!(endpoint.http_url().is_none());
        let ServiceEndpoint::HostPort(address) = endpoint else {
            panic!("expected HostPort");
        };
        assert_eq!(address.to_string(), "127.0.0.1:6400");
    }

    #[test]
    fn composite_with_http_leaf_publishes_http() {
        // Given a composite whose leaves include HTTP
        let health = HealthCheck::composite([
            HealthCheck::tcp(),
            HealthCheck::composite([HealthCheck::http("/live")]),
        ]);
        let endpoint = ServiceEndpoint::for_readiness(&health, 80);
        assert!(matches!(endpoint, ServiceEndpoint::Http(_)));
        assert_eq!(endpoint.http_url(), Some("http://127.0.0.1:80"));
    }

    #[test]
    fn composite_of_tcp_and_command_publishes_host_port() {
        // Given only TCP and command leaves
        let health = HealthCheck::composite([
            HealthCheck::command(["pg_isready"]),
            HealthCheck::tcp(),
            HealthCheck::composite(Vec::<HealthCheck>::new()),
        ]);
        let endpoint = ServiceEndpoint::for_readiness(&health, 5432);
        assert!(matches!(endpoint, ServiceEndpoint::HostPort(_)));
        assert!(endpoint.http_url().is_none());
    }

    #[test]
    #[cfg(feature = "grpc")]
    fn composite_grpc_without_http_publishes_grpc_origin() {
        // Given gRPC and TCP leaves and no HTTP leaf
        let health = HealthCheck::composite([HealthCheck::tcp(), HealthCheck::grpc()]);
        let endpoint = ServiceEndpoint::for_readiness(&health, 50051);
        assert!(matches!(endpoint, ServiceEndpoint::Grpc(_)));
        assert_eq!(endpoint.http_url(), Some("http://127.0.0.1:50051"));
    }

    #[test]
    #[cfg(feature = "grpc")]
    fn composite_http_and_grpc_publishes_http() {
        // Given both HTTP and gRPC leaves
        let health = HealthCheck::composite([HealthCheck::grpc(), HealthCheck::http("/")]);
        let endpoint = ServiceEndpoint::for_readiness(&health, 8080);
        assert!(matches!(endpoint, ServiceEndpoint::Http(_)));
        assert_eq!(endpoint.http_url(), Some("http://127.0.0.1:8080"));
    }
}
