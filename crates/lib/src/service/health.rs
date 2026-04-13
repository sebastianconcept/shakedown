//! Declarative health probes for service-under-test readiness (`services` feature).
//!
//! Values are pure configuration; convergence polling is implemented in later stories.

use std::time::Duration;

/// Default maximum time to wait for a health check to succeed (30 seconds).
pub const DEFAULT_HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(30);

/// Default delay between health check attempts (500 milliseconds).
pub const DEFAULT_HEALTH_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// Describes how Shakedown should determine that a service container is ready.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum HealthCheck {
    /// HTTP `GET` to `path` must return `expected_status`.
    Http {
        path: String,
        expected_status: u16,
        timeout: Duration,
        interval: Duration,
    },
    /// gRPC health (`grpc.health.v1.Health/Check`) with optional service name (requires **`grpc`**).
    #[cfg_attr(docsrs, doc(cfg(feature = "grpc")))]
    #[cfg(feature = "grpc")]
    Grpc {
        service: Option<String>,
        timeout: Duration,
        interval: Duration,
    },
    /// TCP connect to the mapped host port succeeds.
    Tcp {
        timeout: Duration,
        interval: Duration,
    },
    /// Run a command inside the container (container exec via bollard).
    Command {
        cmd: Vec<String>,
        timeout: Duration,
        interval: Duration,
    },
    /// Require every nested check to succeed (evaluated in order).
    Composite {
        checks: Vec<HealthCheck>,
        timeout: Duration,
        interval: Duration,
    },
}

impl HealthCheck {
    /// HTTP GET on `path`, expecting status **200**.
    pub fn http(path: impl Into<String>) -> Self {
        Self::Http {
            path: path.into(),
            expected_status: 200,
            timeout: DEFAULT_HEALTH_CHECK_TIMEOUT,
            interval: DEFAULT_HEALTH_CHECK_INTERVAL,
        }
    }

    /// HTTP GET on `path`, expecting `expected_status`.
    pub fn http_status(path: impl Into<String>, expected_status: u16) -> Self {
        Self::Http {
            path: path.into(),
            expected_status,
            timeout: DEFAULT_HEALTH_CHECK_TIMEOUT,
            interval: DEFAULT_HEALTH_CHECK_INTERVAL,
        }
    }

    /// gRPC health with the default service (empty `HealthCheckRequest.service`).
    ///
    /// Probes `grpc.health.v1.Health/Check` via [`crate::service::lifecycle`]. Requires the
    /// **`grpc`** Cargo feature (enable `grpc` on the `shakedown` dependency).
    #[cfg(feature = "grpc")]
    pub fn grpc() -> Self {
        Self::Grpc {
            service: None,
            timeout: DEFAULT_HEALTH_CHECK_TIMEOUT,
            interval: DEFAULT_HEALTH_CHECK_INTERVAL,
        }
    }

    /// gRPC health for a named service (`HealthCheckRequest.service`).
    ///
    /// Requires the **`grpc`** Cargo feature.
    #[cfg(feature = "grpc")]
    pub fn grpc_service(service_name: impl Into<String>) -> Self {
        Self::Grpc {
            service: Some(service_name.into()),
            timeout: DEFAULT_HEALTH_CHECK_TIMEOUT,
            interval: DEFAULT_HEALTH_CHECK_INTERVAL,
        }
    }

    /// TCP port open on the service host mapping.
    pub fn tcp() -> Self {
        Self::Tcp {
            timeout: DEFAULT_HEALTH_CHECK_TIMEOUT,
            interval: DEFAULT_HEALTH_CHECK_INTERVAL,
        }
    }

    /// Shell-less command line as argv (container exec).
    pub fn command(command_line: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::Command {
            cmd: command_line.into_iter().map(Into::into).collect(),
            timeout: DEFAULT_HEALTH_CHECK_TIMEOUT,
            interval: DEFAULT_HEALTH_CHECK_INTERVAL,
        }
    }

    /// All checks must pass; each retains its own probe configuration.
    pub fn composite(checks: impl IntoIterator<Item = HealthCheck>) -> Self {
        Self::Composite {
            checks: checks.into_iter().collect(),
            timeout: DEFAULT_HEALTH_CHECK_TIMEOUT,
            interval: DEFAULT_HEALTH_CHECK_INTERVAL,
        }
    }

    /// Poll timeout for this check (overall budget for convergence).
    pub fn timeout(&self) -> Duration {
        match self {
            HealthCheck::Http { timeout, .. }
            | HealthCheck::Tcp { timeout, .. }
            | HealthCheck::Command { timeout, .. }
            | HealthCheck::Composite { timeout, .. } => *timeout,
            #[cfg(feature = "grpc")]
            HealthCheck::Grpc { timeout, .. } => *timeout,
        }
    }

    /// Delay between attempts while waiting for readiness.
    pub fn interval(&self) -> Duration {
        match self {
            HealthCheck::Http { interval, .. }
            | HealthCheck::Tcp { interval, .. }
            | HealthCheck::Command { interval, .. }
            | HealthCheck::Composite { interval, .. } => *interval,
            #[cfg(feature = "grpc")]
            HealthCheck::Grpc { interval, .. } => *interval,
        }
    }

    /// Replace the poll timeout; leaves interval unchanged.
    pub fn with_timeout(self, timeout: Duration) -> Self {
        match self {
            HealthCheck::Http {
                path,
                expected_status,
                interval,
                ..
            } => HealthCheck::Http {
                path,
                expected_status,
                timeout,
                interval,
            },
            #[cfg(feature = "grpc")]
            HealthCheck::Grpc {
                service, interval, ..
            } => HealthCheck::Grpc {
                service,
                timeout,
                interval,
            },
            HealthCheck::Tcp { interval, .. } => HealthCheck::Tcp { timeout, interval },
            HealthCheck::Command { cmd, interval, .. } => HealthCheck::Command {
                cmd,
                timeout,
                interval,
            },
            HealthCheck::Composite {
                checks, interval, ..
            } => HealthCheck::Composite {
                checks,
                timeout,
                interval,
            },
        }
    }

    /// Replace the poll interval; leaves timeout unchanged.
    pub fn with_interval(self, interval: Duration) -> Self {
        match self {
            HealthCheck::Http {
                path,
                expected_status,
                timeout,
                ..
            } => HealthCheck::Http {
                path,
                expected_status,
                timeout,
                interval,
            },
            #[cfg(feature = "grpc")]
            HealthCheck::Grpc {
                service, timeout, ..
            } => HealthCheck::Grpc {
                service,
                timeout,
                interval,
            },
            HealthCheck::Tcp { timeout, .. } => HealthCheck::Tcp { timeout, interval },
            HealthCheck::Command { cmd, timeout, .. } => HealthCheck::Command {
                cmd,
                timeout,
                interval,
            },
            HealthCheck::Composite {
                checks, timeout, ..
            } => HealthCheck::Composite {
                checks,
                timeout,
                interval,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_defaults() {
        // Given the http("/health") convenience constructor
        let check = HealthCheck::http("/health");
        // When we destructure the Http variant
        // Then defaults are path /health, 200, and standard timeout/interval
        match check {
            HealthCheck::Http {
                path,
                expected_status,
                timeout,
                interval,
            } => {
                assert_eq!(path, "/health");
                assert_eq!(expected_status, 200);
                assert_eq!(timeout, DEFAULT_HEALTH_CHECK_TIMEOUT);
                assert_eq!(interval, DEFAULT_HEALTH_CHECK_INTERVAL);
            }
            _ => panic!("expected Http"),
        }
    }

    #[test]
    fn http_status_defaults() {
        // Given http_status with a non-200 expected code
        let check = HealthCheck::http_status("/ready", 204);
        match check {
            HealthCheck::Http {
                path,
                expected_status,
                timeout,
                interval,
            } => {
                assert_eq!(path, "/ready");
                assert_eq!(expected_status, 204);
                assert_eq!(timeout, DEFAULT_HEALTH_CHECK_TIMEOUT);
                assert_eq!(interval, DEFAULT_HEALTH_CHECK_INTERVAL);
            }
            _ => panic!("expected Http"),
        }
    }

    #[test]
    #[cfg(feature = "grpc")]
    fn grpc_defaults() {
        // Given grpc() with no service name
        let check = HealthCheck::grpc();
        match check {
            HealthCheck::Grpc {
                service,
                timeout,
                interval,
            } => {
                assert!(service.is_none());
                assert_eq!(timeout, DEFAULT_HEALTH_CHECK_TIMEOUT);
                assert_eq!(interval, DEFAULT_HEALTH_CHECK_INTERVAL);
            }
            _ => panic!("expected Grpc"),
        }
    }

    #[test]
    #[cfg(feature = "grpc")]
    fn grpc_service_defaults() {
        // Given grpc_service with a concrete service name
        let check = HealthCheck::grpc_service("payments.v1.Payments");
        match check {
            HealthCheck::Grpc {
                service,
                timeout,
                interval,
            } => {
                assert_eq!(service.as_deref(), Some("payments.v1.Payments"));
                assert_eq!(timeout, DEFAULT_HEALTH_CHECK_TIMEOUT);
                assert_eq!(interval, DEFAULT_HEALTH_CHECK_INTERVAL);
            }
            _ => panic!("expected Grpc"),
        }
    }

    #[test]
    fn tcp_defaults() {
        // Given tcp() readiness probe
        let check = HealthCheck::tcp();
        match check {
            HealthCheck::Tcp { timeout, interval } => {
                assert_eq!(timeout, DEFAULT_HEALTH_CHECK_TIMEOUT);
                assert_eq!(interval, DEFAULT_HEALTH_CHECK_INTERVAL);
            }
            _ => panic!("expected Tcp"),
        }
    }

    #[test]
    fn command_defaults() {
        // Given command() with argv
        let check = HealthCheck::command(["/bin/sh", "-c", "true"]);
        match check {
            HealthCheck::Command {
                cmd,
                timeout,
                interval,
            } => {
                assert_eq!(cmd, vec!["/bin/sh", "-c", "true"]);
                assert_eq!(timeout, DEFAULT_HEALTH_CHECK_TIMEOUT);
                assert_eq!(interval, DEFAULT_HEALTH_CHECK_INTERVAL);
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn composite_defaults_and_nesting() {
        // Given nested checks inside composite()
        let inner_http = HealthCheck::http("/live");
        let inner_tcp = HealthCheck::tcp().with_timeout(Duration::from_secs(5));
        let composite = HealthCheck::composite([inner_http.clone(), inner_tcp.clone()]);

        match &composite {
            HealthCheck::Composite {
                checks,
                timeout,
                interval,
            } => {
                assert_eq!(checks.len(), 2);
                assert_eq!(checks[0], inner_http);
                assert_eq!(*timeout, DEFAULT_HEALTH_CHECK_TIMEOUT);
                assert_eq!(*interval, DEFAULT_HEALTH_CHECK_INTERVAL);
                match &checks[1] {
                    HealthCheck::Tcp {
                        timeout: inner_t, ..
                    } => assert_eq!(*inner_t, Duration::from_secs(5)),
                    _ => panic!("expected inner Tcp"),
                }
            }
            _ => panic!("expected Composite"),
        }
    }

    #[test]
    fn with_timeout_and_interval() {
        // Given an Http check with overridden timeout and interval
        let check = HealthCheck::http("/")
            .with_timeout(Duration::from_secs(60))
            .with_interval(Duration::from_millis(250));

        assert_eq!(check.timeout(), Duration::from_secs(60));
        assert_eq!(check.interval(), Duration::from_millis(250));
    }

    #[test]
    #[cfg(feature = "grpc")]
    fn accessors_match_variant() {
        // Given grpc_service("svc")
        let grpc = HealthCheck::grpc_service("svc");
        // Then timeout() and interval() match the variant fields
        assert_eq!(grpc.timeout(), DEFAULT_HEALTH_CHECK_TIMEOUT);
        assert_eq!(grpc.interval(), DEFAULT_HEALTH_CHECK_INTERVAL);
    }
}
