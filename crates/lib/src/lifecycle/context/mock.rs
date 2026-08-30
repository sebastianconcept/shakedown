//! HTTP, gRPC, TCP, and UDP mock-service registration on [`super::Context`].
//!
//! This module is useful when a scenario registers a mock listener before the run starts.
//! This module does not bind sockets.

#[cfg(feature = "mock-services")]
use crate::error::{InternalError, ShakedownError};
#[cfg(all(feature = "mock-services", feature = "grpc"))]
use crate::mock_service::grpc::GrpcMockServiceTrait;
#[cfg(feature = "mock-services")]
use crate::mock_service::handle::MockServiceHandle;
#[cfg(feature = "mock-services")]
use crate::mock_service::http::HttpMockServiceTrait;
#[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
use crate::mock_service::tcp::TcpMockServiceRegistration;
#[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
use crate::mock_service::udp::{UdpMockService, UdpMockServiceRegistration};
#[cfg(feature = "mock-services")]
use std::sync::Arc;

use super::Context;

impl Context {
    /// Set the local IP address HTTP and gRPC mock listeners bind to (port is always ephemeral).
    ///
    /// Defaults to [`std::net::Ipv4Addr::LOCALHOST`]. Call before [`run_scenario`], for example
    /// `Ipv4Addr::UNSPECIFIED` (`0.0.0.0`) when something on the host network must dial the stub.
    #[cfg(feature = "mock-services")]
    pub fn set_mock_service_bind_ip(&mut self, ip: std::net::IpAddr) {
        self.mock_service_bind_ip = ip;
    }

    /// Register an HTTP mock service server to be started after the seed phase of [`run_scenario`].
    ///
    /// Call before [`run_scenario`]. After servers start, use [`Self::mock_service_url`] and [`Self::mock_service`].
    #[cfg(feature = "mock-services")]
    pub fn register_http_mock_service(
        &mut self,
        name: &str,
        mock_service: impl HttpMockServiceTrait,
    ) -> Result<(), ShakedownError> {
        if self.mock_service_servers_phase_complete {
            return Err(ShakedownError::Internal(InternalError::InvalidConfiguration(
                String::from(
                    "register_http_mock_service called after mock service servers were started for this scenario run",
                ),
            )));
        }
        #[cfg(all(feature = "mock-services", feature = "grpc"))]
        let grpc_name_taken = self
            .pending_grpc_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "grpc")))]
        let grpc_name_taken = false;

        #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
        let tcp_name_taken = self
            .pending_tcp_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "tcp-mock-services")))]
        let tcp_name_taken = false;

        #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
        let udp_name_taken = self
            .pending_udp_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "udp-mock-services")))]
        let udp_name_taken = false;

        if self.mock_service_handles.contains_key(name)
            || self
                .pending_http_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || grpc_name_taken
            || tcp_name_taken
            || udp_name_taken
        {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(format!(
                    "duplicate mock service name '{name}'"
                )),
            ));
        }
        self.pending_http_mock_services
            .push((name.to_owned(), Arc::new(mock_service)));
        Ok(())
    }

    /// Register a gRPC mock service server to be started after the seed phase of [`run_scenario`].
    ///
    /// Call before [`run_scenario`]. After servers start, use [`Self::mock_service_url`] and [`Self::mock_service`].
    #[cfg(all(feature = "mock-services", feature = "grpc"))]
    pub fn register_grpc_mock_service(
        &mut self,
        name: &str,
        mock_service: impl GrpcMockServiceTrait,
    ) -> Result<(), ShakedownError> {
        if self.mock_service_servers_phase_complete {
            return Err(ShakedownError::Internal(InternalError::InvalidConfiguration(
                String::from(
                    "register_grpc_mock_service called after mock service servers were started for this scenario run",
                ),
            )));
        }
        #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
        let tcp_name_taken = self
            .pending_tcp_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "tcp-mock-services")))]
        let tcp_name_taken = false;

        #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
        let udp_name_taken = self
            .pending_udp_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "udp-mock-services")))]
        let udp_name_taken = false;

        if self.mock_service_handles.contains_key(name)
            || self
                .pending_http_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || self
                .pending_grpc_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || tcp_name_taken
            || udp_name_taken
        {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(format!(
                    "duplicate mock service name '{name}'"
                )),
            ));
        }
        self.pending_grpc_mock_services
            .push((name.to_owned(), Arc::new(mock_service)));
        Ok(())
    }

    /// Register a TCP mock service to be started after the seed phase of [`run_scenario`].
    ///
    /// Call before [`run_scenario`]. After servers start, use [`Self::mock_service_url`] and [`Self::mock_service`].
    #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
    pub fn register_tcp_mock_service(
        &mut self,
        name: &str,
        registration: TcpMockServiceRegistration,
    ) -> Result<(), ShakedownError> {
        if self.mock_service_servers_phase_complete {
            return Err(ShakedownError::Internal(InternalError::InvalidConfiguration(
                String::from(
                    "register_tcp_mock_service called after mock service servers were started for this scenario run",
                ),
            )));
        }
        #[cfg(all(feature = "mock-services", feature = "grpc"))]
        let grpc_name_taken = self
            .pending_grpc_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "grpc")))]
        let grpc_name_taken = false;

        #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
        let udp_name_taken = self
            .pending_udp_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "udp-mock-services")))]
        let udp_name_taken = false;

        if self.mock_service_handles.contains_key(name)
            || self
                .pending_http_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || grpc_name_taken
            || self
                .pending_tcp_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || udp_name_taken
        {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(format!(
                    "duplicate mock service name '{name}'"
                )),
            ));
        }
        self.pending_tcp_mock_services
            .push((name.to_owned(), registration));
        Ok(())
    }

    /// Register a UDP mock service to be started after the seed phase of [`run_scenario`].
    ///
    /// Call before [`run_scenario`]. After servers start, use [`Self::mock_service_url`] and [`Self::mock_service`].
    #[cfg(all(feature = "mock-services", feature = "udp-mock-services"))]
    pub fn register_udp_mock_service(
        &mut self,
        name: &str,
        mock_service: UdpMockService,
    ) -> Result<(), ShakedownError> {
        if self.mock_service_servers_phase_complete {
            return Err(ShakedownError::Internal(InternalError::InvalidConfiguration(
                String::from(
                    "register_udp_mock_service called after mock service servers were started for this scenario run",
                ),
            )));
        }
        #[cfg(all(feature = "mock-services", feature = "grpc"))]
        let grpc_name_taken = self
            .pending_grpc_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "grpc")))]
        let grpc_name_taken = false;

        #[cfg(all(feature = "mock-services", feature = "tcp-mock-services"))]
        let tcp_name_taken = self
            .pending_tcp_mock_services
            .iter()
            .any(|(registered_name, _)| registered_name == name);
        #[cfg(not(all(feature = "mock-services", feature = "tcp-mock-services")))]
        let tcp_name_taken = false;

        if self.mock_service_handles.contains_key(name)
            || self
                .pending_http_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
            || grpc_name_taken
            || tcp_name_taken
            || self
                .pending_udp_mock_services
                .iter()
                .any(|(registered_name, _)| registered_name == name)
        {
            return Err(ShakedownError::Internal(
                InternalError::InvalidConfiguration(format!(
                    "duplicate mock service name '{name}'"
                )),
            ));
        }
        self.pending_udp_mock_services
            .push((name.to_owned(), mock_service.into_registration()));
        Ok(())
    }

    /// Test-only: control [`Self::register_http_mock_service`] / [`Self::register_grpc_mock_service`] phase guards.
    #[cfg(all(test, feature = "mock-services"))]
    pub(crate) fn set_mock_service_servers_phase_complete_for_test(&mut self, complete: bool) {
        self.mock_service_servers_phase_complete = complete;
    }

    /// Client-facing base URL for a registered mock service (HTTP or gRPC).
    ///
    /// Returns [`None`] if no mock service was registered under `name` or mock service servers have not
    /// started yet for this run (e.g. called before [`run_scenario`] has bound listeners).
    #[cfg(feature = "mock-services")]
    pub fn mock_service_url(&self, name: &str) -> Option<String> {
        self.mock_service_handles
            .get(name)
            .map(|handle| handle.client_url.clone())
    }

    /// Handle for assertions on recorded mock service traffic (HTTP or gRPC).
    ///
    /// Returns [`None`] if no mock service was registered under `name` or mock service servers have not
    /// started yet for this run.
    #[cfg(feature = "mock-services")]
    pub fn mock_service(&self, name: &str) -> Option<&MockServiceHandle> {
        self.mock_service_handles.get(name).map(|arc| arc.as_ref())
    }
}
