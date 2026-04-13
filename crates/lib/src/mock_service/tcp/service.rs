//! Declarative [`TcpMockService`] builder (method table) and custom codec/handler registration.

use std::sync::Arc;

use bytes::Bytes;
use tokio_rustls::TlsAcceptor;

use crate::mock_service::types::MockServiceError;

use super::codec::{DeclarativeTcpCodec, DeclarativeTcpRequest, TcpCodec};
use super::server::{TcpMockServiceConnection, TcpMockServiceRegistration, TcpMockServiceSession};
use super::traits::TcpMockServiceTrait;

/// Fluent step after [`TcpMockService::on_method`]: attach a response body for that method.
pub struct TcpMockServiceRoutePending {
    mock_service: TcpMockService,
    method_name: String,
}

/// Declarative TCP mock: route by logical method name (same idea as HTTP mock path routes).
///
/// Wire format is defined by [`super::codec::DeclarativeTcpCodec`].
pub struct TcpMockService {
    /// Logical service label (for diagnostics; the harness name comes from registration).
    #[allow(dead_code)]
    name: String,
    routes: Vec<(String, Bytes)>,
    default_response: Bytes,
    tls: Option<Arc<TlsAcceptor>>,
}

impl TcpMockService {
    /// Start a declarative stub with no routes (only the default response until routes are added).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            routes: Vec::new(),
            default_response: Bytes::new(),
            tls: None,
        }
    }

    /// Optional TLS termination on the TCP listener (same as [`TcpMockCustom::with_tls`]).
    pub fn with_tls(mut self, acceptor: Arc<TlsAcceptor>) -> Self {
        self.tls = Some(acceptor);
        self
    }

    /// Register a route: `method_name` must match the decoded [`DeclarativeTcpRequest::method`].
    pub fn on_method(self, method_name: &str) -> TcpMockServiceRoutePending {
        TcpMockServiceRoutePending {
            mock_service: self,
            method_name: method_name.to_owned(),
        }
    }

    /// Response when no route matches.
    pub fn default_response(mut self, payload: impl Into<Vec<u8>>) -> Self {
        self.default_response = Bytes::from(payload.into());
        self
    }

    /// Finish the builder and produce a registration for [`crate::lifecycle::Context::register_tcp_mock_service`].
    pub fn build(self) -> Result<TcpMockServiceRegistration, MockServiceError> {
        let handler = Arc::new(DeclarativeTcpStub {
            routes: self.routes,
            default_response: self.default_response,
        });
        let session: Arc<TcpMockServiceSession<DeclarativeTcpCodec, DeclarativeTcpStub>> =
            Arc::new(TcpMockServiceSession {
                codec: Arc::new(DeclarativeTcpCodec),
                handler,
            });
        Ok(TcpMockServiceRegistration {
            connection: session as Arc<dyn TcpMockServiceConnection>,
            tls: self.tls,
        })
    }
}

impl TcpMockServiceRoutePending {
    /// Fixed response body for this method (declarative stub).
    pub fn respond(mut self, payload: impl Into<Vec<u8>>) -> TcpMockService {
        self.mock_service
            .routes
            .push((self.method_name, Bytes::from(payload.into())));
        self.mock_service
    }
}

pub(crate) struct DeclarativeTcpStub {
    pub(crate) routes: Vec<(String, Bytes)>,
    pub(crate) default_response: Bytes,
}

#[async_trait::async_trait]
impl TcpMockServiceTrait for DeclarativeTcpStub {
    type Codec = DeclarativeTcpCodec;

    async fn handle(
        &self,
        request: DeclarativeTcpRequest,
        _context: &crate::mock_service::types::MockServiceContext,
    ) -> Result<Bytes, MockServiceError> {
        for (method, response) in &self.routes {
            if method == &request.method {
                return Ok(response.clone());
            }
        }
        Ok(self.default_response.clone())
    }
}

/// Custom codec + handler (for example a length-prefixed echo protocol).
pub struct TcpMockCustom<C, H> {
    codec: Arc<C>,
    handler: Arc<H>,
    tls: Option<Arc<TlsAcceptor>>,
}

impl<C, H> TcpMockCustom<C, H>
where
    C: TcpCodec + Send + Sync + 'static,
    H: TcpMockServiceTrait<Codec = C> + Send + Sync + 'static,
{
    /// Wrap a codec instance and handler.
    pub fn new(codec: C, handler: H) -> Self {
        Self {
            codec: Arc::new(codec),
            handler: Arc::new(handler),
            tls: None,
        }
    }

    /// Optional TLS termination (KRPC-style TLS over TCP).
    pub fn with_tls(mut self, acceptor: Arc<TlsAcceptor>) -> Self {
        self.tls = Some(acceptor);
        self
    }

    /// Build a registration for [`crate::lifecycle::Context::register_tcp_mock_service`].
    pub fn build(self) -> Result<TcpMockServiceRegistration, MockServiceError> {
        let session: Arc<TcpMockServiceSession<C, H>> = Arc::new(TcpMockServiceSession {
            codec: self.codec,
            handler: self.handler,
        });
        Ok(TcpMockServiceRegistration {
            connection: session as Arc<dyn TcpMockServiceConnection>,
            tls: self.tls,
        })
    }
}
