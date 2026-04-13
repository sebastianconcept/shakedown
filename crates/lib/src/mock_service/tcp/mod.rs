//! TCP mock services — [`TcpCodec`], [`TcpMockServiceTrait`], and declarative [`TcpMockService`].
//!
//! Enable Cargo feature **`tcp-mock-services`** (implies **`mock-services`**).

mod codec;
mod server;
mod service;
mod traits;

pub use codec::{DeclarativeTcpCodec, DeclarativeTcpRequest, TcpCodec};
pub use server::TcpMockServiceRegistration;
pub use service::{TcpMockCustom, TcpMockService, TcpMockServiceRoutePending};
pub use tokio_rustls::TlsAcceptor;
pub use traits::TcpMockServiceTrait;

pub(crate) use server::{spawn_tcp_mock_service_server, TcpMockServiceServer};
