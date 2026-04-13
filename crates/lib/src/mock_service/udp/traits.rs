//! [`UdpMockServiceTrait`] — handle one inbound UDP datagram.

use std::net::SocketAddr;

use async_trait::async_trait;

use crate::mock_service::types::{MockServiceContext, MockServiceError};

/// Handle inbound UDP datagrams for a [`super::service::UdpMockService`].
#[async_trait]
pub trait UdpMockServiceTrait: Send + Sync + 'static {
    /// Process one datagram from `source`.
    ///
    /// Return [`None`] to record the request without sending a reply (for example StatsD fire-and-forget).
    async fn handle(
        &self,
        datagram: &[u8],
        source: SocketAddr,
        context: &MockServiceContext,
    ) -> Result<Option<Vec<u8>>, MockServiceError>;
}
