//! [`TcpMockServiceTrait`] — handle one decoded TCP request.

use async_trait::async_trait;

use crate::mock_service::types::{MockServiceContext, MockServiceError};

use super::codec::TcpCodec;

/// Application handler for a TCP mock service (paired with a [`TcpCodec`]).
#[async_trait]
pub trait TcpMockServiceTrait: Send + Sync + 'static {
    /// Codec implementation used to frame bytes for this handler.
    type Codec: TcpCodec;

    /// Handle one decoded request and produce a response (encoded by the harness).
    async fn handle(
        &self,
        request: <Self::Codec as TcpCodec>::Request,
        context: &MockServiceContext,
    ) -> Result<<Self::Codec as TcpCodec>::Response, MockServiceError>;
}
