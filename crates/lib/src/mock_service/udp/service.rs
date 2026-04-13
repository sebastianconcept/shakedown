//! [`UdpMockService`] — register a [`UdpMockServiceTrait`] with the harness.

use std::sync::Arc;

use super::traits::UdpMockServiceTrait;

/// Registration payload for [`crate::lifecycle::Context::register_udp_mock_service`].
pub struct UdpMockServiceRegistration {
    pub(crate) handler: Arc<dyn UdpMockServiceTrait>,
}

/// UDP mock: binds an ephemeral port and dispatches datagrams to your handler.
pub struct UdpMockService {
    handler: Arc<dyn UdpMockServiceTrait>,
}

impl UdpMockService {
    /// Wrap a [`UdpMockServiceTrait`] implementation.
    pub fn new(handler: impl UdpMockServiceTrait) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }

    pub(crate) fn into_registration(self) -> UdpMockServiceRegistration {
        UdpMockServiceRegistration {
            handler: self.handler,
        }
    }
}
