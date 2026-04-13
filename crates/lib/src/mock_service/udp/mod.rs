//! UDP mock services — [`UdpMockServiceTrait`] and [`UdpMockService`].
//!
//! Enable Cargo feature **`udp-mock-services`** (implies **`mock-services`**).

mod server;
mod service;
mod traits;

pub use service::{UdpMockService, UdpMockServiceRegistration};
pub use traits::UdpMockServiceTrait;

pub(crate) use server::{spawn_udp_mock_service_server, UdpMockServiceServer};
