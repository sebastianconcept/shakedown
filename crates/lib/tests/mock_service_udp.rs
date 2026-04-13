#![cfg(feature = "udp-mock-services")]
//! UDP mock service + [`run_scenario`] integration (`udp-mock-services` feature).

use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::net::UdpSocket;

use shakedown::{
    run_scenario, Context, EventLog, MockServiceContext, UdpMockService, UdpMockServiceTrait,
};

struct EchoUdp;

#[async_trait]
impl UdpMockServiceTrait for EchoUdp {
    async fn handle(
        &self,
        datagram: &[u8],
        _source: SocketAddr,
        _context: &MockServiceContext,
    ) -> Result<Option<Vec<u8>>, shakedown::MockServiceError> {
        Ok(Some(datagram.to_vec()))
    }
}

#[tokio::test]
async fn udp_mock_echo_round_trip_and_assert() {
    let mut ctx = Context::new();
    ctx.register_udp_mock_service("statsd-udp", UdpMockService::new(EchoUdp))
        .expect("register_udp_mock_service");

    let mut event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "udp_mock_echo_round_trip_and_assert",
        |context| {
            Box::pin(async move {
                let base = context.mock_service_url("statsd-udp").expect("mock url");
                let address = base.strip_prefix("udp://").expect("udp url");
                let server_addr: SocketAddr = address.parse().expect("parse addr");

                let client = UdpSocket::bind("127.0.0.1:0").await.expect("client bind");
                let payload = b"metric.name:1|c";
                client.send_to(payload, server_addr).await.expect("send_to");

                let mut response_buffer = [0u8; 256];
                let (response_length, _) = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    client.recv_from(&mut response_buffer),
                )
                .await
                .expect("timeout")
                .expect("recv_from");
                assert_eq!(&response_buffer[..response_length], payload);

                let handle = context.mock_service("statsd-udp").expect("handle");
                handle.assert_called_times(1).expect("one datagram");
                handle
                    .assert_path_called_times("datagram", "", 1)
                    .expect("datagram method");
                handle
                    .assert_called_with("datagram", "", |call| {
                        call.request_body().as_ref() == payload && call.response_status() == 200
                    })
                    .expect("predicate");
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}
