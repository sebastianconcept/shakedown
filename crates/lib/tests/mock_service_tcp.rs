#![cfg(feature = "tcp-mock-services")]
//! TCP mock service + [`run_scenario`] integration (`tcp-mock-services` feature).

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use shakedown::{
    run_scenario, Context, EventLog, MockServiceContext, TcpCodec, TcpMockCustom, TcpMockService,
    TcpMockServiceTrait,
};

fn encode_declarative_request(method: &str, body: &[u8]) -> Vec<u8> {
    let method_bytes = method.as_bytes();
    let payload_len = 2 + method_bytes.len() + body.len();
    let mut vector = Vec::new();
    vector.extend_from_slice(&(payload_len as u32).to_be_bytes());
    vector.extend_from_slice(&(method_bytes.len() as u16).to_be_bytes());
    vector.extend_from_slice(method_bytes);
    vector.extend_from_slice(body);
    vector
}

async fn read_declarative_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.expect("read length");
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.expect("read body");
    body
}

#[tokio::test]
async fn declarative_tcp_mock_on_method_respond_and_assert() {
    let stub = TcpMockService::new("accounts")
        .on_method("getAccountData")
        .respond(b"payload-response")
        .build()
        .expect("tcp stub build");

    let mut ctx = Context::new();
    ctx.register_tcp_mock_service("accounts-tcp", stub)
        .expect("register_tcp_mock_service");

    let mut event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "declarative_tcp_mock_on_method_respond_and_assert",
        |context| {
            Box::pin(async move {
                let base = context.mock_service_url("accounts-tcp").expect("mock url");
                let address = base.strip_prefix("tcp://").expect("tcp url").to_owned();

                let mut stream = TcpStream::connect(address).await.expect("tcp connect");
                let request = encode_declarative_request("getAccountData", b"ignored-body");
                stream.write_all(&request).await.expect("write request");
                let response = read_declarative_response(&mut stream).await;
                assert_eq!(response, b"payload-response");

                let handle = context.mock_service("accounts-tcp").expect("mock handle");
                handle
                    .assert_path_called_times("getAccountData", "", 1)
                    .expect("method call count");
                handle
                    .assert_called_with("getAccountData", "", |call| {
                        call.request_body().as_ref() == b"getAccountData:ignored-body"
                    })
                    .expect("predicate");
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}

/// Length-prefixed u32 big-endian echo framing (both directions).
#[derive(Debug, Default, Clone, Copy)]
struct EchoCodec;

impl TcpCodec for EchoCodec {
    type Request = Vec<u8>;
    type Response = Vec<u8>;

    fn decode(
        &self,
        buffer: &mut BytesMut,
    ) -> Result<Option<Self::Request>, shakedown::MockServiceError> {
        if buffer.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes(buffer[..4].try_into().map_err(|_| {
            shakedown::MockServiceError::TcpCodecDecode {
                message: String::from("echo length prefix"),
            }
        })?) as usize;
        if buffer.len() < 4 + len {
            return Ok(None);
        }
        let mut frame = buffer.split_to(4 + len);
        let _ = frame.split_to(4);
        Ok(Some(frame.to_vec()))
    }

    fn encode(
        &self,
        response: Self::Response,
        buffer: &mut BytesMut,
    ) -> Result<(), shakedown::MockServiceError> {
        let len_u32 =
            u32::try_from(response.len()).map_err(|_| shakedown::MockServiceError::Encoding {
                message: String::from("echo response length overflow"),
            })?;
        buffer.put_u32(len_u32);
        buffer.put_slice(&response);
        Ok(())
    }

    fn request_method_label(_request: &Self::Request) -> String {
        String::from("echo")
    }

    fn request_bytes_for_log(request: &Self::Request) -> Bytes {
        Bytes::copy_from_slice(request)
    }

    fn response_bytes_for_log(response: &Self::Response) -> Bytes {
        Bytes::copy_from_slice(response)
    }
}

struct EchoHandler;

#[async_trait]
impl TcpMockServiceTrait for EchoHandler {
    type Codec = EchoCodec;

    async fn handle(
        &self,
        request: Vec<u8>,
        _context: &MockServiceContext,
    ) -> Result<Vec<u8>, shakedown::MockServiceError> {
        Ok(request)
    }
}

#[tokio::test]
async fn custom_tcp_codec_echo_round_trip() {
    let registration = TcpMockCustom::new(EchoCodec, EchoHandler)
        .build()
        .expect("build custom tcp mock");

    let mut ctx = Context::new();
    ctx.register_tcp_mock_service("echo-tcp", registration)
        .expect("register");

    let mut event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "custom_tcp_codec_echo_round_trip",
        |context| {
            Box::pin(async move {
                let base = context.mock_service_url("echo-tcp").expect("url");
                let address = base.strip_prefix("tcp://").expect("tcp url");
                let mut stream = TcpStream::connect(address).await.expect("connect");
                let payload = b"hello-echo";
                let mut frame = Vec::new();
                frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                frame.extend_from_slice(payload);
                stream.write_all(&frame).await.expect("write");
                let mut len_buf = [0u8; 4];
                stream.read_exact(&mut len_buf).await.expect("read len");
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut out = vec![0u8; len];
                stream.read_exact(&mut out).await.expect("read body");
                assert_eq!(out, payload);

                let handle = context.mock_service("echo-tcp").expect("handle");
                handle.assert_called_times(1).expect("one call");
                handle
                    .assert_path_called_times("echo", "", 1)
                    .expect("echo method");
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}
