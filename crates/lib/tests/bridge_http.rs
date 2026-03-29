//! HTTP bridge server + [`run_scenario`] integration (`bridges` feature).

use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use shakedown::{
    run_scenario, BridgeContext, BridgeRequest, BridgeResponse, Context, EventLog, HttpBridge,
};

struct HealthBridge;

#[async_trait]
impl HttpBridge for HealthBridge {
    async fn handle(&self, request: BridgeRequest, _context: &BridgeContext) -> BridgeResponse {
        if request.method() == "GET" && request.path() == "/health" {
            BridgeResponse::ok()
        } else {
            BridgeResponse::status(404)
        }
    }
}

async fn http_get_status(host_and_port: &str, path_and_query: &str) -> u16 {
    let mut tcp = tokio::net::TcpStream::connect(host_and_port)
        .await
        .expect("tcp connect to bridge");
    let request = format!(
        "GET {path_and_query} HTTP/1.1\r\nHost: {host_and_port}\r\nConnection: close\r\n\r\n"
    );
    tcp.write_all(request.as_bytes())
        .await
        .expect("write request");
    let mut buf = vec![0u8; 1024];
    let read_length = tcp.read(&mut buf).await.expect("read response");
    let head = String::from_utf8_lossy(&buf[..read_length]);
    let status_line = head.lines().next().expect("status line");
    let parts: Vec<&str> = status_line.split_whitespace().collect();
    parts
        .get(1)
        .expect("status code")
        .parse()
        .expect("numeric status")
}

struct CountingJsonBridge {
    hits: AtomicU32,
}

#[async_trait]
impl HttpBridge for CountingJsonBridge {
    async fn handle(&self, _request: BridgeRequest, _context: &BridgeContext) -> BridgeResponse {
        let next = self.hits.fetch_add(1, Ordering::SeqCst) + 1;
        BridgeResponse::json(&serde_json::json!({ "n": next })).expect("json body")
    }
}

#[tokio::test]
async fn http_bridge_registers_starts_records_and_teardown() {
    let mut ctx = Context::new();
    ctx.register_http_bridge("payments", HealthBridge)
        .expect("register bridge");

    let mut event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "bridge_http_registers_starts_records_and_teardown",
        |context| {
            Box::pin(async move {
                let base = context.bridge_url("payments");
                let host_port = base.trim_start_matches("http://");
                let status = http_get_status(host_port, "/health").await;
                assert_eq!(status, 200u16);
                context
                    .bridge("payments")
                    .assert_path_called_times("GET", "/health", 1)
                    .expect("one GET /health");
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");

    let messages: Vec<String> = event_log
        .events()
        .map(|event| event.message.clone())
        .collect();
    assert!(
        messages
            .iter()
            .any(|message| message.contains("bridge 'payments' started on")),
        "start event missing: {messages:?}"
    );
    assert!(
        messages.iter().any(|message| {
            message.contains("bridge 'payments' stopped") && message.contains("calls recorded")
        }),
        "stop event missing: {messages:?}"
    );
}

#[tokio::test]
async fn http_bridge_stateful_handler() {
    let mut ctx = Context::new();
    ctx.register_http_bridge(
        "counter",
        CountingJsonBridge {
            hits: AtomicU32::new(0),
        },
    )
    .expect("register");

    let mut event_log = EventLog::new();
    run_scenario(
        &mut ctx,
        "http_bridge_stateful_handler",
        |context| {
            Box::pin(async move {
                let base = context.bridge_url("counter");
                let host_port = base.trim_start_matches("http://");
                let first = http_get_status(host_port, "/").await;
                let second = http_get_status(host_port, "/").await;
                assert_eq!(first, 200u16);
                assert_eq!(second, 200u16);
                let handle = context.bridge("counter");
                assert_eq!(handle.calls().len(), 2);
            })
        },
        &mut event_log,
    )
    .await
    .expect("run_scenario");
}
