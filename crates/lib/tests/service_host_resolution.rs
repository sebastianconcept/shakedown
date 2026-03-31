//! `host.docker.internal` reachability from a service-style container.
//!
//! **Docker:** this test is **`#[ignore]`** so default `cargo test` stays Docker-free.
//!
//! ```text
//! cargo test -p shakedown --features services --test service_host_resolution -- --ignored
//! ```

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use shakedown::ensure_host_docker_internal_resolution;

#[tokio::test]
#[ignore = "requires Docker; run: cargo test -p shakedown --features services --test service_host_resolution -- --ignored"]
async fn curl_container_reaches_host_http_on_host_docker_internal() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback for harness-only listener");
    let port = listener.local_addr().expect("local addr").port();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buffer = [0u8; 2048];
        let _ = socket.read(&mut buffer).await;
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
        socket.write_all(response).await.expect("write response");
    });

    let url = format!("http://host.docker.internal:{port}/");
    let request = GenericImage::new("curlimages/curl", "latest")
        .with_wait_for(WaitFor::message_on_stdout("ok"))
        .with_entrypoint("curl")
        .with_cmd(vec!["-sS".to_string(), "-f".to_string(), url]);
    let request = ensure_host_docker_internal_resolution(request);
    let container = request.start().await.expect("start curl container");
    let _ = container.stop_with_timeout(Some(10)).await;
    server.abort();
}
