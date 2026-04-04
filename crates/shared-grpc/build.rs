fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let protos = [
        "proto/book/fulfillment/v1/fulfillment.proto",
        "proto/book/demo/v1/demo.proto",
    ];
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&protos, &["proto"])?;
    Ok(())
}
