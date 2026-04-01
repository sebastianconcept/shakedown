use std::sync::Arc;

use prost::Message;

use super::GrpcMockServiceTrait;
use super::{GrpcMockService, GrpcMockServiceRequest};
use crate::mock_service::types::{MockServiceContext, MockServiceResources};

#[derive(Clone, PartialEq, prost::Message)]
struct SampleMessage {
    #[prost(int32, tag = "1")]
    pub value: i32,
}

#[tokio::test]
async fn stub_matches_registered_method() {
    // Given a mock service that responds on `test.Service/Do` with a fixed protobuf payload
    let mock_service = GrpcMockService::new("inventory")
        .on_method("test.Service/Do")
        .respond_proto(&SampleMessage { value: 7 })
        .expect("respond_proto");

    let resources = Arc::new(MockServiceResources::default());
    let context = MockServiceContext::new("s1", "inventory", resources);
    let request = GrpcMockServiceRequest {
        service: String::from("test.Service"),
        method: String::from("Do"),
        metadata: Vec::new(),
        body: bytes::Bytes::new(),
    };
    // When the mock service handles a matching request
    let response = mock_service.handle(request, &context).await;
    // Then the status is OK and the body round-trips through prost decode (test-only)
    assert_eq!(response.status_code, tonic::Code::Ok);
    let decoded = SampleMessage::decode(response.body.as_ref())
        .expect("decode response body produced by mock service");
    assert_eq!(decoded.value, 7);
}

#[tokio::test]
async fn stub_unimplemented_when_unmatched() {
    // Given a mock service with no routes registered
    let mock_service = GrpcMockService::new("inventory");
    let resources = Arc::new(MockServiceResources::default());
    let context = MockServiceContext::new("s1", "inventory", resources);
    let request = GrpcMockServiceRequest {
        service: String::from("test.Service"),
        method: String::from("Missing"),
        metadata: Vec::new(),
        body: bytes::Bytes::new(),
    };
    // When the method does not match any route
    let response = mock_service.handle(request, &context).await;
    // Then the mock service returns UNIMPLEMENTED
    assert_eq!(response.status_code, tonic::Code::Unimplemented);
}
