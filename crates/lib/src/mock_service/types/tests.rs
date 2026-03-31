//! Unit tests for mock service shared types.

use bytes::Bytes;

use super::MockServiceError;

#[test]
fn mock_service_error_display() {
    let err = MockServiceError::Encoding {
        message: String::from("test"),
    };
    let msg = err.to_string();
    assert!(msg.contains("encoding failed"));
}
