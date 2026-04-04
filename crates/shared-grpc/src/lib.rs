//! Generated [`tonic`] + [`prost`] types shared by book example services.
//!
//! - **`book.fulfillment.v1`** — Order → Fulfillment unary sync (`Fulfill`).
//! - **`book.demo.v1`** — minimal extra RPC (`Ping`) showing the crate holds **multiple**
//!   contracts; new services can depend on this crate without renaming it when you add APIs.

pub mod book {
    pub mod fulfillment {
        pub mod v1 {
            tonic::include_proto!("book.fulfillment.v1");
        }
    }
    pub mod demo {
        pub mod v1 {
            tonic::include_proto!("book.demo.v1");
        }
    }
}

pub use book::demo::v1::demo_client::DemoClient;
pub use book::demo::v1::demo_server::{Demo, DemoServer};
pub use book::demo::v1::{PingRequest, PongResponse};
pub use book::fulfillment::v1::fulfillment_client::FulfillmentClient;
pub use book::fulfillment::v1::fulfillment_server::{Fulfillment, FulfillmentServer};
pub use book::fulfillment::v1::{FulfillRequest, FulfillResponse, OrderItem};

/// Full gRPC method path for mocks and assertions (`/package.Service/Method`).
pub const FULFILL_METHOD_PATH: &str = "/book.fulfillment.v1.Fulfillment/Fulfill";

/// Example second path (e.g. `GrpcMockService::on_method` in tests).
pub const DEMO_PING_METHOD_PATH: &str = "/book.demo.v1.Demo/Ping";

#[cfg(test)]
mod tests {
    use super::{PingRequest, PongResponse};
    use prost::Message;

    #[test]
    fn demo_messages_round_trip() {
        let req = PingRequest {
            message: "hello".to_string(),
        };
        let mut buf = Vec::new();
        req.encode(&mut buf).expect("encode");
        let decoded = PingRequest::decode(buf.as_slice()).expect("decode");
        assert_eq!(decoded.message, "hello");

        let res = PongResponse {
            echo: "hello".to_string(),
        };
        let mut buf = Vec::new();
        res.encode(&mut buf).expect("encode");
        let decoded = PongResponse::decode(buf.as_slice()).expect("decode");
        assert_eq!(decoded.echo, "hello");
    }
}
