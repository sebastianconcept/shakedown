//! gRPC server for unary Order → Fulfillment sync path (`book.fulfillment.v1.Fulfillment`).

use std::net::SocketAddr;

use async_trait::async_trait;
use shared_grpc::book::fulfillment::v1::fulfillment_server::{Fulfillment, FulfillmentServer};
use shared_grpc::book::fulfillment::v1::{FulfillRequest as ProtoFulfillRequest, FulfillResponse};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::fulfillment::{
    create_fulfillment, CreateFulfillmentOutcome, FulfillRequest as DomainFulfillRequest,
};
use crate::state::AppState;

struct FulfillmentGrpcService {
    state: AppState,
}

#[async_trait]
impl Fulfillment for FulfillmentGrpcService {
    async fn fulfill(
        &self,
        request: Request<ProtoFulfillRequest>,
    ) -> Result<Response<FulfillResponse>, Status> {
        let proto = request.into_inner();
        let items: Vec<serde_json::Value> = proto
            .items
            .into_iter()
            .map(|item| {
                serde_json::json!({
                    "product_id": item.product_id,
                    "quantity": item.quantity,
                })
            })
            .collect();
        let payload = DomainFulfillRequest {
            order_id: proto.order_id,
            items,
        };
        match create_fulfillment(&self.state, payload).await {
            Ok(CreateFulfillmentOutcome::Created { fulfillment_id })
            | Ok(CreateFulfillmentOutcome::AlreadyExisted { fulfillment_id }) => {
                Ok(Response::new(FulfillResponse { fulfillment_id }))
            }
            Err(err) => {
                tracing::error!(?err, "grpc fulfill failed");
                Err(Status::internal("fulfillment failed"))
            }
        }
    }
}

/// Run the gRPC server until the process shuts down (same lifetime as Axum in the book layout).
pub async fn serve(
    state: AppState,
    listen_addr: SocketAddr,
) -> Result<(), tonic::transport::Error> {
    tracing::info!(%listen_addr, "fulfillment gRPC listening");
    let svc = FulfillmentGrpcService { state };
    Server::builder()
        .serve(listen_addr, FulfillmentServer::new(svc))
        .await
}
