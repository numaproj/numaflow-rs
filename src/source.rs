use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, Request, Response, Status};

use crate::source::sourcer::source_server::Source;
use crate::source::sourcer::{
    AckRequest, AckResponse, PendingResponse, ReadRequest, ReadResponse, ReadyResponse,
};

mod sourcer {
    tonic::include_proto!("source.v1");
}

struct SourceService<T> {
    handler: T,
}

#[async_trait]
pub trait Sourcer {
    /// Ack ...
    async fn ack(&self, offsets: Vec<Offset>);
    /// pending...
    async fn pending(&self) -> usize;
}

/// ...
pub struct Offset {
    /// ...
    pub offset: Vec<u8>,
    /// ...
    pub partition_id: String,
}

#[async_trait]
impl<T> Source for SourceService<T>
where
    T: Sourcer + Send + Sync + 'static,
{
    type ReadFnStream = ReceiverStream<Result<ReadResponse, Status>>;

    async fn read_fn(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadFnStream>, Status> {
        _ = request;
        todo!()
    }

    async fn ack_fn(&self, _: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let offsets = vec![];
        self.handler.ack(offsets).await;

        Ok(Response::new(AckResponse {
            result: Some(sourcer::ack_response::Result { success: Some(()) }),
        }))
    }

    async fn pending_fn(&self, _: Request<()>) -> Result<Response<PendingResponse>, Status> {
        let pending = self.handler.pending().await;

        Ok(Response::new(PendingResponse {
            result: Some(sourcer::pending_response::Result {
                count: pending as u64,
            }),
        }))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<ReadyResponse>, Status> {
        Ok(Response::new(ReadyResponse { ready: true }))
    }
}
