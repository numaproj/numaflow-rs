#![warn(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use crate::shared::prost_timestamp_from_utc;
use crate::source::sourcer::source_server::Source;
use crate::source::sourcer::{
    AckRequest, AckResponse, PendingResponse, ReadRequest, ReadResponse, ReadyResponse,
};
use chrono::{DateTime, Utc};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, Request, Response, Status};

mod sourcer {
    tonic::include_proto!("source.v1");
}

struct SourceService<T> {
    handler: Arc<T>,
}

#[async_trait]
/// Sourcer trait implements [`read`], [`ack`], and [`pending`] functions for implementing user-defined source.
pub trait Sourcer {
    /// read ... FILL AFTER EXAMPLE
    async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>);
    /// Ack ... FILL AFTER EXAMPLE
    async fn ack(&self, offsets: Vec<Offset>);
    /// pending... FILL AFTER EXAMPLE
    async fn pending(&self) -> usize;
}

/// [`read`]
pub struct SourceReadRequest {
    /// count ...
    pub count: usize,
    /// timeout ...
    pub timeout: Duration,
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
        let sr = request.into_inner().request.unwrap();

        // tx.rx pair for sending data over to user-defined source
        let (stx, mut srx) = mpsc::channel::<Message>(1);
        // tx,rx pair for gRPC response
        let (tx, rx) = mpsc::channel::<Result<ReadResponse, Status>>(1);

        // start the ud-source rx asynchronously and start populating the gRPC response so it can be streamed to the gRPC client (numaflow).
        tokio::spawn(async move {
            while let Some(resp) = srx.recv().await {
                tx.send(Ok(ReadResponse {
                    result: Some(sourcer::read_response::Result {
                        payload: resp.value,
                        offset: Some(sourcer::Offset {
                            offset: resp.offset.offset,
                            partition_id: resp.offset.partition_id,
                        }),
                        event_time: prost_timestamp_from_utc(resp.event_time),
                        keys: resp.keys,
                    }),
                }))
                .await
                .unwrap();
            }
        });

        let handler_fn = Arc::clone(&self.handler);
        // we want to start streaming to the server as soon as possible
        tokio::spawn(async move {
            // user-defined source read handler
            handler_fn
                .read(
                    SourceReadRequest {
                        count: sr.num_records as usize,
                        timeout: Duration::from_millis(sr.timeout_in_ms as u64),
                    },
                    stx,
                )
                .await
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn ack_fn(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let ar: AckRequest = request.into_inner();

        let offsets = ar
            .request
            .unwrap()
            .offsets
            .into_iter()
            .map(|so| Offset {
                offset: so.offset,
                partition_id: so.partition_id,
            })
            .collect();

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

/// ...
pub struct Message {
    /// ...
    pub value: Vec<u8>,
    /// ...
    pub offset: Offset,
    /// ...
    pub event_time: DateTime<Utc>,
    /// ...
    pub keys: Vec<String>,
}
