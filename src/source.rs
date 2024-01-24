#![warn(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use crate::shared::{self, prost_timestamp_from_utc};
use crate::source::sourcer::source_server::{Source, SourceServer};
use crate::source::sourcer::{
    AckRequest, AckResponse, PendingResponse, ReadRequest, ReadResponse, ReadyResponse,
};
use chrono::{DateTime, Utc};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status};

use self::sourcer::{partitions_response, PartitionsResponse};

mod sourcer {
    tonic::include_proto!("source.v1");
}

struct SourceService<T> {
    handler: Arc<T>,
}

#[async_trait]
/// Trait representing a [user defined source](https://numaflow.numaproj.io/user-guide/sources/overview/).
///
/// ## Example
/// Please refer to [simple source](https://github.com/numaproj/numaflow-rs/tree/main/examples/simple-source) for an example.
///
/// ## NOTE
/// The standard convention for both [`Sourcer::read`] and [`Sourcer::ack`] is that they should be mutable,
/// since they have to update some state. Unfortunately the SDK provides only a shared reference of self and thus makes it immutable. This is because
/// gRPC [tonic] provides only a shared reference for its traits. This means, the implementer for trait will have to use [SharedState] pattern to mutate
/// the values as recommended in [issue-427]. This might change in future as async traits evolves.
///
/// [user-defined source]: https://numaflow.numaproj.io/user-guide/sources/overview/
/// [tonic]: https://github.com/hyperium/tonic/
/// [SharedState]: https://tokio.rs/tokio/tutorial/shared-state
/// [issue-427]: https://github.com/hyperium/tonic/issues/427
pub trait Sourcer {
    /// Reads the messages from the source and sends them to the transmitter.
    async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>);
    /// Acknowledges the messages that have been processed by the user-defined source.
    async fn ack(&self, offsets: Vec<Offset>);
    /// Returns the number of messages that are yet to be processed by the user-defined source.
    async fn pending(&self) -> usize;
    /// Returns the partitions associated with the source. This will be used by the platform to determine
    /// the partitions to which the watermark should be published. Some sources might not have the concept of partitions.
    /// Kafka is an example of source where a reader can read from multiple partitions.
    /// If None is returned, Numaflow replica-id will be returned as the partition.
    async fn partitions(&self) -> Option<Vec<i32>>;
}

/// A request from the gRPC client (numaflow) to the user's [`Sourcer::read`].
pub struct SourceReadRequest {
    /// The number of messages to be read.
    pub count: usize,
    /// Request timeout in milliseconds.
    pub timeout: Duration,
}

/// The offset of the message.
pub struct Offset {
    /// Offset value in bytes.
    pub offset: Vec<u8>,
    /// Partition ID of the message.
    pub partition_id: i32,
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

        // tx,rx pair for sending data over to user-defined source
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

        // invoke the user-defined source's ack handler
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
        // invoke the user-defined source's pending handler
        let pending = self.handler.pending().await;

        Ok(Response::new(PendingResponse {
            result: Some(sourcer::pending_response::Result {
                count: pending as i64,
            }),
        }))
    }

    async fn partitions_fn(
        &self,
        _request: Request<()>,
    ) -> Result<Response<PartitionsResponse>, Status> {
        let partitions = match self.handler.partitions().await {
            Some(v) => v,
            None => vec![std::env::var("NUMAFLOW_REPLICA")
                .unwrap_or_default()
                .parse::<i32>()
                .unwrap_or_default()],
        };
        Ok(Response::new(PartitionsResponse {
            result: Some(partitions_response::Result { partitions }),
        }))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<ReadyResponse>, Status> {
        Ok(Response::new(ReadyResponse { ready: true }))
    }
}

/// Message is the response from the user's [`Sourcer::read`]
pub struct Message {
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Offset is the offset of the message. When the message is acked, the offset is passed to the user's [`Sourcer::ack`].
    pub offset: Offset,
    /// EventTime is the time at which the message was generated.
    pub event_time: DateTime<Utc>,
    /// Keys are the keys of the message.
    pub keys: Vec<String>,
}

/// Starts a gRPC server over an UDS (unix-domain-socket) endpoint.
pub async fn start_uds_server<T>(m: T) -> Result<(), Box<dyn std::error::Error>>
where
    T: Sourcer + Send + Sync + 'static,
{
    let listener = shared::create_listener_stream()?;
    let source_service = SourceService {
        handler: Arc::new(m),
    };

    Server::builder()
        .add_service(SourceServer::new(source_service))
        .serve_with_incoming(listener)
        .await
        .map_err(Into::into)
}
