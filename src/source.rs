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

mod sourcer {
    tonic::include_proto!("source.v1");
}

struct SourceService<T> {
    handler: Arc<T>,
}

#[async_trait]
/// Sourcer trait implements [`read`], [`ack`], and [`pending`] functions for implementing user-defined source.
pub trait Sourcer {
    /// read reads the messages from the source and sends them to the transmitter.
    async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>);
    /// Ack acknowledges the messages that have been processed by the user-defined source.
    async fn ack(&self, offsets: Vec<Offset>);
    /// pending returns the number of messages that are yet to be processed by the user-defined source.
    async fn pending(&self) -> usize;
}

/// SourceReadRequest is the request from the gRPC client (numaflow) to the user's [`Sourcer::read`].
pub struct SourceReadRequest {
    /// count is the number of messages to be read.
    pub count: usize,
    /// timeout is the timeout in milliseconds.
    pub timeout: Duration,
}

/// Offset is the offset of the message. When the message is acked, the offset is passed to the user's [`Sourcer::ack`].
pub struct Offset {
    /// offset is the offset in bytes.
    pub offset: Vec<u8>,
    /// partition_id is the partition_id of the message.
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
                count: pending as u64,
            }),
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

/// start_uds_server starts a gRPC server over an UDS (unix-domain-socket) endpoint.
pub async fn start_uds_server<T>(m: T) -> Result<(), Box<dyn std::error::Error>>
where
    T: Sourcer + Send + Sync + 'static,
{
    shared::write_info_file();

    let path = "/var/run/numaflow/source.sock";
    fs::create_dir_all(std::path::Path::new(path).parent().unwrap())?;
    use std::fs;
    use tokio::net::UnixListener;
    use tokio_stream::wrappers::UnixListenerStream;

    let uds = UnixListener::bind(path)?;
    let _uds_stream = UnixListenerStream::new(uds);

    let source_service = SourceService {
        handler: Arc::new(m),
    };

    Server::builder()
        .add_service(SourceServer::new(source_service))
        .serve_with_incoming(_uds_stream)
        .await?;

    Ok(())
}
