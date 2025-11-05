use chrono::{DateTime, Utc};
use std::collections::HashMap;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming, async_trait};
use tracing::{error, info};

use crate::error::{Error, ErrorKind};
use crate::proto::metadata as metadata_pb;
use crate::proto::source as proto;
use crate::proto::source::{AckRequest, AckResponse, ReadRequest, ReadResponse};
use crate::shared;
use shared::{ContainerType, prost_timestamp_from_utc};

/// Default socket address for source service
pub const SOCK_ADDR: &str = "/var/run/numaflow/source.sock";

/// Default server info file for source service
pub const SERVER_INFO_FILE: &str = "/var/run/numaflow/sourcer-server-info";

// TODO: use batch-size, blocked by https://github.com/numaproj/numaflow/issues/2026
/// Default channel size for source service
const CHANNEL_SIZE: usize = 1000;

struct SourceService<T> {
    handler: Arc<T>,
    shutdown_tx: Sender<()>,
    cancellation_token: CancellationToken,
}

// FIXME: remove async_trait
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
    /// Acknowledges the message that has been processed by the user-defined source.
    async fn ack(&self, offset: Vec<Offset>);
    /// Negatively acknowledges the message that has been processed by the user-defined source.
    async fn nack(&self, offset: Vec<Offset>);
    /// Returns the number of messages that are yet to be processed by the user-defined source.
    /// The None value can be returned if source doesn't support detecting the backlog.
    async fn pending(&self) -> Option<usize>;
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

/// Metadata provides per-message metadata passed between vertices.
/// Source is the origin or the first vertex in the pipeline.
/// Here, for the first time, the user metadata can be set by the user.
/// A vertex could create one or more set of key-value pairs per group-name.
/// UserMetadata wraps user-defined metadata groups per message.
#[derive(Debug, Clone, Default)]
pub struct UserMetadata {
    data: HashMap<String, HashMap<String, Vec<u8>>>,
}

impl UserMetadata {
    /// Create a new UserMetadata instance
    pub fn new() -> Self {
        Self::default()
    }

    /// groups returns the groups of the user metadata.
    /// If there are no groups, it returns an empty vector.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use numaflow::source::UserMetadata;
    /// let mut umd = UserMetadata::new();
    /// umd.add_kv("group1".to_string(), "key1".to_string(), "value1".as_bytes().to_vec());
    /// umd.add_kv("group2".to_string(), "key2".to_string(), "value2".as_bytes().to_vec());
    /// let groups = umd.groups();
    /// println!("{:?}", groups);
    /// ```
    pub fn groups(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    /// keys returns the keys of the user metadata for the given group.
    /// If there are no keys or the group is not present, it returns an empty vector.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use numaflow::source::UserMetadata;
    /// let mut umd = UserMetadata::new();
    /// umd.add_kv("group1".to_string(), "key1".to_string(), "value1".as_bytes().to_vec());
    /// umd.add_kv("group1".to_string(), "key2".to_string(), "value2".as_bytes().to_vec());
    /// let keys = umd.keys("group1");
    /// println!("{:?}", keys);
    /// ```
    pub fn keys(&self, group: &str) -> Vec<String> {
        self.data
            .get(group)
            .map(|kv| kv.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// value returns the value of the user metadata for the given group and key.
    /// If there is no value or the group or key is not present, it returns an empty vector.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use numaflow::source::UserMetadata;
    /// let mut umd = UserMetadata::new();
    /// umd.add_kv("group1".to_string(), "key1".to_string(), "value1".as_bytes().to_vec());
    /// let value = umd.value("group1", "key1");
    /// println!("{:?}", value);
    /// ```
    pub fn value(&self, group: &str, key: &str) -> Vec<u8> {
        self.data
            .get(group)
            .and_then(|kv| kv.get(key))
            .cloned()
            .unwrap_or_default()
    }

    /// create_group creates a new group in the user metadata.
    /// If the group is already present, it's a no-op.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use numaflow::source::UserMetadata;
    /// use std::collections::HashMap;
    /// let mut umd = UserMetadata::new();
    /// umd.create_group("group1".to_string());
    /// println!("{:?}", umd);
    /// ```
    pub fn create_group(&mut self, group: String) {
        self.data.entry(group).or_default();
    }

    /// add_kv adds a key-value pair to the user metadata.
    /// If the group is not present, it creates a new group.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use numaflow::source::UserMetadata;
    /// let mut umd = UserMetadata::new();
    /// umd.add_kv("group1".to_string(), "key1".to_string(), "value1".as_bytes().to_vec());
    /// println!("{:?}", umd);
    /// ```
    pub fn add_kv(&mut self, group: String, key: String, value: Vec<u8>) {
        self.data.entry(group).or_default().insert(key, value);
    }

    /// remove_key removes a key from a group in the user metadata.
    /// If the key or group is not present, it's a no-op.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use numaflow::source::UserMetadata;
    /// let mut umd = UserMetadata::new();
    /// umd.add_kv("group1".to_string(), "key1".to_string(), "value1".as_bytes().to_vec());
    /// umd.remove_key("group1", "key1");
    /// println!("{:?}", umd);
    /// ```
    pub fn remove_key(&mut self, group: &str, key: &str) {
        if let Some(kv) = self.data.get_mut(group) {
            kv.remove(key);
        }
    }

    /// remove_group removes a group from the user metadata.
    /// If the group is not present, it's a no-op.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use numaflow::source::UserMetadata;
    /// let mut umd = UserMetadata::new();
    /// umd.add_kv("group1".to_string(), "key1".to_string(), "value1".as_bytes().to_vec());
    /// umd.remove_group("group1");
    /// println!("{:?}", umd);
    /// ```
    pub fn remove_group(&mut self, group: &str) {
        self.data.remove(group);
    }
}

/// The offset of the message.
pub struct Offset {
    /// Offset value in bytes.
    pub offset: Vec<u8>,
    /// Partition ID of the message.
    pub partition_id: i32,
}

/// Converts Option<&UserMetadata> to proto Metadata.
/// SDKs should always return non-nil metadata.
/// If user metadata is None or empty, it returns a metadata with empty user_metadata map.
fn to_proto(user_metadata: Option<&UserMetadata>) -> metadata_pb::Metadata {
    let mut user = HashMap::new();

    if let Some(umd) = user_metadata {
        for group in umd.groups() {
            let mut kv = HashMap::new();
            for key in umd.keys(&group) {
                kv.insert(key.clone(), umd.value(&group, &key));
            }
            user.insert(group, metadata_pb::KeyValueGroup { key_value: kv });
        }
    }

    metadata_pb::Metadata {
        previous_vertex: String::new(),
        sys_metadata: HashMap::new(),
        user_metadata: user,
    }
}

impl<T> SourceService<T>
where
    T: Sourcer + Send + Sync + 'static,
{
    /// writes a read batch returned by the user-defined handler to the client (numaflow).
    async fn write_a_batch(
        grpc_resp_tx: Sender<Result<ReadResponse, Status>>,
        mut udsource_rx: Receiver<Message>,
    ) -> crate::error::Result<()> {
        // even though we use bi-di; the user-defined source sees this as a 1/2 duplex
        // server side streaming. this means that the below while loop will terminate
        // after every batch of read has been returned.
        while let Some(resp) = udsource_rx.recv().await {
            grpc_resp_tx
                .send(Ok(ReadResponse {
                    result: Some(proto::read_response::Result {
                        payload: resp.value,
                        offset: Some(proto::Offset {
                            offset: resp.offset.offset,
                            partition_id: resp.offset.partition_id,
                        }),
                        event_time: prost_timestamp_from_utc(resp.event_time),
                        keys: resp.keys,
                        headers: resp.headers,
                        metadata: Some(to_proto(resp.user_metadata.as_ref())),
                    }),
                    status: None,
                    handshake: None,
                }))
                .await
                .map_err(|e| Error::SourceError(ErrorKind::InternalError(e.to_string())))?;
        }

        // send end of transmission on success
        grpc_resp_tx
            .send(Ok(ReadResponse {
                result: None,
                status: Some(proto::read_response::Status {
                    eot: true,
                    code: 0,
                    error: None,
                    msg: None,
                }),
                handshake: None,
            }))
            .await
            .map_err(|e| Error::SourceError(ErrorKind::InternalError(e.to_string())))?;

        Ok(())
    }

    /// Invokes the user-defined source handler to get a read batch and streams it to the numaflow
    /// (client).
    async fn forward_a_batch(
        handler_fn: Arc<T>,
        grpc_resp_tx: Sender<Result<ReadResponse, Status>>,
        request: proto::read_request::Request,
    ) -> crate::error::Result<()> {
        // tx,rx pair for sending data over to user-defined source
        let (stx, srx) = mpsc::channel::<Message>(CHANNEL_SIZE);

        // spawn the rx side so that when the handler is invoked, we can stream the handler's read data
        // to the grpc response stream.
        let grpc_writer_handle: JoinHandle<Result<(), Error>> =
            tokio::spawn(async move { Self::write_a_batch(grpc_resp_tx, srx).await });

        // spawn the handler, it will stream the data to tx passed which will be streamed to the client
        // by the above task.
        handler_fn
            .read(
                SourceReadRequest {
                    count: request.num_records as usize,
                    timeout: Duration::from_millis(request.timeout_in_ms as u64),
                },
                stx,
            )
            .await;

        // wait for the spawned grpc writer to end
        let _ = grpc_writer_handle
            .await
            .map_err(|e| Error::SourceError(ErrorKind::InternalError(e.to_string())))?;

        Ok(())
    }
}

#[async_trait]
impl<T> proto::source_server::Source for SourceService<T>
where
    T: Sourcer + Send + Sync + 'static,
{
    type ReadFnStream = ReceiverStream<Result<ReadResponse, Status>>;
    async fn read_fn(
        &self,
        request: Request<Streaming<ReadRequest>>,
    ) -> Result<Response<Self::ReadFnStream>, Status> {
        let mut req_stream = request.into_inner();
        // we have to call the handler over and over for each ReadRequest
        let handler_fn = Arc::clone(&self.handler);

        // tx (read from client), rx (write to client) pair for gRPC response
        let (tx, rx) = mpsc::channel::<Result<ReadResponse, Status>>(CHANNEL_SIZE);

        // this _tx ends up writing to the client side
        let grpc_tx = tx.clone();

        let cln_token = self.cancellation_token.clone();

        // do the handshake first to let the client know that we are ready to receive read requests.
        self.perform_read_handshake(&mut req_stream, &grpc_tx)
            .await?;

        // this is the top-level stream consumer and this task will only exit when stream is closed (which
        // will happen when server and client are shutting down).
        let grpc_read_handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // for each ReadRequest message, the handler will be called and a batch of messages
                    // will be sent over to the client.
                    read_request = req_stream.message() => {
                        let read_request = read_request
                            .map_err(|e| Error::SourceError(ErrorKind::InternalError(e.to_string())))?
                            .ok_or_else(|| Error::SourceError(ErrorKind::InternalError("Stream closed".to_string())))?;

                        let request = read_request.request.ok_or_else(|| Error::SourceError(ErrorKind::InternalError("Stream closed".to_string())))?;

                        // start the ud-source rx asynchronously and start populating the gRPC
                        // response, so it can be streamed to the gRPC client (numaflow).
                        let grpc_resp_tx = grpc_tx.clone();

                        // let's forward a batch for this request
                        Self::forward_a_batch(handler_fn.clone(), grpc_resp_tx, request).await?
                    }
                    _ = cln_token.cancelled() => {
                        info!("Cancellation token triggered, shutting down");
                        break;
                    }
                }
            }
            Ok(())
        });

        let shutdown_tx = self.shutdown_tx.clone();
        // spawn so we can return the recv stream to client.
        tokio::spawn(async move {
            // wait for the grpc read handle; if there are any errors, we set the gRPC Status to failure
            // which will close the stream with failure.
            if let Err(e) = grpc_read_handle.await {
                error!("shutting down the gRPC channel, {}", e);
                tx.send(Err(Status::internal(e.to_string())))
                    .await
                    .map_err(|e| Error::SourceError(ErrorKind::InternalError(e.to_string())))
                    .expect("writing error to grpc response channel should never fail");

                // if there are any failures, we propagate those failures so that the server can shut down.
                shutdown_tx
                    .send(())
                    .await
                    .expect("write to shutdown channel should never fail");
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type AckFnStream = ReceiverStream<Result<AckResponse, Status>>;

    async fn ack_fn(
        &self,
        request: Request<Streaming<AckRequest>>,
    ) -> Result<Response<Self::AckFnStream>, Status> {
        let mut ack_stream = request.into_inner();
        let (ack_tx, ack_rx) = mpsc::channel::<Result<AckResponse, Status>>(CHANNEL_SIZE);

        let handler_fn = Arc::clone(&self.handler);

        // do the handshake first to let the client know that we are ready to receive ack requests.
        self.perform_ack_handshake(&mut ack_stream, &ack_tx).await?;

        let ack_resp_tx = ack_tx.clone();
        let cln_token = self.cancellation_token.clone();
        let grpc_read_handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cln_token.cancelled() => {
                        info!("Cancellation token triggered, shutting down");
                        break;
                    }
                    ack_request = ack_stream.message() => {
                        let ack_request = ack_request
                            .map_err(|e| Error::SourceError(ErrorKind::InternalError(e.to_string())))?
                            .ok_or_else(|| Error::SourceError(ErrorKind::InternalError("Stream closed".to_string())))?;

                        let request = ack_request.request
                            .ok_or_else(|| Error::SourceError(ErrorKind::InternalError("Invalid request, request can't be empty".to_string())))?;

                        let offsets: Vec<Offset> = request.offsets.into_iter().map(|offset| offset.into()).collect();

                        handler_fn
                            .ack(offsets)
                            .await;

                        // the return of handler_fn implicitly means that the ack is successful; hence
                        // we are able to send success. There is no path for failure.
                        ack_resp_tx
                            .send(Ok(AckResponse {
                                result: Some(proto::ack_response::Result { success: Some(()) }),
                                handshake: None,
                            }))
                            .await
                            .map_err(|e| Error::SourceError(ErrorKind::InternalError(e.to_string())))?;
                    }
                }
            }
            Ok(())
        });

        let shutdown_tx = self.shutdown_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = grpc_read_handle.await {
                error!("shutting down the gRPC ack channel, {}", e);
                ack_tx
                    .send(Err(Status::internal(e.to_string())))
                    .await
                    .map_err(|e| Error::SourceError(ErrorKind::InternalError(e.to_string())))
                    .expect("writing error to grpc response channel should never fail");

                shutdown_tx
                    .send(())
                    .await
                    .expect("write to shutdown channel should never fail");
            }
        });

        Ok(Response::new(ReceiverStream::new(ack_rx)))
    }

    /// negatively acknowledge the offsets
    async fn nack_fn(
        &self,
        request: Request<proto::NackRequest>,
    ) -> Result<Response<proto::NackResponse>, Status> {
        let request = request.into_inner().request.ok_or_else(|| {
            Status::invalid_argument("Invalid request, request can't be empty".to_string())
        })?;

        let offsets: Vec<Offset> = request
            .offsets
            .into_iter()
            .map(|offset| offset.into())
            .collect();

        self.handler.nack(offsets).await;
        Ok(Response::new(proto::NackResponse {
            result: Some(proto::nack_response::Result { success: Some(()) }),
        }))
    }

    async fn pending_fn(&self, _: Request<()>) -> Result<Response<proto::PendingResponse>, Status> {
        // invoke the user-defined source's pending handler
        let pending = match self.handler.pending().await {
            None => -1,
            Some(val) => i64::try_from(val).unwrap_or(i64::MAX),
        };

        Ok(Response::new(proto::PendingResponse {
            result: Some(proto::pending_response::Result { count: pending }),
        }))
    }

    async fn partitions_fn(
        &self,
        _request: Request<()>,
    ) -> Result<Response<proto::PartitionsResponse>, Status> {
        let partitions = self.handler.partitions().await.unwrap_or_else(|| {
            vec![
                std::env::var("NUMAFLOW_REPLICA")
                    .unwrap_or_default()
                    .parse::<i32>()
                    .unwrap_or_default(),
            ]
        });
        Ok(Response::new(proto::PartitionsResponse {
            result: Some(proto::partitions_response::Result { partitions }),
        }))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}

impl From<proto::Offset> for Offset {
    fn from(offset: proto::Offset) -> Self {
        Self {
            offset: offset.offset,
            partition_id: offset.partition_id,
        }
    }
}

impl<T> SourceService<T>
where
    T: Sourcer + Send + Sync + 'static,
{
    // performs the read handshake with the client
    async fn perform_read_handshake(
        &self,
        read_stream: &mut Streaming<ReadRequest>,
        resp_tx: &Sender<Result<ReadResponse, Status>>,
    ) -> Result<(), Status> {
        let handshake_request = read_stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("read handshake failed {}", e)))?
            .ok_or_else(|| Status::internal("read stream closed before handshake"))?;

        if let Some(handshake) = handshake_request.handshake {
            resp_tx
                .send(Ok(ReadResponse {
                    result: None,
                    status: None,
                    handshake: Some(handshake),
                }))
                .await
                .map_err(|e| {
                    Status::internal(format!("failed to send read handshake response {}", e))
                })?;
            Ok(())
        } else {
            Err(Status::invalid_argument("Read handshake not present"))
        }
    }

    // performs the ack handshake with the client
    async fn perform_ack_handshake(
        &self,
        ack_stream: &mut Streaming<AckRequest>,
        resp_tx: &Sender<Result<AckResponse, Status>>,
    ) -> Result<(), Status> {
        let handshake_request = ack_stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("ack handshake failed {}", e)))?
            .ok_or_else(|| Status::internal("ack stream closed before handshake"))?;

        if let Some(handshake) = handshake_request.handshake {
            resp_tx
                .send(Ok(AckResponse {
                    result: None,
                    handshake: Some(handshake),
                }))
                .await
                .map_err(|e| {
                    Status::internal(format!("failed to send ack handshake response {}", e))
                })?;
            Ok(())
        } else {
            Err(Status::invalid_argument("Ack handshake not present"))
        }
    }
}

/// Message is the response from the user's [`Sourcer::read`]
pub struct Message {
    /// The value passed to the next vertex.
    pub value: Vec<u8>,
    /// Offset of the message. When the message is acked, the offset is passed to the user's [`Sourcer::ack`].
    pub offset: Offset,
    /// The time at which the message was generated.
    pub event_time: DateTime<Utc>,
    /// Keys of the message.
    pub keys: Vec<String>,
    /// Headers of the message.
    pub headers: HashMap<String, String>,
    /// User Metadata of the message.
    pub user_metadata: Option<UserMetadata>,
}

/// gRPC server for starting a [`Sourcer`] service
#[derive(Debug)]
pub struct Server<T> {
    inner: shared::Server<T>,
}

impl<T> shared::ServerExtras<T> for Server<T> {
    fn transform_inner<F>(self, f: F) -> Self
    where
        F: FnOnce(shared::Server<T>) -> shared::Server<T>,
    {
        Self {
            inner: f(self.inner),
        }
    }

    fn inner_ref(&self) -> &shared::Server<T> {
        &self.inner
    }
}

impl<T> Server<T> {
    /// Creates a new gRPC `Server` instance
    pub fn new(source_svc: T) -> Self {
        Self {
            inner: shared::Server::new(
                source_svc,
                ContainerType::Source,
                SOCK_ADDR,
                SERVER_INFO_FILE,
            ),
        }
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown(
        self,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Sourcer + Send + Sync + 'static,
    {
        self.inner
            .start_with_shutdown(
                shutdown_rx,
                |handler, max_message_size, shutdown_tx, cln_token| {
                    let source_service = SourceService {
                        handler: Arc::new(handler),
                        shutdown_tx,
                        cancellation_token: cln_token,
                    };

                    let source_svc = proto::source_server::SourceServer::new(source_service)
                        .max_decoding_message_size(max_message_size)
                        .max_encoding_message_size(max_message_size);

                    tonic::transport::Server::builder().add_service(source_svc)
                },
            )
            .await
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the signals arrives.
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Sourcer + Send + Sync + 'static,
    {
        self.inner
            .start(|handler, max_message_size, shutdown_tx, cln_token| {
                let source_service = SourceService {
                    handler: Arc::new(handler),
                    shutdown_tx,
                    cancellation_token: cln_token,
                };

                let source_svc = proto::source_server::SourceServer::new(source_service)
                    .max_decoding_message_size(max_message_size)
                    .max_encoding_message_size(max_message_size);

                tonic::transport::Server::builder().add_service(source_svc)
            })
            .await
    }
}
#[cfg(test)]
mod tests {
    use crate::shared::ServerExtras;
    use chrono::Utc;
    use std::collections::{HashMap, HashSet};
    use std::error::Error;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::net::UnixStream;
    use tokio::sync::mpsc::Sender;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::Request;
    use tonic::transport::Uri;
    use tower::service_fn;
    use uuid::Uuid;

    use super::{Message, Offset, SourceReadRequest, proto};
    use crate::source;

    /// A test source that repeats a number for the requested count.
    /// Tracks acknowledgments to simulate realistic source behavior.
    #[derive(Debug)]
    struct Repeater {
        num: usize,
        yet_to_ack: std::sync::RwLock<HashSet<String>>,
    }

    impl Repeater {
        fn new(num: usize) -> Self {
            Self {
                num,
                yet_to_ack: std::sync::RwLock::new(HashSet::new()),
            }
        }
    }

    #[tonic::async_trait]
    impl source::Sourcer for Repeater {
        async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>) {
            let event_time = Utc::now();
            let mut message_offsets = Vec::with_capacity(request.count);

            for i in 0..request.count {
                let mut headers = HashMap::new();
                headers.insert("x-txn-id".to_string(), Uuid::new_v4().to_string());

                // Create unique offset using timestamp and index
                let offset = format!("{}-{}", event_time.timestamp_nanos_opt().unwrap(), i);

                transmitter
                    .send(Message {
                        value: self.num.to_le_bytes().to_vec(),
                        event_time,
                        offset: Offset {
                            offset: offset.clone().into_bytes(),
                            partition_id: 0,
                        },
                        keys: vec![],
                        headers,
                        user_metadata: None,
                    })
                    .await
                    .expect("Failed to send message");

                message_offsets.push(offset);
            }

            // Track unacknowledged messages
            self.yet_to_ack.write().unwrap().extend(message_offsets);
        }

        async fn ack(&self, offsets: Vec<Offset>) {
            let mut pending = self.yet_to_ack.write().unwrap();
            for offset in offsets {
                let offset_str = String::from_utf8(offset.offset).expect("Invalid UTF-8 in offset");
                pending.remove(&offset_str);
            }
        }

        async fn nack(&self, offsets: Vec<Offset>) {
            let mut pending = self.yet_to_ack.write().unwrap();
            for offset in offsets {
                let offset_str = String::from_utf8(offset.offset).expect("Invalid UTF-8 in offset");
                // For nack, we keep the offset in the pending set
                // In a real implementation, this might requeue the message
                pending.insert(offset_str);
            }
        }

        async fn pending(&self) -> Option<usize> {
            Some(self.yet_to_ack.read().unwrap().len())
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![2])
        }
    }

    /// Test utilities for setting up source server and client
    mod test_utils {
        use super::*;
        use std::path::PathBuf;
        use tokio::task::JoinHandle;

        /// Handle for managing test server lifecycle
        pub struct TestServerHandle {
            pub client: proto::source_client::SourceClient<tonic::transport::Channel>,
            pub shutdown_tx: oneshot::Sender<()>,
            pub server_task: JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
        }

        impl TestServerHandle {
            /// Gracefully shutdown the test server
            pub async fn shutdown(self) -> Result<(), Box<dyn Error>> {
                self.shutdown_tx
                    .send(())
                    .map_err(|_| "Failed to send shutdown signal")?;
                tokio::time::sleep(Duration::from_millis(50)).await;

                if !self.server_task.is_finished() {
                    return Err("Server task did not finish".into());
                }

                Ok(())
            }
        }

        /// Start a test source server with the given repeater and return client handle
        pub async fn start_test_server(
            repeater: Repeater,
        ) -> Result<TestServerHandle, Box<dyn Error>> {
            let tmp_dir = TempDir::new()?;
            let sock_file = tmp_dir.path().join("source.sock");
            let server_info_file = tmp_dir.path().join("sourcer-server-info");

            let server = source::Server::new(repeater)
                .with_server_info_file(&server_info_file)
                .with_socket_file(&sock_file)
                .with_max_message_size(10240);

            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let server_task =
                tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

            // Wait for server to start
            tokio::time::sleep(Duration::from_millis(50)).await;

            let client = create_test_client(sock_file).await?;

            Ok(TestServerHandle {
                client,
                shutdown_tx,
                server_task,
            })
        }

        /// Create a gRPC client connected to the test server
        async fn create_test_client(
            sock_file: PathBuf,
        ) -> Result<proto::source_client::SourceClient<tonic::transport::Channel>, Box<dyn Error>>
        {
            let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")?
                .connect_with_connector(service_fn(move |_: Uri| {
                    let sock_file = sock_file.clone();
                    async move {
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                            UnixStream::connect(sock_file).await?,
                        ))
                    }
                }))
                .await?;

            Ok(proto::source_client::SourceClient::new(channel))
        }

        /// Read messages from the source with proper handshake
        pub async fn read_messages(
            client: &mut proto::source_client::SourceClient<tonic::transport::Channel>,
            num_records: u64,
        ) -> Result<Vec<proto::read_response::Result>, Box<dyn Error>> {
            let (read_tx, read_rx) = mpsc::channel(4);

            // Send handshake
            let handshake_request = proto::ReadRequest {
                request: None,
                handshake: Some(proto::Handshake { sot: true }),
            };
            read_tx.send(handshake_request).await?;

            // Send read request
            let read_request = proto::ReadRequest {
                request: Some(proto::read_request::Request {
                    num_records,
                    timeout_in_ms: 1000,
                }),
                handshake: None,
            };
            read_tx.send(read_request).await?;
            drop(read_tx);

            let mut response_stream = client
                .read_fn(Request::new(ReceiverStream::new(read_rx)))
                .await?
                .into_inner();

            let mut messages = Vec::new();
            while let Some(response) = response_stream.message().await? {
                if let Some(status) = response.status {
                    if status.eot {
                        break;
                    }
                }
                if let Some(result) = response.result {
                    messages.push(result);
                }
            }

            Ok(messages)
        }

        /// Acknowledge messages with proper handshake
        pub async fn ack_messages(
            client: &mut proto::source_client::SourceClient<tonic::transport::Channel>,
            messages: &[proto::read_response::Result],
        ) -> Result<(), Box<dyn Error>> {
            let (ack_tx, ack_rx) = mpsc::channel(10);

            // Send handshake
            let ack_handshake_request = proto::AckRequest {
                request: None,
                handshake: Some(proto::Handshake { sot: true }),
            };
            ack_tx.send(ack_handshake_request).await?;

            // Send ack requests
            for message in messages {
                let ack_request = proto::AckRequest {
                    request: Some(proto::ack_request::Request {
                        offsets: vec![proto::Offset {
                            offset: message.offset.as_ref().unwrap().offset.clone(),
                            partition_id: message.offset.as_ref().unwrap().partition_id,
                        }],
                    }),
                    handshake: None,
                };
                ack_tx.send(ack_request).await?;
            }
            drop(ack_tx);

            let mut ack_response = client
                .ack_fn(Request::new(ReceiverStream::new(ack_rx)))
                .await?
                .into_inner();

            // Consume handshake response
            let handshake_response = ack_response.message().await?.unwrap();
            assert!(handshake_response.handshake.unwrap().sot);

            // Consume ack responses
            for _ in 0..messages.len() {
                assert!(ack_response.message().await?.is_some());
            }

            Ok(())
        }

        /// Negatively acknowledge messages
        pub async fn nack_messages(
            client: &mut proto::source_client::SourceClient<tonic::transport::Channel>,
            messages: &[proto::read_response::Result],
        ) -> Result<(), Box<dyn Error>> {
            for message in messages {
                let nack_request = proto::NackRequest {
                    request: Some(proto::nack_request::Request {
                        offsets: vec![proto::Offset {
                            offset: message.offset.as_ref().unwrap().offset.clone(),
                            partition_id: message.offset.as_ref().unwrap().partition_id,
                        }],
                    }),
                };

                client.nack_fn(Request::new(nack_request)).await?;
            }

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_source_read_ack_pending_partitions() -> Result<(), Box<dyn Error>> {
        // Setup test server with Repeater source
        let repeater = Repeater::new(42);
        let mut handle = test_utils::start_test_server(repeater).await?;

        // Test read operation
        let messages = test_utils::read_messages(&mut handle.client, 3).await?;
        assert_eq!(messages.len(), 3, "Should read exactly 3 messages");

        // Verify message content
        for message in &messages {
            // The Repeater stores usize as bytes, so we need to convert back
            let bytes: [u8; std::mem::size_of::<usize>()] = message
                .payload
                .as_slice()
                .try_into()
                .expect("Invalid payload size");
            let value = usize::from_le_bytes(bytes);
            assert_eq!(value, 42, "Message value should be 42");
            assert!(message.offset.is_some(), "Message should have offset");
            assert_eq!(message.offset.as_ref().unwrap().partition_id, 0);
        }

        // Test pending before ack
        let pending_response = handle.client.pending_fn(Request::new(())).await?;
        assert_eq!(
            pending_response.into_inner().result.unwrap().count,
            3,
            "Should have 3 pending messages before ack"
        );

        // Test partitions
        let partitions_response = handle.client.partitions_fn(Request::new(())).await?;
        assert_eq!(
            partitions_response.into_inner().result.unwrap().partitions,
            vec![2],
            "Should return partition [2]"
        );

        // Test ack operation
        test_utils::ack_messages(&mut handle.client, &messages).await?;

        // Test pending after ack
        let pending_response = handle.client.pending_fn(Request::new(())).await?;
        assert_eq!(
            pending_response.into_inner().result.unwrap().count,
            0,
            "Should have 0 pending messages after ack"
        );

        // Cleanup
        handle.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_source_read_nack_pending() -> Result<(), Box<dyn Error>> {
        // Setup test server with Repeater source
        let repeater = Repeater::new(100);
        let mut handle = test_utils::start_test_server(repeater).await?;

        // Test read operation
        let messages = test_utils::read_messages(&mut handle.client, 2).await?;
        assert_eq!(messages.len(), 2, "Should read exactly 2 messages");

        // Verify message content
        for message in &messages {
            // The Repeater stores usize as bytes, so we need to convert back
            let bytes: [u8; std::mem::size_of::<usize>()] = message
                .payload
                .as_slice()
                .try_into()
                .expect("Invalid payload size");
            let value = usize::from_le_bytes(bytes);
            assert_eq!(value, 100, "Message value should be 100");
        }

        // Test pending before nack
        let pending_response = handle.client.pending_fn(Request::new(())).await?;
        assert_eq!(
            pending_response.into_inner().result.unwrap().count,
            2,
            "Should have 2 pending messages before nack"
        );

        // Test nack operation
        test_utils::nack_messages(&mut handle.client, &messages).await?;

        // Test pending after nack (messages should still be pending)
        let pending_response = handle.client.pending_fn(Request::new(())).await?;
        assert_eq!(
            pending_response.into_inner().result.unwrap().count,
            2,
            "Should still have 2 pending messages after nack"
        );

        // Test that we can still ack the messages after nack
        test_utils::ack_messages(&mut handle.client, &messages).await?;

        // Test pending after ack
        let pending_response = handle.client.pending_fn(Request::new(())).await?;
        assert_eq!(
            pending_response.into_inner().result.unwrap().count,
            0,
            "Should have 0 pending messages after ack"
        );

        // Cleanup
        handle.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_source_server_configuration() -> Result<(), Box<dyn Error>> {
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("custom_source.sock");
        let server_info_file = tmp_dir.path().join("custom-server-info");

        let server = source::Server::new(Repeater::new(1))
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file)
            .with_max_message_size(8192);

        // Test configuration getters
        assert_eq!(server.max_message_size(), 8192);
        assert_eq!(server.server_info_file(), server_info_file);
        assert_eq!(server.socket_file(), sock_file);

        Ok(())
    }
}
