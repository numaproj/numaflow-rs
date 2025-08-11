use chrono::{DateTime, Utc};
use std::collections::HashMap;

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info};

use crate::error::Error;

use crate::proto::map as proto;
use crate::proto::map::map_server::Map;
use crate::proto::map::{MapRequest, MapResponse, ReadyResponse};
use crate::shared::{
    self, shutdown_signal, ContainerType, ServerConfig, ServiceError, SocketCleanup,
    DEFAULT_BATCHMAP_SERVER_INFO_FILE, DEFAULT_BATCHMAP_SOCK_ADDR, DEFAULT_CHANNEL_SIZE, DROP,
};

struct BatchMapService<T: BatchMapper> {
    handler: Arc<T>,
    shutdown_tx: mpsc::Sender<()>,
    cancellation_token: CancellationToken,
}

/// BatchMapper trait for implementing batch mode user defined function.
///
/// Types implementing this trait can be passed as user defined batch map handle.
#[tonic::async_trait]
pub trait BatchMapper {
    /// The batch map handle is given a stream of [`Datum`] and the result is
    /// Vec of [`BatchResponse`].
    /// Here it's important to note that the size of the vec for the responses
    /// should be equal to the number of elements in the input stream.
    ///
    /// # Example
    ///
    /// A simple batch map.
    ///
    /// ```no_run
    /// use numaflow::batchmap::{self, BatchResponse, Datum, Message};
    /// use std::error::Error;
    ///
    /// struct FlatMap;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    ///     batchmap::Server::new(FlatMap).start().await
    /// }
    ///
    /// #[tonic::async_trait]
    /// impl batchmap::BatchMapper for FlatMap {
    ///
    /// async fn batchmap(&self, mut input: tokio::sync::mpsc::Receiver<Datum>) -> Vec<BatchResponse> {
    ///     let mut responses: Vec<BatchResponse> = Vec::new();
    ///          while let Some(datum) = input.recv().await {
    ///             let mut response = BatchResponse::from_id(datum.id);
    ///             response.append(Message {
    ///                     keys: Option::from(datum.keys),
    ///                     value: datum.value,
    ///                     tags: None,
    ///             });
    ///             responses.push(response);
    ///         }
    ///         responses
    ///     }
    /// }
    /// ```
    async fn batchmap(&self, input: mpsc::Receiver<Datum>) -> Vec<BatchResponse>;
}

/// Incoming request into the handler of [`BatchMapper`].
pub struct Datum {
    /// Set of keys in the (key, value) terminology of map/reduce paradigm.
    pub keys: Vec<String>,
    /// The value in the (key, value) terminology of map/reduce paradigm.
    pub value: Vec<u8>,
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this time.
    pub watermark: DateTime<Utc>,
    /// Time of the element as seen at source or aligned after a reduce operation.
    pub event_time: DateTime<Utc>,
    /// ID is the unique id of the message to be sent to the Batch Map.
    pub id: String,
    /// Headers for the message.
    pub headers: HashMap<String, String>,
}

impl TryFrom<MapRequest> for Datum {
    type Error = Status;

    fn try_from(sr: MapRequest) -> Result<Self, Self::Error> {
        let request = sr
            .request
            .ok_or_else(|| Status::invalid_argument("Invalid argument, request can't be None"))?;

        Ok(Self {
            keys: request.keys,
            value: request.value,
            watermark: shared::utc_from_timestamp(request.watermark),
            event_time: shared::utc_from_timestamp(request.event_time),
            id: sr.id,
            headers: request.headers,
        })
    }
}

/// Message is the response struct from the [`Mapper::map`][`crate::map::Mapper::map`] .
#[derive(Debug, PartialEq)]
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection.
    pub keys: Option<Vec<String>>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags: Option<Vec<String>>,
}

/// Represents a message that can be modified and forwarded.
impl Message {
    /// Creates a new message with the specified value.
    ///
    /// This constructor initializes the message with no keys, tags, or specific event time.
    ///
    /// # Arguments
    ///
    /// * `value` - A vector of bytes representing the message's payload.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::batchmap::Message;
    /// let message = Message::new(vec![1, 2, 3, 4]);
    /// ```
    pub fn new(value: Vec<u8>) -> Self {
        Self {
            value,
            keys: None,
            tags: None,
        }
    }
    /// Marks the message to be dropped by creating a new `Message` with an empty value and a special "DROP" tag.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::batchmap::Message;
    /// let dropped_message = Message::message_to_drop();
    /// ```
    pub fn message_to_drop() -> Message {
        Message {
            keys: None,
            value: vec![],
            tags: Some(vec![DROP.to_string()]),
        }
    }

    /// Sets or replaces the keys associated with this message.
    ///
    /// # Arguments
    ///
    /// * `keys` - A vector of strings representing the keys.
    ///
    /// # Examples
    ///
    /// ```
    ///  use numaflow::batchmap::Message;
    /// let message = Message::new(vec![1, 2, 3]).with_keys(vec!["key1".to_string(), "key2".to_string()]);
    /// ```
    pub fn with_keys(mut self, keys: Vec<String>) -> Self {
        self.keys = Some(keys);
        self
    }

    /// Sets or replaces the tags associated with this message.
    ///
    /// # Arguments
    ///
    /// * `tags` - A vector of strings representing the tags.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::batchmap::Message;
    /// let message = Message::new(vec![1, 2, 3]).with_tags(vec!["tag1".to_string(), "tag2".to_string()]);
    /// ```
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = Some(tags);
        self
    }
}
/// The result of the call to [`BatchMapper::batchmap`] method.
pub struct BatchResponse {
    /// id is the unique ID of the message.
    pub id: String,
    // message is the response from the batch map.
    pub message: Vec<Message>,
}

impl BatchResponse {
    /// Creates a new `BatchResponse` for a given id and empty message.
    pub fn from_id(id: String) -> Self {
        Self {
            id,
            message: Vec::new(),
        }
    }

    /// append a message to the response.
    pub fn append(&mut self, message: Message) {
        self.message.push(message);
    }
}

impl From<Message> for proto::map_response::Result {
    fn from(value: Message) -> Self {
        proto::map_response::Result {
            keys: value.keys.unwrap_or_default(),
            value: value.value,
            tags: value.tags.unwrap_or_default(),
        }
    }
}

#[tonic::async_trait]
impl<T> Map for BatchMapService<T>
where
    T: BatchMapper + Send + Sync + 'static,
{
    type MapFnStream = ReceiverStream<Result<MapResponse, Status>>;

    async fn map_fn(
        &self,
        request: Request<Streaming<MapRequest>>,
    ) -> Result<Response<Self::MapFnStream>, Status> {
        let mut map_stream = request.into_inner();
        let map_handle = self.handler.clone();
        let shutdown_tx = self.shutdown_tx.clone();
        let cln_token = self.cancellation_token.clone();
        let (resp_tx, resp_rx) = channel::<Result<MapResponse, Status>>(DEFAULT_CHANNEL_SIZE);

        self.perform_handshake(&mut map_stream, &resp_tx).await?;

        let grpc_resp_tx = resp_tx.clone();
        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            Self::process_map_stream(map_handle, map_stream, grpc_resp_tx).await
        });

        tokio::spawn(Self::handle_map_errors(
            handle,
            resp_tx,
            shutdown_tx,
            cln_token,
        ));

        Ok(Response::new(ReceiverStream::new(resp_rx)))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<ReadyResponse>, Status> {
        Ok(Response::new(ReadyResponse { ready: true }))
    }
}

impl<T> BatchMapService<T>
where
    T: BatchMapper + Send + Sync + 'static,
{
    /// processes the stream of requests from the client
    async fn process_map_stream(
        map_handle: Arc<T>,
        mut map_stream: Streaming<MapRequest>,
        grpc_resp_tx: mpsc::Sender<Result<MapResponse, Status>>,
    ) -> Result<(), Error> {
        // loop until the global stream has been shutdown.
        let mut global_stream_ended = false;
        while !global_stream_ended {
            // for every batch, we need to read from the stream. The end-of-batch is
            // encoded in the request.
            global_stream_ended =
                Self::process_map_batch(map_handle.clone(), &mut map_stream, grpc_resp_tx.clone())
                    .await?;
        }
        Ok(())
    }

    /// Processes a batch of messages from the client, sends them to the batch map handler, and sends the
    /// responses back to the client. Batches are separated by an EOT message.
    ///
    /// Returns `true` if the global bidi-stream has ended, otherwise `false`.
    async fn process_map_batch(
        batch_map_handle: Arc<T>,
        map_stream: &mut Streaming<MapRequest>,
        grpc_resp_tx: mpsc::Sender<Result<MapResponse, Status>>,
    ) -> Result<bool, Error> {
        let (tx, rx) = channel::<Datum>(DEFAULT_CHANNEL_SIZE);
        let resp_tx = grpc_resp_tx.clone();
        let batch_map_handle = batch_map_handle.clone();

        let batch_mapper_handle = tokio::spawn(async move {
            let responses = batch_map_handle.batchmap(rx).await;
            for response in responses {
                resp_tx
                    .send(Ok(MapResponse {
                        results: response
                            .message
                            .into_iter()
                            .map(|m| m.into())
                            .collect::<Vec<proto::map_response::Result>>(),
                        id: response.id,
                        handshake: None,
                        status: None,
                    }))
                    .await
                    .expect("Sending response to channel");
            }

            // send the eot message to the client
            resp_tx
                .send(Ok(MapResponse {
                    results: vec![],
                    id: "".to_string(),
                    handshake: None,
                    status: Some(proto::TransmissionStatus { eot: true }),
                }))
                .await
                .expect("Sending response to channel");
        });

        let mut global_stream_ended = false;

        // loop until eot happens or the stream is closed.
        loop {
            let message = match map_stream.message().await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    info!("global bidi stream ended");
                    global_stream_ended = true;
                    break;
                }
                Err(e) => {
                    return Err(Server::<()>::internal_error(format!(
                        "Error reading message from stream: {}",
                        e
                    )))
                }
            };

            // we are done with this batch because eot=true
            if message.status.is_some_and(|status| status.eot) {
                debug!("Batch Ended, received an EOT message");
                break;
            }

            // write to the UDF's tx
            tx.send(
                message
                    .try_into()
                    .map_err(|e| Server::<()>::internal_error(format!("{:?}", e)))?,
            )
            .await
            .map_err(|e| {
                Server::<()>::internal_error(format!("Error sending message to map handler: {}", e))
            })?;
        }

        // drop the sender to signal the batch map handler that the batch has ended
        drop(tx);

        // wait for UDF task to return
        batch_mapper_handle
            .await
            .map_err(|e| Server::<()>::user_error(e.to_string()))?;

        Ok(global_stream_ended)
    }

    async fn handle_map_errors(
        handle: JoinHandle<Result<(), Error>>,
        resp_tx: mpsc::Sender<Result<MapResponse, Status>>,
        shutdown_tx: mpsc::Sender<()>,
        cln_token: CancellationToken,
    ) {
        tokio::select! {
            resp = handle => {
                match resp {
                    Ok(Ok(_)) => {},
                    Ok(Err(e)) => {
                        resp_tx
                            .send(Err(Status::internal(e.to_string())))
                            .await
                            .expect("Sending error to response channel");
                        shutdown_tx.send(()).await.expect("Sending shutdown signal");
                    }
                    Err(e) => {
                        resp_tx
                            .send(Err(Status::internal(format!("Map handler aborted: {}", e))))
                            .await
                            .expect("Sending error to response channel");
                        shutdown_tx.send(()).await.expect("Sending shutdown signal");
                    }
                }
            },
            _ = cln_token.cancelled() => {
                resp_tx
                    .send(Err(Status::cancelled("Map handler cancelled")))
                    .await
                    .expect("Sending error to response channel");
            }
        }
    }

    async fn perform_handshake(
        &self,
        map_stream: &mut Streaming<MapRequest>,
        resp_tx: &mpsc::Sender<Result<MapResponse, Status>>,
    ) -> Result<(), Status> {
        let handshake_request = map_stream
            .message()
            .await
            .map_err(|e| Status::internal(format!("handshake failed {}", e)))?
            .ok_or_else(|| Status::internal("stream closed before handshake"))?;

        if let Some(handshake) = handshake_request.handshake {
            resp_tx
                .send(Ok(MapResponse {
                    results: vec![],
                    id: "".to_string(),
                    handshake: Some(handshake),
                    status: None,
                }))
                .await
                .map_err(|e| {
                    Status::internal(format!("failed to send handshake response {}", e))
                })?;
            Ok(())
        } else {
            Err(Status::invalid_argument("Handshake not present"))
        }
    }
}

/// gRPC server to start a batch map service
#[derive(Debug)]
pub struct Server<T> {
    config: ServerConfig,
    svc: Option<T>,
    _cleanup: SocketCleanup,
}
impl<T> Server<T> {
    pub fn new(batch_map_svc: T) -> Self {
        let config = ServerConfig::new(
            DEFAULT_BATCHMAP_SOCK_ADDR,
            DEFAULT_BATCHMAP_SERVER_INFO_FILE,
        );
        let cleanup = SocketCleanup::new(config.sock_addr.clone());

        Self {
            config,
            svc: Some(batch_map_svc),
            _cleanup: cleanup,
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    /// Default value is `/var/run/numaflow/batchmap.sock`
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.config = self.config.with_socket_file(file);
        self._cleanup = SocketCleanup::new(self.config.sock_addr.clone());
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections. Default value is `/var/run/numaflow/batchmap.sock`
    pub fn socket_file(&self) -> &std::path::Path {
        self.config.socket_file()
    }

    /// Set the maximum size of an encoded and decoded gRPC message. The value of `message_size` is in bytes. Default value is 64MB.
    pub fn with_max_message_size(mut self, message_size: usize) -> Self {
        self.config = self.config.with_max_message_size(message_size);
        self
    }

    /// Get the maximum size of an encoded and decoded gRPC message in bytes. Default value is 64MB.
    pub fn max_message_size(&self) -> usize {
        self.config.max_message_size()
    }

    /// Change the file in which numaflow server information is stored on start up to the new value. Default value is `/var/run/numaflow/batchmapper-server-info`
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.config = self.config.with_server_info_file(file);
        self
    }

    /// Get the path to the file where numaflow server info is stored. Default value is `/var/run/numaflow/mapper-server-info`
    pub fn server_info_file(&self) -> &std::path::Path {
        self.config.server_info_file()
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown(
        &mut self,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: BatchMapper + Send + Sync + 'static,
    {
        let info = shared::ServerInfo::new(ContainerType::BatchMap);
        let listener = shared::create_listener_stream(
            &self.config.sock_addr,
            &self.config.server_info_file,
            info,
        )?;
        let handler = self.svc.take().unwrap();

        let cln_token = CancellationToken::new();

        // Create a channel to send shutdown signal to the server to do graceful shutdown in case of non retryable errors.
        let (internal_shutdown_tx, internal_shutdown_rx) = channel(1);
        let map_svc = BatchMapService {
            handler: Arc::new(handler),
            shutdown_tx: internal_shutdown_tx,
            cancellation_token: cln_token.clone(),
        };

        let map_svc = proto::map_server::MapServer::new(map_svc)
            .max_encoding_message_size(self.config.max_message_size)
            .max_decoding_message_size(self.config.max_message_size);

        let shutdown = shutdown_signal(internal_shutdown_rx, Some(shutdown_rx));

        tonic::transport::Server::builder()
            .add_service(map_svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await?;

        Ok(())
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the signal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: BatchMapper + Send + Sync + 'static,
    {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        self.start_with_shutdown(shutdown_rx).await
    }
}

/// Implementation of ServiceError trait for Batchmap service
impl<T> ServiceError for Server<T> {
    fn service_name() -> &'static str {
        "batchmap"
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};

    use tempfile::TempDir;
    use tokio::net::UnixStream;
    use tokio::sync::mpsc::Receiver;
    use tokio::sync::oneshot;
    use tonic::transport::Uri;
    use tower::service_fn;

    use crate::batchmap;
    use crate::batchmap::{BatchResponse, Datum, Message};
    use crate::proto::map::map_client::MapClient;

    #[tokio::test]
    async fn batch_map_server() -> Result<(), Box<dyn Error>> {
        struct Logger;
        #[tonic::async_trait]
        impl batchmap::BatchMapper for Logger {
            async fn batchmap(&self, mut input: Receiver<Datum>) -> Vec<BatchResponse> {
                let mut responses: Vec<BatchResponse> = Vec::new();
                while let Some(datum) = input.recv().await {
                    let mut response = BatchResponse::from_id(datum.id);
                    response.append(Message {
                        keys: Option::from(datum.keys),
                        value: datum.value,
                        tags: None,
                    });
                    responses.push(response);
                }
                responses
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("batchmap.sock");
        let server_info_file = tmp_dir.path().join("batchmapper-server-info");

        let mut server = batchmap::Server::new(Logger)
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file)
            .with_max_message_size(10240);

        assert_eq!(server.max_message_size(), 10240);
        assert_eq!(server.server_info_file(), server_info_file);
        assert_eq!(server.socket_file(), sock_file);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // https://github.com/hyperium/tonic/blob/master/examples/src/uds/client.rs
        let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(service_fn(move |_: Uri| {
                // https://rust-lang.github.io/async-book/03_async_await/01_chapter.html#async-lifetimes
                let sock_file = sock_file.clone();
                async move {
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                        UnixStream::connect(sock_file).await?,
                    ))
                }
            }))
            .await?;

        let mut client = MapClient::new(channel);
        let handshake_request = crate::proto::map::MapRequest {
            request: None,
            id: "0".to_string(),
            handshake: Some(crate::proto::map::Handshake { sot: true }),
            status: None,
        };

        let request = crate::proto::map::MapRequest {
            request: Some(crate::proto::map::map_request::Request {
                keys: vec!["first".into()],
                value: "hello".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                headers: Default::default(),
            }),
            id: "1".to_string(),
            handshake: None,
            status: None,
        };

        let request2 = crate::proto::map::MapRequest {
            request: Some(crate::proto::map::map_request::Request {
                keys: vec!["second".into()],
                value: "hello2".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                headers: Default::default(),
            }),
            id: "2".to_string(),
            handshake: None,
            status: None,
        };

        let eot_request = crate::proto::map::MapRequest {
            request: None,
            id: "3".to_string(),
            handshake: None,
            status: Some(crate::proto::map::TransmissionStatus { eot: true }),
        };

        let resp = client
            .map_fn(tokio_stream::iter(vec![
                handshake_request,
                request,
                request2,
                eot_request,
            ]))
            .await?;
        let mut r = resp.into_inner();
        let mut responses = Vec::new();

        while let Some(response) = r.message().await? {
            responses.push(response);
        }

        assert_eq!(responses.len(), 5, "Expected five message from server");
        assert!(responses[0].handshake.is_some());
        assert_eq!(&responses[1].id, "1");
        assert_eq!(&responses[2].id, "2");

        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");
        Ok(())
    }

    #[tokio::test]
    async fn batchmap_panic() -> Result<(), Box<dyn Error>> {
        struct PanicBatch;
        #[tonic::async_trait]
        impl batchmap::BatchMapper for PanicBatch {
            async fn batchmap(&self, _input: Receiver<Datum>) -> Vec<BatchResponse> {
                panic!("Should not cross 5");
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("batchmap.sock");
        let server_info_file = tmp_dir.path().join("mapper-server-info");

        let mut server = batchmap::Server::new(PanicBatch)
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file)
            .with_max_message_size(10240);

        assert_eq!(server.max_message_size(), 10240);
        assert_eq!(server.server_info_file(), server_info_file);
        assert_eq!(server.socket_file(), sock_file);

        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // https://github.com/hyperium/tonic/blob/master/examples/src/uds/client.rs
        let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(service_fn(move |_: Uri| {
                // https://rust-lang.github.io/async-book/03_async_await/01_chapter.html#async-lifetimes
                let sock_file = sock_file.clone();
                async move {
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                        UnixStream::connect(sock_file).await?,
                    ))
                }
            }))
            .await?;

        let mut client = MapClient::new(channel);
        let mut requests = Vec::new();

        let handshake_request = crate::proto::map::MapRequest {
            request: None,
            id: "0".to_string(),
            handshake: Some(crate::proto::map::Handshake { sot: true }),
            status: None,
        };
        requests.push(handshake_request);
        for i in 0..10 {
            let request = crate::proto::map::MapRequest {
                request: Some(crate::proto::map::map_request::Request {
                    keys: vec!["first".into(), "second".into()],
                    value: format!("hello {}", i).into(),
                    watermark: Some(prost_types::Timestamp::default()),
                    event_time: Some(prost_types::Timestamp::default()),
                    headers: Default::default(),
                }),
                id: i.to_string(),
                handshake: None,
                status: None,
            };
            requests.push(request);
        }
        let eot_request = crate::proto::map::MapRequest {
            request: None,
            id: "11".to_string(),
            handshake: None,
            status: Some(crate::proto::map::TransmissionStatus { eot: true }),
        };
        requests.push(eot_request);

        let resp = client.map_fn(tokio_stream::iter(requests)).await?;
        let mut response_stream = resp.into_inner();

        // handshake response
        let response = response_stream.message().await.expect("handshake response");
        assert!(response.unwrap().handshake.is_some());

        if let Err(e) = response_stream.message().await {
            println!("Error: {:?}", e);
            assert_eq!(e.code(), tonic::Code::Internal);
            assert!(e.message().contains("User Defined error"))
        } else {
            return Err("Expected error from server".into());
        };

        // server should shut down gracefully because there was a panic in the handler.
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if task.is_finished() {
                break;
            }
        }
        assert!(task.is_finished(), "gRPC server is still running");
        Ok(())
    }
}
