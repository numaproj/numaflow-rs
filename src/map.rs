use crate::error::{Error, ErrorKind};
use crate::map::proto::MapResponse;
use crate::shared::{self, shutdown_signal, ContainerType};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{async_trait, Request, Response, Status, Streaming};
use tracing::{error, info};

const DEFAULT_CHANNEL_SIZE: usize = 1000;
const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_SOCK_ADDR: &str = "/var/run/numaflow/map.sock";
const DEFAULT_SERVER_INFO_FILE: &str = "/var/run/numaflow/mapper-server-info";
const DROP: &str = "U+005C__DROP__";

/// Numaflow Map Proto definitions.
pub mod proto {
    tonic::include_proto!("map.v1");
}

struct MapService<T> {
    handler: Arc<T>,
    shutdown_tx: mpsc::Sender<()>,
    cancellation_token: CancellationToken,
}

/// Mapper trait for implementing Map handler.
#[async_trait]
pub trait Mapper {
    /// The `map` takes in an input element and can produce 0, 1, or more results.
    /// In a `map` function, each element is processed independently and there is no state associated with the elements.
    /// More about map can be read [here](https://numaflow.numaproj.io/user-guide/user-defined-functions/map/map/#map-udf).
    ///
    /// # Example
    ///
    /// Following is an example of a `cat` container that just copies the input to output.
    ///
    /// ```no_run
    /// use numaflow::map;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     map::Server::new(Cat).start().await?;
    ///     Ok(())
    /// }
    ///
    /// struct Cat;
    ///
    /// #[tonic::async_trait]
    /// impl map::Mapper for Cat {
    ///     async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
    ///       use numaflow::map::Message;
    ///       let message=Message::new(input.value).keys(input.keys).tags(vec![]);
    ///         vec![message]
    ///     }
    /// }
    /// ```
    async fn map(&self, input: MapRequest) -> Vec<Message>;
}

/// Message is the response struct from the [`Mapper::map`] .
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
    /// use numaflow::map::Message;
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
    /// use numaflow::map::Message;
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
    ///  use numaflow::map::Message;
    /// let message = Message::new(vec![1, 2, 3]).keys(vec!["key1".to_string(), "key2".to_string()]);
    /// ```
    pub fn keys(mut self, keys: Vec<String>) -> Self {
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
    /// use numaflow::map::Message;
    /// let message = Message::new(vec![1, 2, 3]).tags(vec!["tag1".to_string(), "tag2".to_string()]);
    /// ```
    pub fn tags(mut self, tags: Vec<String>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Replaces the value of the message.
    ///
    /// # Arguments
    ///
    /// * `value` - A new vector of bytes that replaces the current message value.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::map::Message;
    /// let message = Message::new(vec![1, 2, 3]).value(vec![4, 5, 6]);
    /// ```
    pub fn value(mut self, value: Vec<u8>) -> Self {
        self.value = value;
        self
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

/// Incoming request into the map handler of [`Mapper`].
pub struct MapRequest {
    /// Set of keys in the (key, value) terminology of map/reduce paradigm.
    pub keys: Vec<String>,
    /// The value in the (key, value) terminology of map/reduce paradigm.
    pub value: Vec<u8>,
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this time.
    pub watermark: DateTime<Utc>,
    /// Time of the element as seen at source or aligned after a reduce operation.
    pub eventtime: DateTime<Utc>,
    /// Headers for the message.
    pub headers: HashMap<String, String>,
}

impl From<proto::map_request::Request> for MapRequest {
    fn from(value: proto::map_request::Request) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            watermark: shared::utc_from_timestamp(value.watermark),
            eventtime: shared::utc_from_timestamp(value.event_time),
            headers: value.headers,
        }
    }
}

#[async_trait]
impl<T> proto::map_server::Map for MapService<T>
where
    T: Mapper + Send + Sync + 'static,
{
    type MapFnStream = ReceiverStream<Result<MapResponse, Status>>;

    async fn map_fn(
        &self,
        request: Request<Streaming<proto::MapRequest>>,
    ) -> Result<Response<Self::MapFnStream>, Status> {
        let mut stream = request.into_inner();
        let handler = Arc::clone(&self.handler);

        let (stream_response_tx, stream_response_rx) =
            mpsc::channel::<Result<MapResponse, Status>>(DEFAULT_CHANNEL_SIZE);

        // perform handshake
        perform_handshake(&mut stream, &stream_response_tx).await?;

        let (error_tx, error_rx) = mpsc::channel::<Error>(1);

        // Spawn a task to handle incoming stream requests
        let handle: JoinHandle<()> = tokio::spawn(handle_stream_requests(
            handler.clone(),
            stream,
            stream_response_tx.clone(),
            error_tx.clone(),
            self.cancellation_token.child_token(),
        ));

        tokio::spawn(manage_grpc_stream(
            handle,
            self.cancellation_token.clone(),
            stream_response_tx,
            error_rx,
            self.shutdown_tx.clone(),
        ));

        Ok(Response::new(ReceiverStream::new(stream_response_rx)))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}

async fn handle_stream_requests<T>(
    handler: Arc<T>,
    mut stream: Streaming<proto::MapRequest>,
    stream_response_tx: mpsc::Sender<Result<MapResponse, Status>>,
    error_tx: mpsc::Sender<Error>,
    token: CancellationToken,
) where
    T: Mapper + Send + Sync + 'static,
{
    let mut stream_open = true;
    while stream_open {
        stream_open = tokio::select! {
            map_request = stream.message() => handle_request(
                handler.clone(),
                map_request,
                stream_response_tx.clone(),
                error_tx.clone(),
                token.clone(),
            ).await,
            _ = token.cancelled() => {
                info!("Cancellation token is cancelled, shutting down");
                break;
            }
        }
    }
}

async fn manage_grpc_stream(
    request_handler: JoinHandle<()>,
    token: CancellationToken,
    stream_response_tx: mpsc::Sender<Result<MapResponse, Status>>,
    mut error_rx: mpsc::Receiver<Error>,
    server_shutdown_tx: mpsc::Sender<()>,
) {
    let err = tokio::select! {
        _ = request_handler => {
            token.cancel();
            return;
        },
        err = error_rx.recv() => err,
    };

    token.cancel();
    let Some(err) = err else {
        return;
    };
    error!("Shutting down gRPC channel: {err:?}");
    stream_response_tx
        .send(Err(Status::internal(err.to_string())))
        .await
        .expect("Sending error message to gRPC response channel");
    server_shutdown_tx
        .send(())
        .await
        .expect("Writing to shutdown channel");
}

async fn handle_request<T>(
    handler: Arc<T>,
    map_request: Result<Option<proto::MapRequest>, Status>,
    stream_response_tx: mpsc::Sender<Result<MapResponse, Status>>,
    error_tx: mpsc::Sender<Error>,
    token: CancellationToken,
) -> bool
where
    T: Mapper + Send + Sync + 'static,
{
    let map_request = match map_request {
        Ok(None) => return false,
        Ok(Some(val)) => val,
        Err(val) => {
            error!("Received gRPC error from sender: {val:?}");
            return false;
        }
    };
    tokio::spawn(run_map(
        handler,
        map_request,
        stream_response_tx,
        error_tx,
        token,
    ));
    true
}

async fn run_map<T>(
    handler: Arc<T>,
    map_request: proto::MapRequest,
    stream_response_tx: mpsc::Sender<Result<MapResponse, Status>>,
    error_tx: mpsc::Sender<Error>,
    token: CancellationToken,
) where
    T: Mapper + Send + Sync + 'static,
{
    let Some(request) = map_request.request else {
        error_tx
            .send(Error::MapError(ErrorKind::InternalError(
                "Request not present".to_string(),
            )))
            .await
            .expect("Sending error on channel");
        return;
    };

    let message_id = map_request.id.clone();

    // A new task is spawned to catch the panic
    let udf_map_task = tokio::spawn({
        let handler = handler.clone();
        let token = token.child_token();
        async move {
            tokio::select! {
                _ = token.cancelled() => None,
                messages = handler.map(request.into()) => Some(messages),
            }
        }
    });

    let messages = match udf_map_task.await {
        Ok(messages) => messages,
        Err(e) => {
            error!("Failed to run map function: {e:?}");
            error_tx
                .send(Error::MapError(ErrorKind::InternalError(format!(
                    "panicked: {e:?}"
                ))))
                .await
                .expect("Sending error on channel");
            return;
        }
    };

    let Some(messages) = messages else {
        // CancellationToken is cancelled
        return;
    };

    let send_response_result = stream_response_tx
        .send(Ok(MapResponse {
            results: messages.into_iter().map(|msg| msg.into()).collect(),
            id: message_id,
            handshake: None,
        }))
        .await;

    let Err(e) = send_response_result else {
        return;
    };

    error_tx
        .send(Error::MapError(ErrorKind::InternalError(format!(
            "Failed to send response: {e:?}"
        ))))
        .await
        .expect("Sending error on channel");
}

async fn perform_handshake(
    stream: &mut Streaming<proto::MapRequest>,
    stream_response_tx: &mpsc::Sender<Result<MapResponse, Status>>,
) -> Result<(), Status> {
    let handshake_request = stream
        .message()
        .await
        .map_err(|e| Status::internal(format!("Handshake failed: {}", e)))?
        .ok_or_else(|| Status::internal("Stream closed before handshake"))?;

    if let Some(handshake) = handshake_request.handshake {
        stream_response_tx
            .send(Ok(MapResponse {
                results: vec![],
                id: "".to_string(),
                handshake: Some(handshake),
            }))
            .await
            .map_err(|e| Status::internal(format!("Failed to send handshake response: {}", e)))?;
        Ok(())
    } else {
        Err(Status::invalid_argument("Handshake not present"))
    }
}

/// gRPC server to start a map service
#[derive(Debug)]
pub struct Server<T> {
    sock_addr: PathBuf,
    max_message_size: usize,
    server_info_file: PathBuf,
    svc: Option<T>,
}

impl<T> Server<T> {
    pub fn new(map_svc: T) -> Self {
        Server {
            sock_addr: DEFAULT_SOCK_ADDR.into(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            server_info_file: DEFAULT_SERVER_INFO_FILE.into(),
            svc: Some(map_svc),
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    /// Default value is `/var/run/numaflow/map.sock`
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.sock_addr = file.into();
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections. Default value is `/var/run/numaflow/map.sock`
    pub fn socket_file(&self) -> &std::path::Path {
        self.sock_addr.as_path()
    }

    /// Set the maximum size of an encoded and decoded gRPC message. The value of `message_size` is in bytes. Default value is 64MB.
    pub fn with_max_message_size(mut self, message_size: usize) -> Self {
        self.max_message_size = message_size;
        self
    }

    /// Get the maximum size of an encoded and decoded gRPC message in bytes. Default value is 64MB.
    pub fn max_message_size(&self) -> usize {
        self.max_message_size
    }

    /// Change the file in which numaflow server information is stored on start up to the new value. Default value is `/var/run/numaflow/mapper-server-info`
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.server_info_file = file.into();
        self
    }

    /// Get the path to the file where numaflow server info is stored. Default value is `/var/run/numaflow/mapper-server-info`
    pub fn server_info_file(&self) -> &std::path::Path {
        self.server_info_file.as_path()
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown(
        &mut self,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Mapper + Send + Sync + 'static,
    {
        let info = shared::ServerInfo::new(ContainerType::Map);
        let listener =
            shared::create_listener_stream(&self.sock_addr, &self.server_info_file, info)?;
        let handler = self.svc.take().unwrap();
        let cln_token = CancellationToken::new();

        // Create a channel to send shutdown signal to the server to do graceful shutdown in case of non retryable errors.
        let (internal_shutdown_tx, internal_shutdown_rx) = mpsc::channel(1);
        let map_svc = MapService {
            handler: Arc::new(handler),
            shutdown_tx: internal_shutdown_tx,
            cancellation_token: cln_token.clone(),
        };

        let map_svc = proto::map_server::MapServer::new(map_svc)
            .max_encoding_message_size(self.max_message_size)
            .max_decoding_message_size(self.max_message_size);

        let shutdown = shutdown_signal(internal_shutdown_rx, Some(shutdown_rx));

        // will call cancel_token.cancel() on drop of _drop_guard
        let _drop_guard = cln_token.drop_guard();

        tonic::transport::Server::builder()
            .add_service(map_svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await?;

        Ok(())
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the signal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Mapper + Send + Sync + 'static,
    {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        self.start_with_shutdown(shutdown_rx).await
    }
}

impl<C> Drop for Server<C> {
    // Cleanup the socket file when the server is dropped so that when the server is restarted, it can bind to the
    // same address. UnixListener doesn't implement Drop trait, so we have to manually remove the socket file.
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.sock_addr);
    }
}

#[cfg(test)]
mod tests {
    use crate::map;
    use crate::map::proto::map_client::MapClient;
    use std::{error::Error, time::Duration};

    use crate::map::proto;
    use tempfile::TempDir;
    use tokio::net::UnixStream;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::Uri;
    use tower::service_fn;

    #[tokio::test]
    async fn map_server() -> Result<(), Box<dyn Error>> {
        struct Cat;
        #[tonic::async_trait]
        impl map::Mapper for Cat {
            async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
                vec![map::Message {
                    keys: Some(input.keys),
                    value: input.value,
                    tags: Some(vec![]),
                }]
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("map.sock");
        let server_info_file = tmp_dir.path().join("mapper-server-info");

        let mut server = map::Server::new(Cat)
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
        let request = proto::MapRequest {
            request: Some(proto::map_request::Request {
                keys: vec!["first".into(), "second".into()],
                value: "hello".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                headers: Default::default(),
            }),
            id: "".to_string(),
            handshake: None,
        };

        let (tx, rx) = mpsc::channel(2);
        let handshake_request = proto::MapRequest {
            request: None,
            id: "".to_string(),
            handshake: Some(proto::Handshake { sot: true }),
        };

        tx.send(handshake_request).await?;
        tx.send(request).await?;

        let resp = client.map_fn(ReceiverStream::new(rx)).await?;
        let mut resp = resp.into_inner();

        let handshake_response = resp.message().await?;
        assert!(handshake_response.is_some());

        let handshake_response = handshake_response.unwrap();
        assert!(handshake_response.handshake.is_some());

        let actual_response = resp.message().await?;
        assert!(actual_response.is_some());

        let actual_response = actual_response.unwrap();
        assert_eq!(
            actual_response.results.len(),
            1,
            "Expected single message from server"
        );
        let msg = &actual_response.results[0];
        assert_eq!(msg.keys.first(), Some(&"first".to_owned()));
        assert_eq!(msg.value, "hello".as_bytes());

        drop(tx);
        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");
        Ok(())
    }

    #[tokio::test]
    async fn map_server_panic() -> Result<(), Box<dyn Error>> {
        struct PanicMapper;
        #[tonic::async_trait]
        impl map::Mapper for PanicMapper {
            async fn map(&self, _: map::MapRequest) -> Vec<map::Message> {
                panic!("Panic in mapper");
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("map.sock");
        let server_info_file = tmp_dir.path().join("mapper-server-info");

        let mut server = map::Server::new(PanicMapper)
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

        let (tx, rx) = mpsc::channel(2);
        let handshake_request = proto::MapRequest {
            request: None,
            id: "".to_string(),
            handshake: Some(proto::Handshake { sot: true }),
        };
        tx.send(handshake_request).await.unwrap();

        let mut stream = tokio::time::timeout(
            Duration::from_secs(2),
            client.map_fn(ReceiverStream::new(rx)),
        )
        .await
        .map_err(|_| "timeout while getting stream for map_fn")??
        .into_inner();

        let handshake_resp = stream.message().await?.unwrap();
        assert!(
            handshake_resp.handshake.is_some(),
            "Not a valid response for handshake request"
        );

        let request = proto::MapRequest {
            request: Some(proto::map_request::Request {
                keys: vec!["three".into(), "four".into()],
                value: "hello".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                headers: Default::default(),
            }),
            id: "".to_string(),
            handshake: None,
        };
        tx.send(request).await.unwrap();

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

    // tests for panic when we have multiple inflight requests, only one of the requests
    // causes panic, the other requests should be processed successfully and the server
    // should shut down gracefully.
    #[tokio::test]
    async fn panic_with_multiple_requests() -> Result<(), Box<dyn Error>> {
        struct PanicMapper;
        #[tonic::async_trait]
        impl map::Mapper for PanicMapper {
            async fn map(&self, _: map::MapRequest) -> Vec<map::Message> {
                panic!("Panic in mapper");
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("map.sock");
        let server_info_file = tmp_dir.path().join("mapper-server-info");

        let mut server = map::Server::new(PanicMapper)
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
                let sock_file = sock_file.clone();
                async move {
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                        UnixStream::connect(sock_file).await?,
                    ))
                }
            }))
            .await?;

        let mut client = MapClient::new(channel);

        let (tx, rx) = mpsc::channel(2);
        let handshake_request = proto::MapRequest {
            request: None,
            id: "".to_string(),
            handshake: Some(proto::Handshake { sot: true }),
        };
        tx.send(handshake_request).await.unwrap();

        let mut stream = tokio::time::timeout(
            Duration::from_secs(2),
            client.map_fn(ReceiverStream::new(rx)),
        )
        .await
        .map_err(|_| "timeout while getting stream for map_fn")??
        .into_inner();

        let handshake_resp = stream.message().await?.unwrap();
        assert!(
            handshake_resp.handshake.is_some(),
            "Not a valid response for handshake request"
        );

        let request = proto::MapRequest {
            request: Some(proto::map_request::Request {
                keys: vec!["five".into(), "six".into()],
                value: "hello".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                headers: Default::default(),
            }),
            id: "".to_string(),
            handshake: None,
        };
        tx.send(request).await.unwrap();

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
