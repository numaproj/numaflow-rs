use chrono::{DateTime, Utc};
use std::collections::HashMap;

use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming, async_trait};

use tracing::{error, info};

use crate::error::{Error, ErrorKind};
use crate::proto::map::{self as proto, MapResponse};
use crate::shared;
use shared::{ContainerType, DROP, build_panic_status, get_panic_info};

/// Default socket address for map service
pub const SOCK_ADDR: &str = "/var/run/numaflow/map.sock";

/// Default server info file for map service
pub const SERVER_INFO_FILE: &str = "/var/run/numaflow/mapper-server-info";

/// Default channel size for map service
const CHANNEL_SIZE: usize = 1000;

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
    ///       let message=Message::new(input.value).with_keys(input.keys).with_tags(vec![]);
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
    /// use numaflow::map::Message;
    /// let message = Message::new(vec![1, 2, 3]).with_tags(vec!["tag1".to_string(), "tag2".to_string()]);
    /// ```
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = Some(tags);
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
            mpsc::channel::<Result<MapResponse, Status>>(CHANNEL_SIZE);

        // perform handshake
        perform_handshake(&mut stream, &stream_response_tx).await?;

        let (error_tx, error_rx) = mpsc::channel::<Error>(1);

        // Spawn a task to handle incoming stream requests
        let handle: JoinHandle<()> = tokio::spawn(handle_stream_requests(
            handler.clone(),
            stream,
            stream_response_tx.clone(),
            error_tx,
            self.cancellation_token.clone(),
        ));

        tokio::spawn(manage_grpc_stream(
            handle,
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
    stream_response_tx: mpsc::Sender<Result<MapResponse, Status>>,
    mut error_rx: mpsc::Receiver<Error>,
    server_shutdown_tx: mpsc::Sender<()>,
) {
    let err = match error_rx.recv().await {
        Some(err) => err,
        None => match request_handler.await {
            Ok(_) => return,
            Err(e) => Error::MapError(ErrorKind::InternalError(format!(
                "Map request handler aborted: {e:?}"
            ))),
        },
    };

    error!("Shutting down gRPC channel: {err:?}");
    stream_response_tx
        .send(Err(err.into_status()))
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
    tokio::spawn(run_map(handler, map_request, stream_response_tx, error_tx));
    true
}

async fn run_map<T>(
    handler: Arc<T>,
    map_request: proto::MapRequest,
    stream_response_tx: mpsc::Sender<Result<MapResponse, Status>>,
    error_tx: mpsc::Sender<Error>,
) where
    T: Mapper + Send + Sync + 'static,
{
    let request = map_request.request.expect("request can not be none");
    let message_id = map_request.id.clone();

    // A new task is spawned to catch the panic
    let udf_map_task = tokio::spawn({
        let handler = handler.clone();
        async move { handler.map(request.into()).await }
    });

    let messages = match udf_map_task.await {
        Ok(messages) => messages,
        Err(e) => {
            error!("Failed to run map function: {e:?}");

            // Check if we have detailed panic info from our hook
            if let Some(panic_info) = get_panic_info() {
                // This is a panic - send detailed panic information
                let status = build_panic_status(&panic_info);
                let _ = error_tx.send(Error::GrpcStatus(status)).await;
            } else {
                // This is a non-panic error
                let _ = error_tx
                    .send(Error::MapError(ErrorKind::InternalError(format!(
                        "Map task execution failed: {e:?}",
                    ))))
                    .await;
            }
            return;
        }
    };

    let send_response_result = stream_response_tx
        .send(Ok(MapResponse {
            results: messages.into_iter().map(|msg| msg.into()).collect(),
            id: message_id,
            handshake: None,
            status: None,
        }))
        .await;

    let Err(e) = send_response_result else {
        return;
    };

    let _ = error_tx
        .send(Error::MapError(ErrorKind::InternalError(format!(
            "Failed to send response: {e:?}"
        ))))
        .await;
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
                status: None,
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
    pub fn new(map_svc: T) -> Self {
        Self {
            inner: shared::Server::new(map_svc, ContainerType::Map, SOCK_ADDR, SERVER_INFO_FILE),
        }
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown(
        self,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Mapper + Send + Sync + 'static,
    {
        self.inner
            .start_with_shutdown(
                shutdown_rx,
                |handler, max_message_size, shutdown_tx, cln_token| {
                    let map_svc = MapService {
                        handler: Arc::new(handler),
                        shutdown_tx,
                        cancellation_token: cln_token,
                    };

                    let map_svc = proto::map_server::MapServer::new(map_svc)
                        .max_encoding_message_size(max_message_size)
                        .max_decoding_message_size(max_message_size);

                    tonic::transport::Server::builder().add_service(map_svc)
                },
            )
            .await
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the signal arrives.
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Mapper + Send + Sync + 'static,
    {
        self.inner
            .start(|handler, max_message_size, shutdown_tx, cln_token| {
                let map_svc = MapService {
                    handler: Arc::new(handler),
                    shutdown_tx,
                    cancellation_token: cln_token,
                };

                let map_svc = proto::map_server::MapServer::new(map_svc)
                    .max_encoding_message_size(max_message_size)
                    .max_decoding_message_size(max_message_size);

                tonic::transport::Server::builder().add_service(map_svc)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};

    use crate::shared::ServerExtras;
    use tempfile::TempDir;
    use tokio::net::UnixStream;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::Uri;
    use tower::service_fn;

    use crate::map;
    use crate::proto::map as proto;
    use crate::proto::map::map_client::MapClient;

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

        let server = map::Server::new(Cat)
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
                metadata: None,
            }),
            id: "".to_string(),
            handshake: None,
            status: None,
        };

        let (tx, rx) = mpsc::channel(2);
        let handshake_request = proto::MapRequest {
            request: None,
            id: "".to_string(),
            handshake: Some(proto::Handshake { sot: true }),
            status: None,
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

    #[cfg(feature = "test-panic")]
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

        let server = map::Server::new(PanicMapper)
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
            status: None,
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
                metadata: None,
            }),
            id: "".to_string(),
            handshake: None,
            status: None,
        };
        tx.send(request).await.unwrap();

        if let Err(e) = stream.message().await {
            assert_eq!(e.code(), tonic::Code::Internal);
            // Check for enhanced panic error message
            assert!(
                e.message().contains("UDF_EXECUTION_ERROR")
                    || e.message().contains("Panic in mapper"),
                "Expected enhanced panic message, got: {}",
                e.message()
            );
        } else {
            return Err("Expected error from server".into());
        }

        drop(tx);
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
    #[cfg(feature = "test-panic")]
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

        let server = map::Server::new(PanicMapper)
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
            status: None,
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
                metadata: None,
            }),
            id: "".to_string(),
            handshake: None,
            status: None,
        };
        tx.send(request).await.unwrap();

        if let Err(e) = stream.message().await {
            assert_eq!(e.code(), tonic::Code::Internal);
            // Check for enhanced panic error message
            assert!(
                e.message().contains("UDF_EXECUTION_ERROR")
                    || e.message().contains("Panic in mapper"),
                "Expected enhanced panic message, got: {}",
                e.message()
            );
        } else {
            return Err("Expected error from server".into());
        }

        drop(tx);
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
