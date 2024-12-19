use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{async_trait, Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::error::{Error, ErrorKind};
use crate::servers::map as proto;
use crate::servers::map::TransmissionStatus;
use crate::shared::{self, shutdown_signal, ContainerType};

const DEFAULT_CHANNEL_SIZE: usize = 1000;
const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_SOCK_ADDR: &str = "/var/run/numaflow/mapstream.sock";
const DEFAULT_SERVER_INFO_FILE: &str = "/var/run/numaflow/mapper-server-info";

const DROP: &str = "U+005C__DROP__";

/// MapStreamer trait for implementing MapStream handler.
#[async_trait]
pub trait MapStreamer {
    /// The `map_stream` function processes each incoming message and streams the result back using a channel.
    ///
    /// # Arguments
    ///
    /// * `input` - The input request containing keys, value, event time, watermark, and headers.
    /// * `tx` - The channel to send the resulting messages.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use tokio::sync::mpsc;
    /// use tonic::async_trait;
    /// use std::collections::HashMap;
    /// use chrono::{DateTime, Utc};
    /// use tokio::sync::mpsc::Sender;
    /// use numaflow::mapstream::{MapStreamRequest, MapStreamer, Message};
    ///
    /// struct ExampleMapStreamer;
    ///
    /// #[async_trait]
    /// impl MapStreamer for ExampleMapStreamer {
    ///     async fn map_stream(&self, input: MapStreamRequest, tx: Sender<Message>) {
    ///         let payload_str = String::from_utf8(input.value).unwrap_or_default();
    ///         let splits: Vec<&str> = payload_str.split(',').collect();
    ///
    ///         for split in splits {
    ///             let message = Message::new(split.as_bytes().to_vec())
    ///                 .keys(input.keys.clone())
    ///                 .tags(vec![]);
    ///             if tx.send(message).await.is_err() {
    ///                 break;
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    async fn map_stream(&self, input: MapStreamRequest, tx: Sender<Message>);
}

/// Message is the response struct from the [`MapStreamer::map_stream`] .
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

/// Incoming request into the map handler of [`MapStreamer`].
pub struct MapStreamRequest {
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

impl From<proto::map_request::Request> for MapStreamRequest {
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

struct MapStreamService<T> {
    handler: Arc<T>,
    shutdown_tx: mpsc::Sender<()>,
    cancellation_token: CancellationToken,
}

#[async_trait]
impl<T> proto::map_server::Map for MapStreamService<T>
where
    T: MapStreamer + Send + Sync + 'static,
{
    type MapFnStream = ReceiverStream<Result<proto::MapResponse, Status>>;

    async fn map_fn(
        &self,
        request: Request<Streaming<proto::MapRequest>>,
    ) -> Result<Response<Self::MapFnStream>, Status> {
        let mut stream = request.into_inner();
        let handler = Arc::clone(&self.handler);

        let (stream_response_tx, stream_response_rx) =
            mpsc::channel::<Result<proto::MapResponse, Status>>(DEFAULT_CHANNEL_SIZE);

        // Perform handshake
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

/// Handles incoming stream requests and processes them and
/// sends the response back to the client.
async fn handle_stream_requests<T>(
    handler: Arc<T>,
    mut stream: Streaming<proto::MapRequest>,
    stream_response_tx: Sender<Result<proto::MapResponse, Status>>,
    error_tx: Sender<Error>,
    token: CancellationToken,
) where
    T: MapStreamer + Send + Sync + 'static,
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

/// Handles a single request from the client. If the request is invalid or the stream is closed,
/// it returns false. Otherwise, it spawns a task to handle the request, and
/// streams the response back to the client.
async fn handle_request<T>(
    handler: Arc<T>,
    map_request: Result<Option<proto::MapRequest>, Status>,
    stream_response_tx: mpsc::Sender<Result<proto::MapResponse, Status>>,
    error_tx: mpsc::Sender<Error>,
    token: CancellationToken,
) -> bool
where
    T: MapStreamer + Send + Sync + 'static,
{
    let map_request = match map_request {
        Ok(None) => return false,
        Ok(Some(val)) => val,
        Err(val) => {
            error!("Received gRPC error from sender: {val:?}");
            return false;
        }
    };
    tokio::spawn(run_map_stream(
        handler,
        map_request,
        stream_response_tx,
        error_tx,
        token,
    ));
    true
}

/// Runs the map_stream function of the handler and streams the response back to the client.
async fn run_map_stream<T>(
    handler: Arc<T>,
    map_request: proto::MapRequest,
    stream_response_tx: Sender<Result<proto::MapResponse, Status>>,
    error_tx: Sender<Error>,
    token: CancellationToken,
) where
    T: MapStreamer + Send + Sync + 'static,
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

    let (tx, mut rx) = mpsc::channel::<Message>(DEFAULT_CHANNEL_SIZE);

    // Spawn a task to run the map_stream function
    let token = token.child_token();
    let map_stream_task = tokio::spawn({
        let handler = handler.clone();
        let token = token.clone();
        async move {
            tokio::select! {
                _ = token.cancelled() => {
                    info!("Task was cancelled");
                }
                _ = handler.map_stream(request.into(), tx) => {}
            }
        }
    });

    // Wait for the map_stream_task to complete and handle any errors
    let panic_listener = tokio::spawn({
        let error_tx = error_tx.clone();
        async move {
            if let Err(e) = map_stream_task.await {
                error_tx
                    .send(Error::MapError(ErrorKind::InternalError(format!(
                        "Task panicked: {e:?}"
                    ))))
                    .await
                    .expect("Sending error on channel");
                return Err(e);
            }
            Ok(())
        }
    })
    .await;

    while let Some(message) = rx.recv().await {
        let send_response_result = stream_response_tx
            .send(Ok(proto::MapResponse {
                results: vec![message.into()],
                id: message_id.clone(),
                handshake: None,
                status: None,
            }))
            .await;

        if let Err(e) = send_response_result {
            error_tx
                .send(Error::MapError(ErrorKind::InternalError(format!(
                    "Failed to send response: {e:?}"
                ))))
                .await
                .expect("Sending error on channel");
            return;
        }
    }

    // we should not end eof message if the map stream panicked
    if panic_listener.is_err() {
        return;
    }

    // send eof message to indicate end of stream
    stream_response_tx
        .send(Ok(proto::MapResponse {
            results: vec![],
            id: message_id,
            handshake: None,
            status: Some(TransmissionStatus { eot: true }),
        }))
        .await
        .expect("Sending eof message to gRPC response channel");
}

/// Manages the gRPC stream. If the request handler is finished, it cancels the token.
async fn manage_grpc_stream(
    request_handler: JoinHandle<()>,
    token: CancellationToken,
    stream_response_tx: Sender<Result<proto::MapResponse, Status>>,
    mut error_rx: mpsc::Receiver<Error>,
    server_shutdown_tx: Sender<()>,
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

/// Performs the handshake with the client.
async fn perform_handshake(
    stream: &mut Streaming<proto::MapRequest>,
    stream_response_tx: &mpsc::Sender<Result<proto::MapResponse, Status>>,
) -> Result<(), Status> {
    let handshake_request = stream
        .message()
        .await
        .map_err(|e| Status::internal(format!("Handshake failed: {}", e)))?
        .ok_or_else(|| Status::internal("Stream closed before handshake"))?;

    if let Some(handshake) = handshake_request.handshake {
        stream_response_tx
            .send(Ok(proto::MapResponse {
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

/// gRPC server to start a map stream service
#[derive(Debug)]
pub struct Server<T> {
    sock_addr: PathBuf,
    max_message_size: usize,
    server_info_file: PathBuf,
    svc: Option<T>,
}

impl<T> Server<T> {
    pub fn new(map_stream_svc: T) -> Self {
        Server {
            sock_addr: DEFAULT_SOCK_ADDR.into(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            server_info_file: DEFAULT_SERVER_INFO_FILE.into(),
            svc: Some(map_stream_svc),
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
        T: MapStreamer + Send + Sync + 'static,
    {
        let info = shared::ServerInfo::new(ContainerType::MapStream);
        let listener =
            shared::create_listener_stream(&self.sock_addr, &self.server_info_file, info)?;
        let handler = self.svc.take().unwrap();
        let cln_token = CancellationToken::new();

        let (internal_shutdown_tx, internal_shutdown_rx) = mpsc::channel(1);
        let map_stream_svc = MapStreamService {
            handler: Arc::new(handler),
            shutdown_tx: internal_shutdown_tx,
            cancellation_token: cln_token.clone(),
        };

        let map_stream_svc = proto::map_server::MapServer::new(map_stream_svc)
            .max_encoding_message_size(self.max_message_size)
            .max_decoding_message_size(self.max_message_size);

        let shutdown = shutdown_signal(internal_shutdown_rx, Some(shutdown_rx));

        let _drop_guard = cln_token.drop_guard();

        tonic::transport::Server::builder()
            .add_service(map_stream_svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await?;

        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: MapStreamer + Send + Sync + 'static,
    {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        self.start_with_shutdown(shutdown_rx).await
    }
}

/// Remove the socket file and server info file
impl<C> Drop for Server<C> {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.sock_addr);
        let _ = fs::remove_file(&self.server_info_file);
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};

    use tempfile::TempDir;
    use tokio::net::UnixStream;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::Uri;
    use tower::service_fn;

    use super::*;
    use crate::servers::map::map_client::MapClient;

    #[tokio::test]
    async fn map_stream_single_response() -> Result<(), Box<dyn Error>> {
        struct Cat;
        #[async_trait]
        impl MapStreamer for Cat {
            async fn map_stream(&self, input: MapStreamRequest, tx: Sender<Message>) {
                let message = Message::new(input.value).keys(input.keys).tags(vec![]);
                tx.send(message).await.unwrap();
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("mapstream.sock");
        let server_info_file = tmp_dir.path().join("mapstream-server-info");

        let mut server = Server::new(Cat)
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file)
            .with_max_message_size(10240);

        assert_eq!(server.max_message_size(), 10240);
        assert_eq!(server.server_info_file(), server_info_file);
        assert_eq!(server.socket_file(), sock_file);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

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
        let request = proto::MapRequest {
            request: Some(proto::map_request::Request {
                keys: vec!["map".into(), "stream".into()],
                value: "hello".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                headers: Default::default(),
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
        assert_eq!(msg.keys.first(), Some(&"map".to_owned()));
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
    async fn map_stream_multi_response() -> Result<(), Box<dyn Error>> {
        struct StreamCat;

        #[async_trait]
        impl MapStreamer for StreamCat {
            async fn map_stream(&self, request: MapStreamRequest, sender: Sender<Message>) {
                let value = String::from_utf8(request.value).unwrap();
                for part in value.split(',') {
                    let message = Message::new(part.as_bytes().to_vec());
                    sender.send(message).await.unwrap();
                }
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("map_stream.sock");
        let server_info_file = tmp_dir.path().join("mapper-stream-server-info");

        let mut server = Server::new(StreamCat)
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file)
            .with_max_message_size(10240);

        assert_eq!(server.max_message_size(), 10240);
        assert_eq!(server.server_info_file(), server_info_file);
        assert_eq!(server.socket_file(), sock_file);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

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
        let request = proto::MapRequest {
            request: Some(proto::map_request::Request {
                keys: vec!["first".into()],
                value: "test,map,stream".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                headers: Default::default(),
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

        for expected_value in &["test", "map", "stream"] {
            let actual_response = resp.message().await?;
            assert!(actual_response.is_some());

            let actual_response = actual_response.unwrap();
            assert_eq!(
                actual_response.results.len(),
                1,
                "Expected single message from server"
            );
            let msg = &actual_response.results[0];
            assert_eq!(msg.value, expected_value.as_bytes());
        }

        // read eof response
        let eof_response = resp.message().await?;
        assert!(eof_response.is_some());
        assert!(eof_response.unwrap().status.is_some());

        drop(tx);
        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");
        Ok(())
    }

    #[tokio::test]
    async fn map_stream_server_panic() -> Result<(), Box<dyn Error>> {
        struct PanicStreamer;
        #[async_trait]
        impl MapStreamer for PanicStreamer {
            async fn map_stream(&self, _: MapStreamRequest, _tx: Sender<Message>) {
                panic!("Panic in Map Stream");
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("mapstream.sock");
        let server_info_file = tmp_dir.path().join("mapstream-server-info");

        let mut server = Server::new(PanicStreamer)
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file)
            .with_max_message_size(10240);

        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

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
                keys: vec!["panic".into(), "stream".into()],
                value: "hello".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                headers: Default::default(),
            }),
            id: "".to_string(),
            handshake: None,
            status: None,
        };

        tx.send(request).await.unwrap();

        if let Err(e) = stream.message().await {
            assert_eq!(e.code(), tonic::Code::Internal);
            assert!(e.message().contains("panicked"));
        } else {
            return Err("Expected error from server".into());
        }

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
