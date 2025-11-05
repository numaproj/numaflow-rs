use chrono::{DateTime, Utc};
use proto::SourceTransformResponse;
use std::collections::HashMap;

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming, async_trait};
use tracing::{error, info};

use crate::error::{Error, ErrorKind};
use crate::proto::metadata as metadata_pb;
use crate::proto::source_transformer as proto;
use crate::shared;

use shared::{
    ContainerType, DROP, build_panic_status, get_panic_info, prost_timestamp_from_utc,
    utc_from_timestamp,
};

/// Default socket address for source transformer service
pub const SOCK_ADDR: &str = "/var/run/numaflow/sourcetransform.sock";

/// Default server info file for source transformer service
pub const SERVER_INFO_FILE: &str = "/var/run/numaflow/sourcetransformer-server-info";

/// Default channel size for source transformer service
const CHANNEL_SIZE: usize = 1000;

/// SystemMetadata is mapping of group name to key-value pairs
/// SystemMetadata wraps system-generated metadata groups per message.
/// It is read-only to UDFs
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SystemMetadata {
    data: HashMap<String, HashMap<String, Vec<u8>>>,
}

impl SystemMetadata {
    /// Create a new SystemMetadata instance
    /// This is for internal and testing purposes only.
    pub fn new() -> Self {
        Self::default()
    }

    /// groups returns the groups of the system metadata.
    /// If there are no groups, it returns an empty vector.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use numaflow::sourcetransform::SourceTransformRequest;
    /// # let input: SourceTransformRequest = unimplemented!();
    /// let smd = input.system_metadata;
    /// let groups = smd.groups();
    /// println!("System metadata groups: {:?}", groups);
    /// ```
    pub fn groups(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    /// keys returns the keys of the system metadata for the given group.
    /// If there are no keys or the group is not present, it returns an empty vector.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use numaflow::sourcetransform::SourceTransformRequest;
    /// # let input: SourceTransformRequest = unimplemented!();
    /// let smd = input.system_metadata;
    /// let keys = smd.keys("system-group");
    /// println!("Keys in system-group: {:?}", keys);
    /// ```
    pub fn keys(&self, group: &str) -> Vec<String> {
        self.data
            .get(group)
            .map(|kv| kv.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// value returns the value of the system metadata for the given group and key.
    /// If there is no value or the group or key is not present, it returns an empty vector.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use numaflow::sourcetransform::SourceTransformRequest;
    /// # let input: SourceTransformRequest = unimplemented!();
    /// let smd = input.system_metadata;
    /// let value = smd.value("system-group", "system-key");
    /// println!("Value: {:?}", value);
    /// ```
    pub fn value(&self, group: &str, key: &str) -> Vec<u8> {
        self.data
            .get(group)
            .and_then(|kv| kv.get(key))
            .cloned()
            .unwrap_or_default()
    }
}

/// UserMetadata wraps user-defined metadata groups per message.
#[derive(Debug, Clone, PartialEq, Default)]
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
    /// # use numaflow::sourcetransform::SourceTransformRequest;
    /// # let input: SourceTransformRequest = unimplemented!();
    /// let umd = input.user_metadata;
    /// let groups = umd.groups();
    /// println!("User metadata groups: {:?}", groups);
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
    /// # use numaflow::sourcetransform::SourceTransformRequest;
    /// # let input: SourceTransformRequest = unimplemented!();
    /// let umd = input.user_metadata;
    /// let keys = umd.keys("my-group");
    /// println!("Keys in my-group: {:?}", keys);
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
    /// # use numaflow::sourcetransform::SourceTransformRequest;
    /// # let input: SourceTransformRequest = unimplemented!();
    /// let umd = input.user_metadata;
    /// let value = umd.value("my-group", "my-key");
    /// println!("Value: {:?}", value);
    /// ```
    pub fn value(&self, group: &str, key: &str) -> Vec<u8> {
        self.data
            .get(group)
            .and_then(|kv| kv.get(key))
            .cloned()
            .unwrap_or_default()
    }

    /// create_group creates a new group in the user metadata.
    /// If the group is not present, it's a no-op.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use numaflow::sourcetransform::UserMetadata;
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
    /// use numaflow::sourcetransform::UserMetadata;
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
    /// use numaflow::sourcetransform::UserMetadata;
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
    /// use numaflow::sourcetransform::UserMetadata;
    /// let mut umd = UserMetadata::new();
    /// umd.create_group("group1".to_string());
    /// umd.remove_group("group1");
    /// println!("{:?}", umd);
    /// ```
    pub fn remove_group(&mut self, group: &str) {
        self.data.remove(group);
    }
}

struct SourceTransformerService<T> {
    handler: Arc<T>,
    shutdown_tx: mpsc::Sender<()>,
    cancellation_token: CancellationToken,
}

/// SourceTransformer trait for implementing SourceTransform handler.
#[async_trait]
pub trait SourceTransformer {
    /// transform takes in an input element and can produce 0, 1, or more results. The input is a [`SourceTransformRequest`]
    /// and the output is a [`Vec`] of [`Message`]. In a `transform` each element is processed independently
    /// and there is no state associated with the elements. Source transformer can be used for transforming
    /// and assigning event time to input messages. More about source transformer can be read
    /// [here](https://numaflow.numaproj.io/user-guide/sources/transformer/overview/)
    ///
    /// #Example
    ///
    /// ```no_run
    /// use numaflow::sourcetransform;
    /// use std::error::Error;
    ///
    /// // A simple source transformer which assigns event time to the current time in utc.
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    ///     sourcetransform::Server::new(NowCat).start().await
    /// }
    ///
    /// struct NowCat;
    ///
    /// #[tonic::async_trait]
    /// impl sourcetransform::SourceTransformer for NowCat {
    ///     async fn transform(
    ///         &self,
    ///         input: sourcetransform::SourceTransformRequest,
    ///     ) -> Vec<sourcetransform::Message> {
    ///         use numaflow::sourcetransform::Message;
    ///         let message=Message::new(input.value, chrono::offset::Utc::now()).with_keys(input.keys).with_tags(vec![]);
    ///        vec![message]
    ///     }
    /// }
    /// ```
    async fn transform(&self, input: SourceTransformRequest) -> Vec<Message>;
}

/// Message is the response struct from the [`SourceTransformer::transform`] .
#[derive(Debug, PartialEq)]
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection.
    pub keys: Option<Vec<String>>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Time for the given event. This will be used for tracking watermarks. If cannot be derived, set it to the incoming
    /// event_time from the [`SourceTransformRequest`].
    pub event_time: DateTime<Utc>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags: Option<Vec<String>>,
    /// User metadata for the message.
    pub user_metadata: Option<UserMetadata>,
}

/// Represents a message that can be modified and forwarded.
impl Message {
    /// Creates a new message with the specified value and event time.
    ///
    /// This constructor initializes the message with no keys, tags.
    ///
    /// # Arguments
    ///
    /// * `value` - A vector of bytes representing the message's payload.
    /// * `event_time` - The `DateTime<Utc>` that specifies when the event occurred.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::sourcetransform::Message;
    /// use chrono::Utc;
    /// let now = Utc::now();
    /// let message = Message::new(vec![1, 2, 3, 4], now);
    /// ```
    pub fn new(value: Vec<u8>, event_time: DateTime<Utc>) -> Self {
        Self {
            value,
            event_time,
            keys: None,
            tags: None,
            user_metadata: None,
        }
    }
    /// Marks the message to be dropped by creating a new `Message` with an empty value, a special "DROP" tag, and the specified event time.
    ///
    /// # Arguments
    ///
    /// * `event_time` - The `DateTime<Utc>` that specifies when the event occurred. Event time is required because, even though a message is dropped,
    ///   it is still considered as being processed, hence the watermark should be updated accordingly using the provided event time.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::sourcetransform::Message;
    /// use chrono::Utc;
    /// let now = Utc::now();
    /// let dropped_message = Message::message_to_drop(now);
    /// ```
    pub fn message_to_drop(event_time: DateTime<Utc>) -> Message {
        Message {
            keys: None,
            value: vec![],
            event_time,
            tags: Some(vec![DROP.to_string()]),
            user_metadata: None,
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
    /// use numaflow::sourcetransform::Message;
    /// use chrono::Utc;
    /// let now = Utc::now();
    /// let message = Message::new(vec![1, 2, 3], now).with_keys(vec!["key1".to_string(), "key2".to_string()]);
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
    /// use numaflow::sourcetransform::Message;
    /// use chrono::Utc;
    /// let now = Utc::now();
    /// let message = Message::new(vec![1, 2, 3], now).with_tags(vec!["tag1".to_string(), "tag2".to_string()]);
    /// ```
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Sets the user metadata for the message.
    pub fn with_user_metadata(mut self, user_metadata: UserMetadata) -> Self {
        self.user_metadata = Some(user_metadata);
        self
    }
}

/// Incoming request to the Source Transformer.
pub struct SourceTransformRequest {
    /// keys are the keys in the (key, value) terminology of map/reduce paradigm.
    pub keys: Vec<String>,
    /// value is the value in (key, value) terminology of map/reduce paradigm.
    pub value: Vec<u8>,
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this
    /// time.
    pub watermark: DateTime<Utc>,
    /// event_time is the time of the element as seen at source or aligned after a reduce operation.
    pub eventtime: DateTime<Utc>,
    /// Headers for the message.
    pub headers: HashMap<String, String>,
    /// User metadata for the message.
    pub user_metadata: UserMetadata,
    /// System metadata for the message.
    pub system_metadata: SystemMetadata,
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

impl From<Message> for proto::source_transform_response::Result {
    fn from(value: Message) -> Self {
        proto::source_transform_response::Result {
            keys: value.keys.unwrap_or_default(),
            value: value.value,
            event_time: prost_timestamp_from_utc(value.event_time),
            tags: value.tags.unwrap_or_default(),
            metadata: Some(to_proto(value.user_metadata.as_ref())),
        }
    }
}

/// Get UserMetadata from proto Metadata
fn user_metadata_from_proto(proto: Option<&metadata_pb::Metadata>) -> UserMetadata {
    let proto = match proto {
        Some(p) => p,
        None => return UserMetadata::new(),
    };

    let mut user_map = HashMap::new();
    for (group, kv_group) in &proto.user_metadata {
        user_map.insert(group.clone(), kv_group.key_value.clone());
    }

    UserMetadata { data: user_map }
}

/// Get SystemMetadata from proto Metadata
fn system_metadata_from_proto(proto: Option<&metadata_pb::Metadata>) -> SystemMetadata {
    let proto = match proto {
        Some(p) => p,
        None => return SystemMetadata::new(),
    };

    let mut sys_map = HashMap::new();
    for (group, kv_group) in &proto.sys_metadata {
        sys_map.insert(group.clone(), kv_group.key_value.clone());
    }

    SystemMetadata { data: sys_map }
}

impl From<proto::source_transform_request::Request> for SourceTransformRequest {
    fn from(request: proto::source_transform_request::Request) -> Self {
        let user_metadata = user_metadata_from_proto(request.metadata.as_ref());
        let system_metadata = system_metadata_from_proto(request.metadata.as_ref());

        SourceTransformRequest {
            keys: request.keys,
            value: request.value,
            watermark: utc_from_timestamp(request.watermark),
            eventtime: utc_from_timestamp(request.event_time),
            headers: request.headers,
            user_metadata,
            system_metadata,
        }
    }
}

#[async_trait]
impl<T> proto::source_transform_server::SourceTransform for SourceTransformerService<T>
where
    T: SourceTransformer + Send + Sync + 'static,
{
    type SourceTransformFnStream = ReceiverStream<Result<SourceTransformResponse, Status>>;

    async fn source_transform_fn(
        &self,
        request: Request<Streaming<proto::SourceTransformRequest>>,
    ) -> Result<Response<Self::SourceTransformFnStream>, Status> {
        let mut stream = request.into_inner();
        let handler = Arc::clone(&self.handler);

        let (stream_response_tx, stream_response_rx) =
            mpsc::channel::<Result<SourceTransformResponse, Status>>(CHANNEL_SIZE);

        // do the handshake first to let the client know that we are ready to receive transformation requests.
        perform_handshake(&mut stream, &stream_response_tx).await?;

        let (error_tx, error_rx) = mpsc::channel::<Error>(1);

        // Spawn a task to continuously receive messages from the client over the gRPC stream.
        // For each message received from the stream, a new task is spawned to call the transform function and send the response back to the client
        let handle: JoinHandle<()> = tokio::spawn(handle_stream_requests(
            handler.clone(),
            stream,
            stream_response_tx.clone(),
            error_tx,
            self.cancellation_token.child_token(),
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

async fn perform_handshake(
    stream: &mut Streaming<proto::SourceTransformRequest>,
    stream_response_tx: &mpsc::Sender<Result<SourceTransformResponse, Status>>,
) -> Result<(), Status> {
    let handshake_request = stream
        .message()
        .await
        .map_err(|e| Status::internal(format!("Handshake failed: {}", e)))?
        .ok_or_else(|| Status::internal("Stream closed before handshake"))?;

    if let Some(handshake) = handshake_request.handshake {
        stream_response_tx
            .send(Ok(SourceTransformResponse {
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

// shutdown the gRPC server on first error
async fn manage_grpc_stream(
    request_handler: JoinHandle<()>,
    stream_response_tx: mpsc::Sender<Result<SourceTransformResponse, Status>>,
    mut error_rx: mpsc::Receiver<Error>,
    server_shutdown_tx: mpsc::Sender<()>,
) {
    let err = match error_rx.recv().await {
        Some(err) => err,
        None => match request_handler.await {
            Ok(_) => return,
            Err(e) => Error::SourceTransformerError(ErrorKind::InternalError(format!(
                "Source transformer request handler aborted: {e:?}"
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

// Receives messages from the stream. For each message received from the stream,
// a new task is spawned to call the transform function and send the response back to the client
async fn handle_stream_requests<T>(
    handler: Arc<T>,
    mut stream: Streaming<proto::SourceTransformRequest>,
    stream_response_tx: mpsc::Sender<Result<SourceTransformResponse, Status>>,
    error_tx: mpsc::Sender<Error>,
    token: CancellationToken,
) where
    T: SourceTransformer + Send + Sync + 'static,
{
    let mut stream_open = true;
    while stream_open {
        stream_open = tokio::select! {
            transform_request = stream.message() => handle_request(
                handler.clone(),
                transform_request,
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

// The return boolean value indicates whether a task was created to handle the request.
// If the return value is false, either client sent an error gRPC status or the stream was closed.
async fn handle_request<T>(
    handler: Arc<T>,
    transform_request: Result<Option<proto::SourceTransformRequest>, Status>,
    stream_response_tx: mpsc::Sender<Result<SourceTransformResponse, Status>>,
    error_tx: mpsc::Sender<Error>,
) -> bool
where
    T: SourceTransformer + Send + Sync + 'static,
{
    let transform_request = match transform_request {
        Ok(None) => return false,
        Ok(Some(val)) => val,
        Err(val) => {
            error!("Received gRPC error from sender: {val:?}");
            return false;
        }
    };
    tokio::spawn(run_transform(
        handler,
        transform_request,
        stream_response_tx,
        error_tx,
    ));
    true
}

// Calls the user implemented transform function on the request.
async fn run_transform<T>(
    handler: Arc<T>,
    transform_request: proto::SourceTransformRequest,
    stream_response_tx: mpsc::Sender<Result<SourceTransformResponse, Status>>,
    error_tx: mpsc::Sender<Error>,
) where
    T: SourceTransformer + Send + Sync + 'static,
{
    let request = transform_request.request.expect("request can not be none");
    let message_id = request.id.clone();

    // A new task is spawned to catch the panic
    let udf_transform_task = tokio::spawn({
        let handler = handler.clone();
        async move { handler.transform(request.into()).await }
    });

    let messages = match udf_transform_task.await {
        Ok(messages) => messages,
        Err(e) => {
            error!("Failed to run transform function: {e:?}");

            // Check if this is a panic or a regular error
            if let Some(panic_info) = get_panic_info() {
                // This is a panic - send detailed panic information
                let status = build_panic_status(&panic_info);
                let _ = error_tx.send(Error::GrpcStatus(status)).await;
            } else {
                // This is a non-panic error
                let _ = error_tx
                    .send(Error::SourceTransformerError(ErrorKind::InternalError(
                        format!("Transform task execution failed: {e:?}"),
                    )))
                    .await;
            }
            return;
        }
    };

    let send_response_result = stream_response_tx
        .send(Ok(SourceTransformResponse {
            results: messages.into_iter().map(|msg| msg.into()).collect(),
            id: message_id,
            handshake: None,
        }))
        .await;

    let Err(e) = send_response_result else {
        return;
    };

    let _ = error_tx
        .send(Error::SourceTransformerError(ErrorKind::InternalError(
            format!("sending source transform response over gRPC stream: {e:?}"),
        )))
        .await;
}

/// gRPC server to start a sourcetransform service
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
    pub fn new(sourcetransformer_svc: T) -> Self {
        Self {
            inner: shared::Server::new(
                sourcetransformer_svc,
                ContainerType::SourceTransformer,
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
        T: SourceTransformer + Send + Sync + 'static,
    {
        self.inner
            .start_with_shutdown(
                shutdown_rx,
                |handler, max_message_size, shutdown_tx, cln_token| {
                    let sourcetrf_svc = SourceTransformerService {
                        handler: Arc::new(handler),
                        shutdown_tx,
                        cancellation_token: cln_token,
                    };

                    let sourcetrf_svc =
                        proto::source_transform_server::SourceTransformServer::new(sourcetrf_svc)
                            .max_encoding_message_size(max_message_size)
                            .max_decoding_message_size(max_message_size);

                    tonic::transport::Server::builder().add_service(sourcetrf_svc)
                },
            )
            .await
    }

    /// Starts the gRPC server. Automatically registers singal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the singal arrives.
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: SourceTransformer + Send + Sync + 'static,
    {
        self.inner
            .start(|handler, max_message_size, shutdown_tx, cln_token| {
                let sourcetrf_svc = SourceTransformerService {
                    handler: Arc::new(handler),
                    shutdown_tx,
                    cancellation_token: cln_token,
                };

                let sourcetrf_svc =
                    proto::source_transform_server::SourceTransformServer::new(sourcetrf_svc)
                        .max_encoding_message_size(max_message_size)
                        .max_decoding_message_size(max_message_size);

                tonic::transport::Server::builder().add_service(sourcetrf_svc)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::shared::ServerExtras;
    use chrono::Utc;
    use std::{error::Error, time::Duration};
    use tempfile::TempDir;
    use tokio::net::UnixStream;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::Uri;
    use tower::service_fn;

    use crate::proto::source_transformer::{
        self as proto, source_transform_client::SourceTransformClient,
    };
    use crate::sourcetransform::{self};

    #[tokio::test]
    async fn source_transformer_server() -> Result<(), Box<dyn Error>> {
        struct NowCat;
        #[tonic::async_trait]
        impl sourcetransform::SourceTransformer for NowCat {
            async fn transform(
                &self,
                input: sourcetransform::SourceTransformRequest,
            ) -> Vec<sourcetransform::Message> {
                vec![sourcetransform::Message {
                    keys: Some(input.keys),
                    value: input.value,
                    tags: Some(vec![]),
                    event_time: Utc::now(),
                    user_metadata: None,
                }]
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server = sourcetransform::Server::new(NowCat)
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

        let mut client = SourceTransformClient::new(channel);

        let (tx, rx) = mpsc::channel(2);

        let handshake_request = proto::SourceTransformRequest {
            request: None,
            handshake: Some(proto::Handshake { sot: true }),
        };
        tx.send(handshake_request).await.unwrap();

        let mut stream = tokio::time::timeout(
            Duration::from_secs(2),
            client.source_transform_fn(ReceiverStream::new(rx)),
        )
        .await
        .map_err(|_| "timeout while getting stream for source_transform_fn")??
        .into_inner();

        let handshake_resp = stream.message().await?.unwrap();
        assert!(
            handshake_resp.results.is_empty(),
            "The handshake response should not contain any messages"
        );
        assert!(
            handshake_resp.id.is_empty(),
            "The message id of the handshake response should be empty"
        );
        assert!(
            handshake_resp.handshake.is_some(),
            "Not a valid response for handshake request"
        );

        let request = proto::SourceTransformRequest {
            request: Some(proto::source_transform_request::Request {
                id: "1".to_string(),
                keys: vec!["first".into(), "second".into()],
                value: "hello".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                headers: Default::default(),
                metadata: None,
            }),
            handshake: None,
        };

        tx.send(request).await.unwrap();

        let resp = stream.message().await?.unwrap();
        assert_eq!(resp.results.len(), 1, "Expected single message from server");
        let msg = &resp.results[0];
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
    async fn source_transformer_panic() -> Result<(), Box<dyn Error>> {
        struct PanicTransformer;
        #[tonic::async_trait]
        impl sourcetransform::SourceTransformer for PanicTransformer {
            async fn transform(
                &self,
                _: sourcetransform::SourceTransformRequest,
            ) -> Vec<sourcetransform::Message> {
                panic!("Panic in transformer");
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server = sourcetransform::Server::new(PanicTransformer)
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

        let mut client = SourceTransformClient::new(channel);

        let (tx, rx) = mpsc::channel(2);
        let handshake_request = proto::SourceTransformRequest {
            request: None,
            handshake: Some(proto::Handshake { sot: true }),
        };
        tx.send(handshake_request).await.unwrap();

        let mut stream = tokio::time::timeout(
            Duration::from_secs(2),
            client.source_transform_fn(ReceiverStream::new(rx)),
        )
        .await
        .map_err(|_| "timeout while getting stream for source_transform_fn")??
        .into_inner();

        let handshake_resp = stream.message().await?.unwrap();
        assert!(
            handshake_resp.handshake.is_some(),
            "Not a valid response for handshake request"
        );

        let request = proto::SourceTransformRequest {
            request: Some(proto::source_transform_request::Request {
                id: "2".to_string(),
                keys: vec!["first".into(), "second".into()],
                value: "hello".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                headers: Default::default(),
                metadata: None,
            }),
            handshake: None,
        };
        tx.send(request).await.unwrap();
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
