use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};

use crate::batchmap::proto::batch_map_server::BatchMap;
use crate::error::Error;
use crate::error::Error::BatchMapError;
use crate::error::ErrorKind::{InternalError, UserDefinedError};
use crate::shared;
use crate::shared::shutdown_signal;

const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_SOCK_ADDR: &str = "/var/run/numaflow/batchmap.sock";
const DEFAULT_SERVER_INFO_FILE: &str = "/var/run/numaflow/mapper-server-info";
const DROP: &str = "U+005C__DROP__";
/// Numaflow Batch Map Proto definitions.
pub mod proto {
    tonic::include_proto!("batchmap.v1");
}

struct BatchMapService<T: BatchMapper> {
    handler: Arc<T>,
    _shutdown_tx: mpsc::Sender<()>,
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

impl From<proto::BatchMapRequest> for Datum {
    fn from(sr: proto::BatchMapRequest) -> Self {
        Self {
            keys: sr.keys,
            value: sr.value,
            watermark: shared::utc_from_timestamp(sr.watermark),
            event_time: shared::utc_from_timestamp(sr.event_time),
            id: sr.id,
            headers: sr.headers,
        }
    }
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
impl crate::batchmap::Message {
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
    pub fn message_to_drop() -> crate::batchmap::Message {
        crate::batchmap::Message {
            keys: None,
            value: vec![],
            tags: Some(vec![crate::batchmap::DROP.to_string()]),
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
    /// use numaflow::batchmap::Message;
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
    /// use numaflow::batchmap::Message;
    /// let message = Message::new(vec![1, 2, 3]).value(vec![4, 5, 6]);
    /// ```
    pub fn value(mut self, value: Vec<u8>) -> Self {
        self.value = value;
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

impl From<Message> for proto::batch_map_response::Result {
    fn from(value: Message) -> Self {
        proto::batch_map_response::Result {
            keys: value.keys.unwrap_or_default(),
            value: value.value,
            tags: value.tags.unwrap_or_default(),
        }
    }
}

#[tonic::async_trait]
impl<T> BatchMap for BatchMapService<T>
where
    T: BatchMapper + Send + Sync + 'static,
{
    async fn is_ready(
        &self,
        _: Request<()>,
    ) -> Result<tonic::Response<proto::ReadyResponse>, Status> {
        Ok(tonic::Response::new(proto::ReadyResponse { ready: true }))
    }

    type BatchMapFnStream = ReceiverStream<Result<proto::BatchMapResponse, Status>>;

    async fn batch_map_fn(
        &self,
        request: Request<Streaming<proto::BatchMapRequest>>,
    ) -> Result<Response<Self::BatchMapFnStream>, Status> {
        let mut stream = request.into_inner();

        // Create a channel to send the messages to the user defined function.
        let (tx, rx) = mpsc::channel::<Datum>(1);

        // Create a channel to send the response back to the grpc client.
        let (grpc_response_tx, grpc_response_rx) =
            channel::<Result<proto::BatchMapResponse, Status>>(1);

        // clone the shutdown_tx to be used in the writer spawn
        let shutdown_tx = self._shutdown_tx.clone();

        // clone the cancellation token to be used in the writer spawn
        let writer_cln_token = self.cancellation_token.clone();

        // counter to keep track of the number of messages received
        let total_messages_recvd = Arc::new(AtomicUsize::new(0));

        // clone the counter to be used in the request spawn
        let counter = Arc::clone(&total_messages_recvd);

        // clone the shutdown_tx to be used in the request spawn
        let read_shutdown_tx = shutdown_tx.clone();
        // read the messages from the grpc client and send it to the user defined function
        let read_handler = tokio::spawn(async move {
            loop {
                match stream.message().await {
                    Ok(Some(message)) => {
                        let datum = Datum::from(message);
                        if let Err(e) = tx.send(datum).await {
                            tracing::error!("Failed to send message: {}", e);
                            break;
                        }
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    // If there's an error or the stream ends, break the loop to stop the task.
                    // and send a shutdown signal to the grpc server.
                    Ok(None) => break,
                    Err(e) => {
                        tracing::error!("Error reading message: {}", e);
                        read_shutdown_tx
                            .send(())
                            .await
                            .expect("shutdown_tx send failed");
                        break;
                    }
                }
            }
        });

        // Create a channel for receiving the response from the user defined function.
        let (response_tx, mut response_rx) = channel::<Result<proto::BatchMapResponse, Error>>(1);

        let handler = Arc::clone(&self.handler);

        let udf_response_tx = response_tx.clone();
        // spawn a task to invoke the user defined function
        let udf_task_handle = tokio::spawn(async move {
            // wait for the messages to be received
            let messages = handler.batchmap(rx).await;

            let counter = total_messages_recvd.load(Ordering::Relaxed);
            // check if the number of responses matches the number of messages received
            // if not send an error back to the grpc client.
            if counter != messages.len() {
                let _ = udf_response_tx
                    .send(Err(BatchMapError(InternalError(
                        "number of responses does not match the number of messages received"
                            .to_string(),
                    ))))
                    .await;
                return;
            }

            // send the response back to the grpc client
            for response in messages {
                let send_result = udf_response_tx
                    .send(Ok(proto::BatchMapResponse {
                        results: response.message.into_iter().map(|m| m.into()).collect(),
                        id: response.id,
                    }))
                    .await;
                // if there's an error sending the response back, send an error back to the grpc client.
                if let Err(e) = send_result {
                    let _ = udf_response_tx
                        .send(Err(BatchMapError(InternalError(format!(
                            "Failed to send response back: {}",
                            e
                        )))))
                        .await;
                    return;
                }
            }
        });

        // Spawn a task to handle the error from the user defined function
        let error_handle = tokio::spawn(async move {
            // if there was an error while executing the user defined function spawn,
            // send an error back to the grpc client.
            if let Err(e) = udf_task_handle.await {
                let _ = response_tx
                    .send(Err(BatchMapError(UserDefinedError(format!(" {}", e)))))
                    .await;
            }
        });

        // Spawn a task to write the response to the grpc client, we also need to check if the cancel token is set
        // in that case we need to stop the task.
        tokio::spawn(async move {
            // wait for the batch map handle to respond
            loop {
                tokio::select! {
                    response = response_rx.recv() => {
                        match response {
                            Some(Ok(response)) => {
                               grpc_response_tx
                                    .send(Ok(response))
                                    .await
                                    .expect("send to grpc response channel failed");
                            },
                            Some(Err(error)) => {
                                tracing::error!("Error from UDF: {:?}", error);
                                grpc_response_tx
                                    .send(Err(Status::internal(error.to_string())))
                                    .await
                                    .expect("send to grpc response channel failed");
                                // Send a shutdown signal to the grpc server.
                                shutdown_tx.send(()).await.expect("shutdown_tx send failed");
                            }
                            None => {
                                // TODO: What should be for None? Is this reachable
                                break;
                            }
                        }
                    }
                    // If the cancellation token is set, stop the task.
                    _ = writer_cln_token.cancelled() => {
                        tracing::info!("token cancelled!, shutting down");
                        // Send an abort signal to the task executor to abort all the tasks.
                        error_handle.abort();
                        read_handler.abort();
                        break;
                    }
                }
            }
        });

        // Return the receiver stream to the client
        Ok(Response::new(ReceiverStream::new(grpc_response_rx)))
    }
}

/// gRPC server to start a batch map service
#[derive(Debug)]
pub struct Server<T> {
    sock_addr: PathBuf,
    max_message_size: usize,
    server_info_file: PathBuf,
    svc: Option<T>,
}
impl<T> crate::batchmap::Server<T> {
    pub fn new(batch_map_svc: T) -> Self {
        crate::batchmap::Server {
            sock_addr: DEFAULT_SOCK_ADDR.into(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            server_info_file: DEFAULT_SERVER_INFO_FILE.into(),
            svc: Some(batch_map_svc),
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    /// Default value is `/var/run/numaflow/batchmap.sock`
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.sock_addr = file.into();
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections. Default value is `/var/run/numaflow/batchmap.sock`
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

    /// Change the file in which numaflow server information is stored on start up to the new value. Default value is `/var/run/numaflow/batchmapper-server-info`
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
        T: BatchMapper + Send + Sync + 'static,
    {
        let mut info = shared::ServerInfo::default();
        // update the info json metadata field, and add the map mode
        info.set_metadata(shared::MAP_MODE_KEY, shared::BATCH_MAP);
        let listener =
            shared::create_listener_stream(&self.sock_addr, &self.server_info_file, info)?;
        let handler = self.svc.take().unwrap();

        let cln_token = CancellationToken::new();

        // Create a channel to send shutdown signal to the server to do graceful shutdown in case of non retryable errors.
        let (internal_shutdown_tx, internal_shutdown_rx) = mpsc::channel(1);
        let map_svc = crate::batchmap::BatchMapService {
            handler: Arc::new(handler),
            _shutdown_tx: internal_shutdown_tx,
            cancellation_token: cln_token.clone(),
        };

        let map_svc = proto::batch_map_server::BatchMapServer::new(map_svc)
            .max_encoding_message_size(self.max_message_size)
            .max_decoding_message_size(self.max_message_size);

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

impl<C> Drop for Server<C> {
    // Cleanup the socket file when the server is dropped so that when the server is restarted, it can bind to the
    // same address. UnixListener doesn't implement Drop trait, so we have to manually remove the socket file.
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.sock_addr);
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
    use crate::batchmap::proto::batch_map_client::BatchMapClient;
    use crate::batchmap::{BatchResponse, Datum, Message};

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

        let mut client = BatchMapClient::new(channel);
        let request = batchmap::proto::BatchMapRequest {
            keys: vec!["first".into()],
            value: "hello".into(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
            id: "1".to_string(),
            headers: Default::default(),
        };

        let request2 = batchmap::proto::BatchMapRequest {
            keys: vec!["second".into()],
            value: "hello2".into(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
            id: "2".to_string(),
            headers: Default::default(),
        };

        let resp = client
            .batch_map_fn(tokio_stream::iter(vec![request, request2]))
            .await?;
        let mut r = resp.into_inner();
        let mut responses = Vec::new();

        while let Some(response) = r.message().await? {
            responses.push(response);
        }

        assert_eq!(responses.len(), 2, "Expected two message from server");
        assert_eq!(&responses[0].id, "1");
        assert_eq!(&responses[1].id, "2");

        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");
        Ok(())
    }

    #[tokio::test]
    async fn error_length() -> Result<(), Box<dyn Error>> {
        struct Logger;
        #[tonic::async_trait]
        impl batchmap::BatchMapper for Logger {
            async fn batchmap(&self, mut input: Receiver<Datum>) -> Vec<BatchResponse> {
                let responses: Vec<BatchResponse> = Vec::new();
                while let Some(_datum) = input.recv().await {}
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

        let mut client = BatchMapClient::new(channel);
        let request = batchmap::proto::BatchMapRequest {
            keys: vec!["first".into(), "second".into()],
            value: "hello".into(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
            id: "1".to_string(),
            headers: Default::default(),
        };

        let resp = client
            .batch_map_fn(tokio_stream::iter(vec![request]))
            .await?;
        let mut r = resp.into_inner();

        let Err(server_err) = r.message().await else {
            return Err("Expected error from server".into());
        };

        assert_eq!(server_err.code(), tonic::Code::Internal);
        assert!(server_err.message().contains(
            "number of responses does not \
              match the number of messages received"
        ));

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

        let mut client = BatchMapClient::new(channel);
        let mut requests = Vec::new();

        for i in 0..10 {
            let request = batchmap::proto::BatchMapRequest {
                keys: vec!["first".into(), "second".into()],
                value: format!("hello {}", i).into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                id: i.to_string(),
                headers: Default::default(),
            };
            requests.push(request);
        }

        let resp = client.batch_map_fn(tokio_stream::iter(requests)).await?;
        let mut response_stream = resp.into_inner();

        if let Err(e) = response_stream.message().await {
            println!("Error: {:?}", e);
            assert_eq!(e.code(), tonic::Code::Internal);
            assert!(e.message().contains("User Defined Error"))
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
