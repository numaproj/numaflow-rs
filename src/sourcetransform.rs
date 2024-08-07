use crate::error::Error::SourceTransformerError;
use crate::error::ErrorKind::UserDefinedError;
use crate::shared::{self, prost_timestamp_from_utc};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::{async_trait, Request, Response, Status};

const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_SOCK_ADDR: &str = "/var/run/numaflow/sourcetransform.sock";
const DEFAULT_SERVER_INFO_FILE: &str = "/var/run/numaflow/sourcetransformer-server-info";

const DROP: &str = "U+005C__DROP__";

/// Numaflow SourceTransformer Proto definitions.
pub mod proto {
    tonic::include_proto!("sourcetransformer.v1");
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
    ///     use numaflow::sourcetransform::Message;
    /// let message=Message::new(input.value, chrono::offset::Utc::now()).keys(input.keys).tags(vec![]);
    ///         vec![message]
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
    /// event_time from the [`Datum`].
    pub event_time: DateTime<Utc>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags: Option<Vec<String>>,
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
        }
    }
    /// Marks the message to be dropped by creating a new `Message` with an empty value, a special "DROP" tag, and the specified event time.
    ///
    /// # Arguments
    ///
    /// * `event_time` - The `DateTime<Utc>` that specifies when the event occurred. Event time is required because, even though a message is dropped,
    ///  it is still considered as being processed, hence the watermark should be updated accordingly using the provided event time.
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
    /// let message = Message::new(vec![1, 2, 3], now).keys(vec!["key1".to_string(), "key2".to_string()]);
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
    /// use numaflow::sourcetransform::Message;
    /// use chrono::Utc;
    /// let now = Utc::now();
    /// let message = Message::new(vec![1, 2, 3], now).tags(vec!["tag1".to_string(), "tag2".to_string()]);
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
    /// use numaflow::sourcetransform::Message;
    /// use chrono::Utc;
    /// let now = Utc::now();
    /// let message = Message::new(vec![1, 2, 3], now).value(vec![4, 5, 6]);
    /// ```
    pub fn value(mut self, value: Vec<u8>) -> Self {
        self.value = value;
        self
    }
}

/// Incoming request to the Source Transformer.
pub struct SourceTransformRequest {
    /// keys are the keys in the (key, value) terminology of map/reduce paradigm.
    /// Once called, it will replace the content with None, so subsequent calls will return None
    pub keys: Vec<String>,
    /// value is the value in (key, value) terminology of map/reduce paradigm.
    /// Once called, it will replace the content with None, so subsequent calls will return None
    pub value: Vec<u8>,
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this
    /// time.
    pub watermark: DateTime<Utc>,
    /// event_time is the time of the element as seen at source or aligned after a reduce operation.
    pub eventtime: DateTime<Utc>,
    /// Headers for the message.
    pub headers: HashMap<String, String>,
}

impl From<Message> for proto::source_transform_response::Result {
    fn from(value: Message) -> Self {
        proto::source_transform_response::Result {
            keys: value.keys.unwrap_or_default(),
            value: value.value,
            event_time: prost_timestamp_from_utc(value.event_time),
            tags: value.tags.unwrap_or_default(),
        }
    }
}

impl From<proto::SourceTransformRequest> for SourceTransformRequest {
    fn from(value: proto::SourceTransformRequest) -> Self {
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
impl<T> proto::source_transform_server::SourceTransform for SourceTransformerService<T>
where
    T: SourceTransformer + Send + Sync + 'static,
{
    async fn source_transform_fn(
        &self,
        request: Request<proto::SourceTransformRequest>,
    ) -> Result<Response<proto::SourceTransformResponse>, Status> {
        let request = request.into_inner();
        let handler = Arc::clone(&self.handler);
        let handle = tokio::spawn(async move { handler.transform(request.into()).await });
        let shutdown_tx = self.shutdown_tx.clone();
        let cancellation_token = self.cancellation_token.clone();

        // Wait for the handler to finish processing the request. If the server is shutting down(token will be cancelled),
        // then return an error.
        tokio::select! {
            result = handle => {
                match result {
                    Ok(messages) => Ok(Response::new(proto::SourceTransformResponse {
                        results: messages.into_iter().map(|msg| msg.into()).collect(),
                    })),
                    Err(e) => {
                        tracing::error!("Error in source transform handler: {:?}", e);
                        // Send a shutdown signal to the server to do a graceful shutdown because there was
                        // a panic in the handler.
                        shutdown_tx.send(()).await.expect("Sending shutdown signal to gRPC server");
                        Err(Status::internal(SourceTransformerError(UserDefinedError(e.to_string())).to_string()))
                    }
                }
            },
            _ = cancellation_token.cancelled() => {
                Err(Status::internal(SourceTransformerError(UserDefinedError("Server is shutting down".to_string())).to_string()))
            },
        }
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}

/// gRPC server to start a sourcetransform service
#[derive(Debug)]
pub struct Server<T> {
    sock_addr: PathBuf,
    max_message_size: usize,
    server_info_file: PathBuf,
    svc: Option<T>,
}

impl<T> Server<T> {
    pub fn new(sourcetransformer_svc: T) -> Self {
        Server {
            sock_addr: DEFAULT_SOCK_ADDR.into(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            server_info_file: DEFAULT_SERVER_INFO_FILE.into(),
            svc: Some(sourcetransformer_svc),
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    /// Default value is `/var/run/numaflow/sourcetransform.sock`
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.sock_addr = file.into();
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections. Default value is `/var/run/numaflow/XXX.sock`
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

    /// Change the file in which numflow server information is stored on start up to the new value. Default value is `/var/run/numaflow/sourcetransformer-server-info`
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.server_info_file = file.into();
        self
    }

    /// Get the path to the file where numaflow server info is stored. Default value is `/var/run/numaflow/sourcetransformer-server-info`
    pub fn server_info_file(&self) -> &std::path::Path {
        self.server_info_file.as_path()
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown(
        &mut self,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: SourceTransformer + Send + Sync + 'static,
    {
        let listener = shared::create_listener_stream(
            &self.sock_addr,
            &self.server_info_file,
            shared::default_info_file(),
        )?;
        let handler = self.svc.take().unwrap();
        let (internal_shutdown_tx, internal_shutdown_rx) = mpsc::channel(1);
        let cln_token = CancellationToken::new();

        let sourcetrf_svc = SourceTransformerService {
            handler: Arc::new(handler),
            shutdown_tx: internal_shutdown_tx,
            cancellation_token: cln_token.clone(),
        };
        let sourcetrf_svc =
            proto::source_transform_server::SourceTransformServer::new(sourcetrf_svc)
                .max_encoding_message_size(self.max_message_size)
                .max_decoding_message_size(self.max_message_size);

        let shutdown = shared::shutdown_signal(internal_shutdown_rx, Some(shutdown_rx));

        // will call cancel_token.cancel() on drop of _drop_guard
        let _drop_guard = cln_token.drop_guard();

        tonic::transport::Server::builder()
            .add_service(sourcetrf_svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await?;

        Ok(())
    }

    /// Starts the gRPC server. Automatically registers singal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the singal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: SourceTransformer + Send + Sync + 'static,
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
    use tokio::sync::oneshot;
    use tonic::transport::Uri;
    use tower::service_fn;

    use crate::sourcetransform;
    use crate::sourcetransform::proto::source_transform_client::SourceTransformClient;

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
                    event_time: chrono::offset::Utc::now(),
                }]
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let mut server = sourcetransform::Server::new(NowCat)
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
        let request = tonic::Request::new(sourcetransform::proto::SourceTransformRequest {
            keys: vec!["first".into(), "second".into()],
            value: "hello".into(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
            headers: Default::default(),
        });

        let resp = client.source_transform_fn(request).await?;
        let resp = resp.into_inner();
        assert_eq!(resp.results.len(), 1, "Expected single message from server");
        let msg = &resp.results[0];
        assert_eq!(msg.keys.first(), Some(&"first".to_owned()));
        assert_eq!(msg.value, "hello".as_bytes());

        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");
        Ok(())
    }

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

        let mut server = sourcetransform::Server::new(PanicTransformer)
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
        let request = tonic::Request::new(sourcetransform::proto::SourceTransformRequest {
            keys: vec!["first".into(), "second".into()],
            value: "hello".into(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
            headers: Default::default(),
        });

        let resp = client.source_transform_fn(request).await;
        assert!(resp.is_err(), "Expected error from server");

        if let Err(e) = resp {
            assert_eq!(e.code(), tonic::Code::Internal);
            assert!(e.message().contains("User Defined Error"));
        }

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
