use std::future::Future;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use tonic::{async_trait, Request, Response, Status};

use crate::shared::{self, prost_timestamp_from_utc};

const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_SOCK_ADDR: &str = "/var/run/numaflow/sourcetransform.sock";
const DEFAULT_SERVER_INFO_FILE: &str = "/var/run/numaflow/sourcetransformer-server-info";

const DROP: &str ="U+005C__DROP__";
/// Numaflow SourceTransformer Proto definitions.
pub mod proto {
    tonic::include_proto!("sourcetransformer.v1");
}

struct SourceTransformerService<T> {
    handler: T,
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
    ///     use numaflow::sourcetransform::MessageBuilder;
    /// let message=MessageBuilder::new().keys(input.keys).values(input.value).tags(vec![]).event_time(chrono::offset::Utc::now()).build();
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
     keys: Vec<String>,
    /// Value is the value passed to the next vertex.
     value: Vec<u8>,
    /// Time for the given event. This will be used for tracking watermarks. If cannot be derived, set it to the incoming
    /// event_time from the [`Datum`].
    event_time: DateTime<Utc>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    tags: Vec<String>,
}

#[derive(Default)]
pub struct MessageBuilder{
    keys: Vec<String>,
    value: Vec<u8>,
    tags: Vec<String>,
    event_time:DateTime<Utc>
}
impl MessageBuilder {
    pub fn new()->Self{
        Default::default()
    }
    pub fn message_to_drop(mut self) -> Self {
        self.tags.push(DROP.parse().unwrap());
        self
    }
    pub fn keys(mut self,keys:Vec<String>)->  Self{
        self.keys=keys;
        self
    }

    pub fn tags(mut self,tags:Vec<String>)->  Self{
        self.tags=tags;
        self
    }

    pub fn values( mut self,value: Vec<u8>)->Self{
        self.value=value;
        self
    }

    pub fn event_time(mut self,event_time:DateTime<Utc>)->Self{
        self.event_time=event_time;
        self

    }
    pub fn build(self)-> Message {
        Message {
            keys: self.keys,
            value:self.value,
            tags: self.tags,
            event_time:self.event_time
        }
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
}

impl From<Message> for proto::source_transform_response::Result {
    fn from(value: Message) -> Self {
        proto::source_transform_response::Result {
            keys: value.keys,
            value: value.value,
            event_time: prost_timestamp_from_utc(value.event_time),
            tags: value.tags,
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

        let messages = self.handler.transform(request.into()).await;

        Ok(Response::new(proto::SourceTransformResponse {
            results: messages
                .into_iter()
                .map(|msg| msg.into())
                .collect::<Vec<_>>(),
        }))
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
    pub async fn start_with_shutdown<F>(
        &mut self,
        shutdown: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: SourceTransformer + Send + Sync + 'static,
        F: Future<Output = ()>,
    {
        let listener = shared::create_listener_stream(&self.sock_addr, &self.server_info_file)?;
        let handler = self.svc.take().unwrap();
        let sourcetrf_svc = SourceTransformerService { handler };
        let sourcetrf_svc =
            proto::source_transform_server::SourceTransformServer::new(sourcetrf_svc)
                .max_encoding_message_size(self.max_message_size)
                .max_decoding_message_size(self.max_message_size);

        tonic::transport::Server::builder()
            .add_service(sourcetrf_svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await
            .map_err(Into::into)
    }

    /// Starts the gRPC server. Automatically registers singal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the singal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: SourceTransformer + Send + Sync + 'static,
    {
        self.start_with_shutdown(shared::shutdown_signal()).await
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};
    use tower::service_fn;

    use crate::sourcetransform;
    use crate::sourcetransform::proto::source_transform_client::SourceTransformClient;
    use tempfile::TempDir;
    use tokio::sync::oneshot;
    use tonic::transport::Uri;

    #[tokio::test]
    async fn sourcetransformer_server() -> Result<(), Box<dyn Error>> {
        struct NowCat;
        #[tonic::async_trait]
        impl sourcetransform::SourceTransformer for NowCat {
            async fn transform(
                &self,
                input: sourcetransform::SourceTransformRequest,
            ) -> Vec<sourcetransform::Message> {
                vec![sourcetransform::Message {
                    keys: input.keys,
                    value: input.value,
                    tags: vec![],
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
        let shutdown = async {
            shutdown_rx.await.unwrap();
        };
        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // https://github.com/hyperium/tonic/blob/master/examples/src/uds/client.rs
        let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(service_fn(move |_: Uri| {
                // Connect to a Uds socket
                let sock_file = sock_file.clone();
                tokio::net::UnixStream::connect(sock_file)
            }))
            .await?;

        let mut client = SourceTransformClient::new(channel);
        let request = tonic::Request::new(sourcetransform::proto::SourceTransformRequest {
            keys: vec!["first".into(), "second".into()],
            value: "hello".into(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
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
}
