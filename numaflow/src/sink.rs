use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Status, Streaming};
use tracing::{debug, info};

use crate::error::Error;

use crate::proto::sink::{self as sink_pb, SinkResponse};
use crate::shared::{
    self, ContainerType, ServerConfig, ServiceError, SocketCleanup, DEFAULT_CHANNEL_SIZE,
};

/// Configuration for sink service
pub struct SinkConfig;

impl SinkConfig {
    /// Default socket address for sink service
    pub const SOCK_ADDR: &'static str = "/var/run/numaflow/sink.sock";

    /// Default server info file for sink service
    pub const SERVER_INFO_FILE: &'static str = "/var/run/numaflow/sinker-server-info";

    /// Default socket address for fallback sink
    pub const FB_SOCK_ADDR: &'static str = "/var/run/numaflow/fb-sink.sock";

    /// Default server info file for fallback sink
    pub const FB_SERVER_INFO_FILE: &'static str = "/var/run/numaflow/fb-sinker-server-info";

    /// Container identifier for fallback sink
    pub const FB_CONTAINER_TYPE: &'static str = "fb-udsink";

    /// Environment variable for the container type
    pub const ENV_CONTAINER_TYPE: &'static str = "NUMAFLOW_UD_CONTAINER_TYPE";
}

// TODO: use batch-size, blocked by https://github.com/numaproj/numaflow/issues/2026

struct SinkService<T: Sinker> {
    handler: Arc<T>,
    shutdown_tx: mpsc::Sender<()>,
    cancellation_token: CancellationToken,
}

/// Sinker trait for implementing user defined sinks.
///
/// Types implementing this trait can be passed as user-defined sink handle.
#[tonic::async_trait]
pub trait Sinker {
    /// The sink handle is given a stream of [`SinkRequest`] and the result is [`Response`].
    ///
    /// # Example
    ///
    /// A simple log sink.
    ///
    /// ```no_run
    /// use numaflow::sink::{self, Response, SinkRequest};
    /// use std::error::Error;
    ///
    /// struct Logger;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    ///     sink::Server::new(Logger).start().await
    /// }
    ///
    /// #[tonic::async_trait]
    /// impl sink::Sinker for Logger {
    ///     async fn sink(&self, mut input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
    ///         let mut responses: Vec<Response> = Vec::new();
    ///
    ///         while let Some(datum) = input.recv().await {
    ///             // do something better, but for now let's just log it.
    ///             // please note that `from_utf8` is working because the input in this
    ///             // example uses utf-8 data.
    ///             let response = match std::str::from_utf8(&datum.value) {
    ///                 Ok(v) => {
    ///                     println!("{}", v);
    ///                     // record the response
    ///                     Response::ok(datum.id)
    ///                 }
    ///                 Err(e) => Response::failure(datum.id, format!("Invalid UTF-8 sequence: {}", e)),
    ///             };
    ///
    ///             // return the responses
    ///             responses.push(response);
    ///         }
    ///
    ///         responses
    ///     }
    /// }
    /// ```
    async fn sink(&self, input: mpsc::Receiver<SinkRequest>) -> Vec<Response>;
}

/// Incoming request into the  handler of [`Sinker`].
pub struct SinkRequest {
    /// Set of keys in the (key, value) terminology of map/reduce paradigm.
    pub keys: Vec<String>,
    /// The value in the (key, value) terminology of map/reduce paradigm.
    pub value: Vec<u8>,
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this time.
    pub watermark: DateTime<Utc>,
    /// Time of the element as seen at source or aligned after a reduce operation.
    pub event_time: DateTime<Utc>,
    /// ID is the unique id of the message to be sent to the Sink.
    pub id: String,
    /// Headers for the message.
    pub headers: HashMap<String, String>,
}

impl From<sink_pb::sink_request::Request> for SinkRequest {
    fn from(sr: sink_pb::sink_request::Request) -> Self {
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

/// Type of response from the sink handler.
pub enum ResponseType {
    /// write to the sink was successful.
    Success,
    /// write to the sink failed.
    Failure,
    /// message should be forwarded to the fallback sink.
    FallBack,
    /// message should be written to the serving store.
    Serve,
}

/// The result of the call to [`Sinker::sink`] method.
pub struct Response {
    /// id is the unique ID of the message.
    pub id: String,
    /// response_type indicates the type of the response.
    pub response_type: ResponseType,
    /// err string is used to describe the error if [`ResponseType::Failure`]  is set.
    pub err: Option<String>,
    pub serve_response: Option<Vec<u8>>,
}

impl Response {
    /// Creates a new `Response` instance indicating a successful operation.
    pub fn ok(id: String) -> Self {
        Self {
            id,
            response_type: ResponseType::Success,
            err: None,
            serve_response: None,
        }
    }

    /// Creates a new `Response` instance indicating a failed operation.
    pub fn failure(id: String, err: String) -> Self {
        Self {
            id,
            response_type: ResponseType::Failure,
            err: Some(err),
            serve_response: None,
        }
    }

    /// Creates a new `Response` instance indicating a failed operation with a fallback
    /// set to 'true'. So that the message will be forwarded to the fallback sink.
    pub fn fallback(id: String) -> Self {
        Self {
            id,
            response_type: ResponseType::FallBack,
            err: None,
            serve_response: None,
        }
    }

    pub fn serve(id: String, payload: Vec<u8>) -> Self {
        Self {
            id,
            response_type: ResponseType::Serve,
            err: None,
            serve_response: Some(payload),
        }
    }
}

impl From<Response> for sink_pb::sink_response::Result {
    fn from(r: Response) -> Self {
        Self {
            id: r.id,
            status: match r.response_type {
                ResponseType::Success => sink_pb::Status::Success as i32,
                ResponseType::Failure => sink_pb::Status::Failure as i32,
                ResponseType::FallBack => sink_pb::Status::Fallback as i32,
                ResponseType::Serve => sink_pb::Status::Serve as i32,
            },
            err_msg: r.err.unwrap_or_default(),
            serve_response: r.serve_response,
        }
    }
}

#[tonic::async_trait]
impl<T> sink_pb::sink_server::Sink for SinkService<T>
where
    T: Sinker + Send + Sync + 'static,
{
    type SinkFnStream = ReceiverStream<Result<SinkResponse, Status>>;

    async fn sink_fn(
        &self,
        request: Request<Streaming<sink_pb::SinkRequest>>,
    ) -> Result<tonic::Response<Self::SinkFnStream>, Status> {
        let mut sink_stream = request.into_inner();
        let sink_handle = self.handler.clone();
        let shutdown_tx = self.shutdown_tx.clone();
        let cln_token = self.cancellation_token.clone();
        let (resp_tx, resp_rx) =
            mpsc::channel::<Result<SinkResponse, Status>>(DEFAULT_CHANNEL_SIZE);

        self.perform_handshake(&mut sink_stream, &resp_tx).await?;

        let grpc_resp_tx = resp_tx.clone();
        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            Self::process_sink_stream(sink_handle, sink_stream, grpc_resp_tx).await
        });

        tokio::spawn(Self::handle_sink_errors(
            handle,
            resp_tx,
            shutdown_tx,
            cln_token,
        ));

        Ok(tonic::Response::new(ReceiverStream::new(resp_rx)))
    }

    async fn is_ready(
        &self,
        _: Request<()>,
    ) -> Result<tonic::Response<sink_pb::ReadyResponse>, Status> {
        Ok(tonic::Response::new(sink_pb::ReadyResponse { ready: true }))
    }
}

impl<T> SinkService<T>
where
    T: Sinker + Send + Sync + 'static,
{
    /// processes the stream of requests from the client
    async fn process_sink_stream(
        sink_handle: Arc<T>,
        mut sink_stream: Streaming<sink_pb::SinkRequest>,
        grpc_resp_tx: mpsc::Sender<Result<SinkResponse, Status>>,
    ) -> Result<(), Error> {
        // loop until the global stream has been shutdown.
        let mut global_stream_ended = false;
        while !global_stream_ended {
            // for every batch, we need to read from the stream. The end-of-batch is
            // encoded in the request.
            global_stream_ended = Self::process_sink_batch(
                sink_handle.clone(),
                &mut sink_stream,
                grpc_resp_tx.clone(),
            )
            .await?;
        }
        Ok(())
    }

    /// processes a batch of messages from the client, sends them to the sink handler and sends the
    /// responses back to the client batches are separated by an EOT message.
    /// Returns true if the global bidi-stream has ended, otherwise false.
    async fn process_sink_batch(
        sink_handle: Arc<T>,
        sink_stream: &mut Streaming<sink_pb::SinkRequest>,
        grpc_resp_tx: mpsc::Sender<Result<SinkResponse, Status>>,
    ) -> Result<bool, Error> {
        let (tx, rx) = mpsc::channel::<SinkRequest>(DEFAULT_CHANNEL_SIZE);
        let resp_tx = grpc_resp_tx.clone();
        let sink_handle = sink_handle.clone();

        // spawn the UDF
        let sinker_handle = tokio::spawn(async move {
            let responses = sink_handle.sink(rx).await;
            resp_tx
                .send(Ok(SinkResponse {
                    results: responses.into_iter().map(|r| r.into()).collect(),
                    handshake: None,
                    status: None,
                }))
                .await
                .expect("Sending response to channel");

            // send an EOT message to the client to indicate the end of transmission for this batch
            resp_tx
                .send(Ok(SinkResponse {
                    results: vec![],
                    handshake: None,
                    status: Some(sink_pb::TransmissionStatus { eot: true }),
                }))
                .await
                .expect("Sending EOT message to channel");
        });

        let mut global_stream_ended = false;

        // loop until eot happens or stream is closed.
        loop {
            let message = match sink_stream.message().await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    info!("global bidi stream ended");
                    // NOTE: this will only happen during shutdown. We can be certain that there
                    // are no messages left hanging in the UDF.
                    global_stream_ended = true;
                    break; // bidi stream ended
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

            // message.request cannot be none
            let request = message.request.ok_or_else(|| {
                Server::<()>::internal_error("Invalid argument, request can't be None")
            })?;

            // write to the UDF's tx
            tx.send(request.into()).await.map_err(|e| {
                Server::<()>::internal_error(format!(
                    "Error sending message to sink handler: {}",
                    e
                ))
            })?;
        }

        // drop the sender to signal the sink handler that the batch has ended
        drop(tx);

        // wait for UDF task to return
        sinker_handle
            .await
            .map_err(|e| Server::<()>::user_error(e.to_string()))?;

        Ok(global_stream_ended)
    }

    /// handles errors from the sink handler and sends them to the client via the response channel
    async fn handle_sink_errors(
        handle: JoinHandle<Result<(), Error>>,
        resp_tx: mpsc::Sender<Result<SinkResponse, Status>>,
        shutdown_tx: mpsc::Sender<()>,
        cln_token: CancellationToken,
    ) {
        tokio::select! {
            resp = handle => {
                match resp {
                    Ok(Ok(_)) => {},
                    Ok(Err(e)) => {
                        resp_tx
                            .send(Err(Server::<()>::grpc_internal_error(e.to_string())))
                            .await
                            .expect("Sending error to response channel");
                        shutdown_tx.send(()).await.expect("Sending shutdown signal");
                    }
                    Err(e) => {
                        resp_tx
                            .send(Err(Server::<()>::grpc_internal_error(format!(
                                "Sink handler aborted: {}",
                                e
                            ))))
                            .await
                            .expect("Sending error to response channel");
                        shutdown_tx.send(()).await.expect("Sending shutdown signal");
                    }
                }
            },
            _ = cln_token.cancelled() => {
                resp_tx
                    .send(Err(Server::<()>::grpc_cancelled_error("Sink handler cancelled")))
                    .await
                    .expect("Sending error to response channel");
            }
        }
    }

    /// performs handshake with the client
    async fn perform_handshake(
        &self,
        sink_stream: &mut Streaming<sink_pb::SinkRequest>,
        resp_tx: &mpsc::Sender<Result<SinkResponse, Status>>,
    ) -> Result<(), Status> {
        let handshake_request = sink_stream
            .message()
            .await
            .map_err(|e| Server::<()>::grpc_internal_error(format!("handshake failed {}", e)))?
            .ok_or_else(|| Server::<()>::grpc_internal_error("stream closed before handshake"))?;

        if let Some(handshake) = handshake_request.handshake {
            resp_tx
                .send(Ok(SinkResponse {
                    results: vec![],
                    handshake: Some(handshake),
                    status: None,
                }))
                .await
                .map_err(|e| {
                    Server::<()>::grpc_internal_error(format!(
                        "failed to send handshake response {}",
                        e
                    ))
                })?;
            Ok(())
        } else {
            Err(Server::<()>::grpc_invalid_argument_error(
                "Handshake not present",
            ))
        }
    }
}

/// gRPC server to start a sink service
#[derive(Debug)]
pub struct Server<T> {
    config: ServerConfig,
    svc: Option<T>,
    _cleanup: SocketCleanup,
}

impl<T> Server<T> {
    pub fn new(svc: T) -> Self {
        let container_type = env::var(SinkConfig::ENV_CONTAINER_TYPE).unwrap_or_default();
        let (sock_addr, server_info_file) = if container_type == SinkConfig::FB_CONTAINER_TYPE {
            (SinkConfig::FB_SOCK_ADDR, SinkConfig::FB_SERVER_INFO_FILE)
        } else {
            (SinkConfig::SOCK_ADDR, SinkConfig::SERVER_INFO_FILE)
        };

        let config = ServerConfig::new(sock_addr, server_info_file);
        let cleanup = SocketCleanup::new(config.sock_addr.clone());

        Self {
            config,
            svc: Some(svc),
            _cleanup: cleanup,
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    /// Default value is `/var/run/numaflow/sink.sock`
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.config = self.config.with_socket_file(file);
        self._cleanup = SocketCleanup::new(self.config.sock_addr.clone());
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections. Default value is `/var/run/numaflow/sink.sock`
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

    /// Change the file in which numaflow server information is stored on start up to the new value. Default value is `/var/run/numaflow/sinker-server-info`
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.config = self.config.with_server_info_file(file);
        self
    }

    /// Get the path to the file where numaflow server info is stored. Default value is `/var/run/numaflow/sinker-server-info`
    pub fn server_info_file(&self) -> &std::path::Path {
        self.config.server_info_file()
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown(
        &mut self,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Sinker + Send + Sync + 'static,
    {
        let info = shared::ServerInfo::new(ContainerType::Sink);
        let listener = shared::create_listener_stream(
            &self.config.sock_addr,
            &self.config.server_info_file,
            info,
        )?;
        let handler = self.svc.take().unwrap();
        let cln_token = CancellationToken::new();
        let (internal_shutdown_tx, internal_shutdown_rx) = mpsc::channel(1);

        let svc = SinkService {
            handler: Arc::new(handler),
            shutdown_tx: internal_shutdown_tx,
            cancellation_token: cln_token.clone(),
        };

        let svc = sink_pb::sink_server::SinkServer::new(svc)
            .max_encoding_message_size(self.config.max_message_size)
            .max_decoding_message_size(self.config.max_message_size);

        let shutdown = shared::shutdown_signal(internal_shutdown_rx, Some(shutdown_rx), cln_token);

        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await?;

        Ok(())
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the singal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Sinker + Send + Sync + 'static,
    {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        self.start_with_shutdown(shutdown_rx).await
    }
}

/// Implementation of ServiceError trait for Sink service
impl<T> ServiceError for Server<T> {
    fn service_name() -> &'static str {
        "sink"
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

    use crate::proto::sink::sink_client::SinkClient;
    use crate::proto::sink::sink_request::Request;
    use crate::proto::sink::Handshake;
    use crate::proto::sink::TransmissionStatus;
    use crate::sink;

    #[tokio::test]
    async fn sink_server() -> Result<(), Box<dyn Error>> {
        struct Logger;
        #[tonic::async_trait]
        impl sink::Sinker for Logger {
            async fn sink(
                &self,
                mut input: tokio::sync::mpsc::Receiver<sink::SinkRequest>,
            ) -> Vec<sink::Response> {
                let mut responses: Vec<sink::Response> = Vec::new();
                while let Some(datum) = input.recv().await {
                    let response = match std::str::from_utf8(&datum.value) {
                        Ok(_) => sink::Response::ok(datum.id),
                        Err(e) => sink::Response::failure(
                            datum.id,
                            format!("Invalid UTF-8 sequence: {}", e),
                        ),
                    };
                    responses.push(response);
                }
                responses
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sink.sock");
        let server_info_file = tmp_dir.path().join("sinker-server-info");

        let mut server = sink::Server::new(Logger)
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

        let mut client = SinkClient::new(channel);
        // Send handshake request
        let handshake_request = crate::proto::sink::SinkRequest {
            request: None,
            status: None,
            handshake: Some(Handshake { sot: true }),
        };
        let request = crate::proto::sink::SinkRequest {
            request: Some(Request {
                keys: vec!["first".into(), "second".into()],
                value: "hello".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                id: "1".to_string(),
                headers: Default::default(),
            }),
            status: None,
            handshake: None,
        };

        let eot_request = crate::proto::sink::SinkRequest {
            request: None,
            status: Some(TransmissionStatus { eot: true }),
            handshake: None,
        };

        let request_two = crate::proto::sink::SinkRequest {
            request: Some(Request {
                keys: vec!["first".into(), "second".into()],
                value: "hello".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                id: "2".to_string(),
                headers: Default::default(),
            }),
            status: None,
            handshake: None,
        };

        let resp = client
            .sink_fn(tokio_stream::iter(vec![
                handshake_request,
                request,
                eot_request.clone(),
                request_two,
                eot_request,
            ]))
            .await?;

        let mut resp_stream = resp.into_inner();
        // handshake response
        let resp = resp_stream.message().await.unwrap().unwrap();
        assert!(resp.handshake.is_some());

        let resp = resp_stream.message().await.unwrap().unwrap();
        assert!(!resp.results.is_empty());
        let msg = resp.results.first().unwrap();
        assert_eq!(msg.err_msg, "");
        assert_eq!(msg.id, "1");

        // eot for first request
        let resp = resp_stream.message().await.unwrap().unwrap();
        assert!(resp.results.is_empty());
        assert!(resp.handshake.is_none());
        let msg = &resp.status.unwrap();
        assert!(msg.eot);

        let resp = resp_stream.message().await.unwrap().unwrap();
        assert!(!resp.results.is_empty());
        assert!(resp.handshake.is_none());
        let msg = resp.results.first().unwrap();
        assert_eq!(msg.err_msg, "");
        assert_eq!(msg.id, "2");

        // eot for second request
        let resp = resp_stream.message().await.unwrap().unwrap();
        assert!(resp.results.is_empty());
        assert!(resp.handshake.is_none());
        let msg = &resp.status.unwrap();
        assert!(msg.eot);

        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");
        Ok(())
    }

    #[tokio::test]
    async fn sink_panic() -> Result<(), Box<dyn Error>> {
        struct PanicSink;
        #[tonic::async_trait]
        impl sink::Sinker for PanicSink {
            async fn sink(
                &self,
                mut input: tokio::sync::mpsc::Receiver<sink::SinkRequest>,
            ) -> Vec<sink::Response> {
                let mut responses: Vec<sink::Response> = Vec::new();
                let mut count = 0;

                while let Some(datum) = input.recv().await {
                    if count > 5 {
                        panic!("Should not cross 5");
                    }
                    count += 1;
                    responses.push(sink::Response::ok(datum.id));
                }
                responses
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sink.sock");
        let server_info_file = tmp_dir.path().join("sinker-server-info");

        let mut server = sink::Server::new(PanicSink)
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

        let mut client = SinkClient::new(channel);
        // Send handshake request
        let handshake_request = crate::proto::sink::SinkRequest {
            request: None,
            status: None,
            handshake: Some(Handshake { sot: true }),
        };

        let mut requests = vec![handshake_request];

        for i in 0..10 {
            let request = crate::proto::sink::SinkRequest {
                request: Some(Request {
                    keys: vec!["first".into(), "second".into()],
                    value: format!("hello {}", i).into(),
                    watermark: Some(prost_types::Timestamp::default()),
                    event_time: Some(prost_types::Timestamp::default()),
                    id: i.to_string(),
                    headers: Default::default(),
                }),
                status: None,
                handshake: None,
            };
            requests.push(request);
        }

        requests.push(crate::proto::sink::SinkRequest {
            request: None,
            status: Some(TransmissionStatus { eot: true }),
            handshake: None,
        });

        let mut resp_stream = client
            .sink_fn(tokio_stream::iter(requests))
            .await
            .unwrap()
            .into_inner();

        // handshake response
        let resp = resp_stream.message().await.unwrap().unwrap();
        assert!(resp.results.is_empty());
        assert!(resp.handshake.is_some());

        let err_resp = resp_stream.message().await;
        assert!(err_resp.is_err());

        if let Err(e) = err_resp {
            assert_eq!(e.code(), tonic::Code::Internal);
            assert!(e.message().contains("User Defined error"));
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
