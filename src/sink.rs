use std::path::PathBuf;

use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Status, Streaming};

use crate::sink::sinker_grpc::{
    sink_response,
    sink_server::{Sink, SinkServer},
    ReadyResponse, SinkRequest as RPCSinkRequest, SinkResponse,
};

use crate::shared;

mod sinker_grpc {
    tonic::include_proto!("sink.v1");
}

struct SinkService<T: Sinker> {
    pub handler: T,
}

/// Sinker trait implements the user defined sink handle.
///
/// Types implementing this trait can be passed as user-defined sink handle.
#[tonic::async_trait]
pub trait Sinker {
    /// The sink handle is given a stream of [`Datum`]. The result is [`Response`].
    ///
    /// # Example
    ///
    /// A simple log sink.
    ///
    /// ```rust,ignore
    /// use numaflow::sink::{self, Response, SinkRequest};
    /// use std::error::Error;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    ///     sink::Server::new(Logger {}).start().await
    /// }
    ///
    /// struct Logger {}
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
    ///                     Response {
    ///                         id: datum.id,
    ///                         success: true,
    ///                         err: "".to_string(),
    ///                     }
    ///                 }
    ///                 Err(e) => Response {
    ///                     id: datum.id,
    ///                     success: true, // there is no point setting success to false as retrying is not going to help
    ///                     err: format!("Invalid UTF-8 sequence: {}", e),
    ///                 },
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
    async fn sink(&self, mut input: mpsc::Receiver<SinkRequest>) -> Vec<Response>;
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
    /// ID is the unique id of the message to be send to the Sink.
    pub id: String,
}

impl From<RPCSinkRequest> for SinkRequest {
    fn from(sr: RPCSinkRequest) -> Self {
        Self {
            keys: sr.keys,
            value: sr.value,
            watermark: shared::utc_from_timestamp(sr.watermark),
            event_time: shared::utc_from_timestamp(sr.event_time),
            id: sr.id,
        }
    }
}

/// Response is the result returned from the [`Sinker::sink`].
pub struct Response {
    /// id is the unique ID of the message.
    pub id: String,
    /// success indicates whether the write to the sink was successful. If set to `false`, it will be
    /// retried, hence it is better to try till it is successful.
    pub success: bool,
    /// err string is used to describe the error if [`Response::success`]  was `false`.
    pub err: String,
}

impl From<Response> for sink_response::Result {
    fn from(r: Response) -> Self {
        Self {
            id: r.id,
            success: r.success,
            err_msg: r.err.to_string(),
        }
    }
}

#[tonic::async_trait]
impl<T> Sink for SinkService<T>
where
    T: Sinker + Send + Sync + 'static,
{
    async fn sink_fn(
        &self,
        request: Request<Streaming<RPCSinkRequest>>,
    ) -> Result<tonic::Response<SinkResponse>, Status> {
        let mut stream = request.into_inner();

        // TODO: what should be the idle buffer size?
        let (tx, rx) = mpsc::channel::<SinkRequest>(1);

        // call the user's sink handle
        let sink_handle = self.handler.sink(rx);

        // write to the user-defined channel
        tokio::spawn(async move {
            while let Some(next_message) = stream
                .message()
                .await
                .expect("expected next message from stream")
            {
                // panic is good i think!
                tx.send(next_message.into())
                    .await
                    .expect("send be successfully received!");
            }
        });

        // wait for the sink handle to respond
        let responses = sink_handle.await;

        Ok(tonic::Response::new(SinkResponse {
            results: responses.into_iter().map(|r| r.into()).collect(),
        }))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<tonic::Response<ReadyResponse>, Status> {
        Ok(tonic::Response::new(ReadyResponse { ready: true }))
    }
}

/// gRPC server to start a sink service
#[derive(Debug)]
pub struct Server<T> {
    sock_addr: PathBuf,
    max_message_size: usize,
    server_info_file: PathBuf,
    svc: Option<T>,
}

impl<T: Sinker> Server<T> {
    pub fn new(svc: T) -> Self {
        let server_info_file = if std::env::var_os("NUMAFLOW_POD").is_some() {
            "/var/run/numaflow/server-info"
        } else {
            "/tmp/numaflow.server-info"
        };
        Self {
            sock_addr: "/var/run/numaflow/sink.sock".into(),
            max_message_size: 64 * 1024 * 1024,
            server_info_file: server_info_file.into(),
            svc: Some(svc),
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    /// Default value is `/var/run/numaflow/sink.sock`
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.sock_addr = file.into();
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections. Default value is `/var/run/numaflow/map.sock`
    pub fn socket_file(&self) -> &std::path::Path {
        self.sock_addr.as_path()
    }

    /// Set the maximum size of an encoded and decoded gRPC message. The value of `message_size` is in bytes. Default value is 4MB.
    pub fn with_max_message_size(mut self, message_size: usize) -> Self {
        self.max_message_size = message_size;
        self
    }

    /// Get the maximum size of an encoded and decoded gRPC message in bytes. Default value is 4MB.
    pub fn max_message_size(&self) -> usize {
        self.max_message_size
    }

    /// Change the file in which numflow server information is stored on start up to the new value. Default value is `/tmp/numaflow.server-info`
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.server_info_file = file.into();
        self
    }

    /// Get the path to the file where numaflow server info is stored. Default value is `/tmp/numaflow.server-info`
    pub fn server_info_file(&self) -> &std::path::Path {
        self.server_info_file.as_path()
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown(
        &mut self,
        shutdown: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Sinker + Send + Sync + 'static,
    {
        let listener = shared::create_listener_stream(&self.sock_addr, &self.server_info_file)?;
        let handler = self.svc.take().unwrap();
        let svc = SinkService { handler };
        let svc = SinkServer::new(svc)
            .max_encoding_message_size(self.max_message_size)
            .max_decoding_message_size(self.max_message_size);

        let shutdown = async {
            shutdown
                .await
                .expect("Receiving message from shutdown channel");
        };
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await
            .map_err(Into::into)
    }

    /// Starts the gRPC server. Automatically registers singal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the singal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Sinker + Send + Sync + 'static,
    {
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(shared::wait_for_signal(tx));
        self.start_with_shutdown(rx).await
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};
    use tower::service_fn;

    use crate::sink;
    use crate::sink::sinker_grpc::sink_client::SinkClient;
    use tempfile::TempDir;
    use tokio::sync::oneshot;
    use tonic::transport::Uri;

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
                    // do something better, but for now let's just log it.
                    // please note that `from_utf8` is working because the input in this
                    // example uses utf-8 data.
                    let response = match std::str::from_utf8(&datum.value) {
                        Ok(v) => {
                            println!("{}", v);
                            // record the response
                            sink::Response {
                                id: datum.id,
                                success: true,
                                err: "".to_string(),
                            }
                        }
                        Err(e) => sink::Response {
                            id: datum.id,
                            success: true, // there is no point setting success to false as retrying is not going to help
                            err: format!("Invalid UTF-8 sequence: {}", e),
                        },
                    };

                    // return the responses
                    responses.push(response);
                }

                responses
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("map.sock");
        let server_info_file = tmp_dir.path().join("server_info");

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
                // Connect to a Uds socket
                let sock_file = sock_file.clone();
                tokio::net::UnixStream::connect(sock_file)
            }))
            .await?;

        let mut client = SinkClient::new(channel);
        let request = sink::sinker_grpc::SinkRequest {
            keys: vec!["first".into(), "second".into()],
            value: "hello".into(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
            id: "1".to_string(),
        };

        let resp = client.sink_fn(tokio_stream::iter(vec![request])).await?;
        let resp = resp.into_inner();
        assert_eq!(resp.results.len(), 1, "Expected single message from server");
        let msg = &resp.results[0];
        assert_eq!(msg.err_msg, "");
        assert_eq!(msg.id, "1");

        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");
        Ok(())
    }
}
