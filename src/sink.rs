use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tonic::transport::Server;
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
    /// use numaflow::sink;
    /// use numaflow::sink::{Datum, Response};
    /// use tonic::async_trait;
    ///
    /// pub(crate) struct Logger {}
    ///
    ///
    /// impl Logger {
    ///     pub(crate) fn new() -> Self {
    ///         Self {}
    ///     }
    /// }
    ///
    /// #[async_trait]
    /// impl sink::Sinker for Logger {
    ///     async fn sink<T: Datum + Send + Sync + 'static>(
    ///         &self,
    ///         mut input: tokio::sync::mpsc::Receiver<T>,
    ///     ) -> Vec<Response> {
    ///         let mut responses: Vec<Response> = Vec::new();
    ///
    ///         while let Some(datum) = input.recv().await {
    ///             // do something better, but for now let's just log it.
    ///             // please note that `from_utf8` is working because the input in this
    ///             // example uses utf-8 data.
    ///             let response = match std::str::from_utf8(datum.value()) {
    ///                 Ok(v) => {
    ///                     println!("{}", v);
    ///                     // record the response
    ///                     Response {
    ///                         id: datum.id().to_string(),
    ///                         success: true,
    ///                         err: "".to_string(),
    ///                     }
    ///                 }
    ///                 Err(e) => Response {
    ///                     id: datum.id().to_string(),
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
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     use numaflow::sink::start_uds_server;
    ///
    ///     // sink handler
    ///     let sink_handler = Logger::new();
    ///
    ///     start_uds_server(sink_handler).await?;
    ///
    ///     Ok(())
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
    pub eventtime: DateTime<Utc>,
    /// ID is the unique id of the message to be send to the Sink.
    pub id: String,
}

impl From<RPCSinkRequest> for SinkRequest {
    fn from(sr: RPCSinkRequest) -> Self {
        Self {
            keys: sr.keys,
            value: sr.value,
            watermark: shared::utc_from_timestamp(sr.watermark),
            eventtime: shared::utc_from_timestamp(sr.event_time),
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

/// start_uds_server starts a gRPC server over an UDS (unix-domain-socket) endpoint.
pub async fn start_uds_server<T>(m: T) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: Sinker + Send + Sync + 'static,
{
    let server_info_file = if std::env::var_os("NUMAFLOW_POD").is_some() {
        "/var/run/numaflow/server-info"
    } else {
        "/tmp/numaflow.server-info"
    };
    let socket_file = "/var/run/numaflow/sink.sock";
    let listener = shared::create_listener_stream(socket_file, server_info_file)?;
    let sink_service = SinkService { handler: m };

    Server::builder()
        .add_service(SinkServer::new(sink_service))
        .serve_with_incoming(listener)
        .await?;

    Ok(())
}
