use chrono::{DateTime, TimeZone, Utc};
use prost_types::Timestamp;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Request, Status, Streaming};

use sinker_grpc::sink_server::SinkServer;
use sinker_grpc::{ReadyResponse, SinkRequest, SinkResponse};

use crate::sink::sinker_grpc::sink_server::Sink;
use crate::startup;

mod sinker_grpc {
    tonic::include_proto!("sink.v1");
}

struct SinkService<T: Sinker> {
    pub handler: T,
}

/// SinkerFoo trait implements the user defined sink handle.
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
    /// ```rust
    /// use numaflow::sink;
    /// use numaflow::sink::{Datum, Response};
    /// use tonic::async_trait;
    ///
    /// pub(crate) struct Logger {}
    ///
    /// impl Logger {
    ///     pub(crate) fn new() -> Self {
    ///         Self {}
    ///     }
    /// }
    ///
    /// #[async_trait]
    /// impl sink::Sinker for Logger {
    ///     async fn handle<T: Datum + Send + Sync + 'static>(
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
    /// ```
    async fn sink<T: Datum + Send + Sync + 'static>(
        &self,
        input: mpsc::Receiver<T>,
    ) -> Vec<Response>;
}

/// Response is the result returned from the [`Sinker::handle`].
pub struct Response {
    /// id is the unique ID of the message.
    pub id: String,
    /// success indicates whether the write to the sink was successful. If set to `false`, it will be
    /// retried, hence it is better to try till it is successful.
    pub success: bool,
    /// err string is used to describe the error if [`Response::success`]  was `false`.
    pub err: String,
}

/// Datum trait represents an incoming element into the [`Sinker::handle`].
pub trait Datum {
    /// keys are the keys in the (key, value) terminology of map/reduce paradigm.
    fn keys(&mut self) -> &Vec<String>;
    /// value is the value in (key, value) terminology of map/reduce paradigm.
    fn value(&mut self) -> &Vec<u8>;
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this
    /// time.
    fn watermark(&self) -> DateTime<Utc>;
    /// event_time is the time of the element as seen at source or aligned after a reduce operation.
    fn event_time(&self) -> DateTime<Utc>;
    /// ID corresponds the unique ID in the message.
    fn id(&mut self) -> &str;
}

struct OwnedSinkRequest {
    keys: Vec<String>,
    value: Vec<u8>,
    watermark: DateTime<Utc>,
    eventtime: DateTime<Utc>,
    id: String,
}

fn utc_from_timestamp(t: Option<Timestamp>) -> DateTime<Utc> {
    if let Some(ref t) = t {
        Utc.timestamp_nanos(t.seconds * (t.nanos as i64))
    } else {
        Utc.timestamp_nanos(-1)
    }
}

impl OwnedSinkRequest {
    fn new(sr: SinkRequest) -> Self {
        Self {
            keys: sr.keys,
            value: sr.value,
            watermark: utc_from_timestamp(sr.watermark),
            eventtime: utc_from_timestamp(sr.event_time),
            id: sr.id,
        }
    }
}

impl Datum for OwnedSinkRequest {
    fn keys(&mut self) -> &Vec<String> {
        &self.keys
    }

    fn value(&mut self) -> &Vec<u8> {
        &self.value
    }

    fn watermark(&self) -> DateTime<Utc> {
        self.watermark
    }

    fn event_time(&self) -> DateTime<Utc> {
        self.eventtime
    }

    fn id(&mut self) -> &str {
        &self.id
    }
}

#[tonic::async_trait]
impl<T> Sink for SinkService<T>
where
    T: Sinker + Send + Sync + 'static,
{
    async fn sink_fn(
        &self,
        request: Request<Streaming<SinkRequest>>,
    ) -> Result<tonic::Response<SinkResponse>, Status> {
        let mut stream = request.into_inner();

        // TODO: what should be the idle buffer size?
        let (tx, rx) = mpsc::channel::<OwnedSinkRequest>(1);

        // call the user's sink handle
        let sink_handle = self.handler.sink(rx);

        // write to the user-defined channel
        tokio::spawn(async move {
            while let Some(next_message) = stream
                .message()
                .await
                .expect("expected next message from stream")
            {
                let owned_next_message = OwnedSinkRequest::new(next_message);
                // panic is good i think!
                tx.send(owned_next_message)
                    .await
                    .expect("send be successfully received!");
            }
        });

        // wait for the sink handle to respond
        let responses = sink_handle.await;

        // build the result
        let mut sink_responses: Vec<sinker_grpc::sink_response::Result> = Vec::new();
        for response in responses {
            sink_responses.push(sinker_grpc::sink_response::Result {
                id: response.id,
                success: response.success,
                err_msg: response.err.to_string(),
            })
        }

        Ok(tonic::Response::new(SinkResponse {
            results: sink_responses,
        }))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<tonic::Response<ReadyResponse>, Status> {
        Ok(tonic::Response::new(ReadyResponse { ready: true }))
    }
}

/// start_uds_server starts a gRPC server over an UDS (unix-domain-socket) endpoint.
///
/// # Example
///
///```rust
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     use numaflow::sink::start_uds_server;
///
///     // sink handler
///     let sink_handler = todo!();
///
///     start_uds_server(sink_handler).await?;
///
///     Ok(())
/// }
/// ```
pub async fn start_uds_server<T>(m: T) -> Result<(), Box<dyn std::error::Error>>
where
    T: Sinker + Send + Sync + 'static,
{
    startup::write_info_file();

    let path = "/var/run/numaflow/sink.sock";
    fs::create_dir_all(std::path::Path::new(path).parent().unwrap())?;
    use std::fs;
    use tokio::net::UnixListener;
    use tokio_stream::wrappers::UnixListenerStream;

    let uds = UnixListener::bind(path)?;
    let _uds_stream = UnixListenerStream::new(uds);

    let sink_service = SinkService { handler: m };

    Server::builder()
        .add_service(SinkServer::new(sink_service))
        .serve_with_incoming(_uds_stream)
        .await?;

    Ok(())
}
