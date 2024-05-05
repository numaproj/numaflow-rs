use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataMap;
use tonic::{async_trait, Request, Response, Status};
use crate::shared;

const KEY_JOIN_DELIMITER: &str = ":";
// grpc window metadata
const WIN_START_TIME: &str = "x-numaflow-win-start-time";
const WIN_END_TIME: &str = "x-numaflow-win-end-time";
const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_SOCK_ADDR: &str = "/var/run/numaflow/reduce.sock";
const DEFAULT_SERVER_INFO_FILE: &str = "/var/run/numaflow/reducer-server-info";

/// Numaflow Reduce Proto definitions.
pub mod proto {
    tonic::include_proto!("reduce.v1");
}

struct ReduceService<T> {
    handler: Arc<T>,
}

/// Reducer trait for implementing Reduce handler.
#[async_trait]
pub trait Reducer {
    /// reduce_handle is provided with a set of keys, a channel of [`Datum`], and [`Metadata`]. It
    /// returns 0, 1, or more results as a [`Vec`] of [`Message`]. Reduce is a stateful operation and
    /// the channel is for the collection of keys and for that time [Window].
    /// You can read more about reduce [here](https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/reduce/).
    ///
    /// # Example
    ///
    /// Below is a reduce code to count the number of elements for a given set of keys and window.
    ///
    /// ```no_run
    /// use numaflow::reduce;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let reduce_handler = counter::Counter::new();
    ///     reduce::Server::new(reduce_handler).start().await?;
    ///     Ok(())
    /// }
    /// mod counter {
    ///     use numaflow::reduce::{Message, ReduceRequest};
    ///     use numaflow::reduce::{Reducer, Metadata};
    ///     use tokio::sync::mpsc::Receiver;
    ///     use tonic::async_trait;
    /// use numaflow::reduce::proto::reduce_server::Reduce;
    ///     pub(crate) struct Counter {}
    ///     impl Counter {
    ///         pub(crate) fn new() -> Self {
    ///             Self {}
    ///         }
    ///     }
    ///     #[async_trait]
    ///     impl Reducer for Counter {
    ///         async fn reduce(
    ///             &self,
    ///             keys: Vec<String>,
    ///             mut input: Receiver<ReduceRequest>,
    ///             md: Metadata,
    ///         ) -> Vec<Message> {
    ///             let mut counter = 0;
    ///             // the loop exits when input is closed which will happen only on close of book.
    ///             while (input.recv().await).is_some() {
    ///                 counter += 1;
    ///             }
    ///             vec![Message {
    ///                 keys: keys.clone(),
    ///                 value: counter.to_string().into_bytes(),
    ///                 tags: vec![],
    ///             }]
    ///         }
    ///     }
    /// }
    ///```
    /// [Window]: https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/windowing/
    async fn reduce(
        &self,
        keys: Vec<String>,
        input: mpsc::Receiver<ReduceRequest>,
        md: Metadata,
    ) -> Vec<Message>;
}

/// IntervalWindow is the start and end boundary of the window.
#[derive(Clone)]
pub struct IntervalWindow {
    // start time of the window
    pub start_time: DateTime<Utc>,
    // end time of the window
    pub end_time: DateTime<Utc>,
}

impl IntervalWindow {
    fn new(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> Self {
        Self { start_time, end_time }
    }
}

impl Metadata {
    pub fn new(interval_window: IntervalWindow) -> Self {
        Self { interval_window }
    }
}

#[derive(Clone)]
/// Metadata are additional information passed into the [`Reducer::reduce`].
pub struct Metadata {
    pub interval_window: IntervalWindow
}

/// Message is the response from the user's [`Reducer::reduce`].
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection. It is mainly used in creating a partition in [`Reducer::reduce`].
    pub keys: Vec<String>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags: Vec<String>,
}

/// Incoming request into the reducer handler of [`Reducer`].
pub struct ReduceRequest {
    /// Set of keys in the (key, value) terminology of map/reduce paradigm.
    pub keys: Vec<String>,
    /// The value in the (key, value) terminology of map/reduce paradigm.    /// The value in the (key, value) terminology of map/reduce paradigm.
    pub value: Vec<u8>,
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this time.    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this time.
    pub watermark: DateTime<Utc>,
    /// Time of the element as seen at source or aligned after a reduce operation.
    pub eventtime: DateTime<Utc>,
}

impl From<proto::ReduceRequest> for ReduceRequest {
    fn from(mr: proto::ReduceRequest) -> Self {
        Self {
            keys: mr.keys,
            value: mr.value,
            watermark: shared::utc_from_timestamp(mr.watermark),
            eventtime: shared::utc_from_timestamp(mr.event_time),
        }
    }
}

// extract start and end time from the gRPC MetadataMap
// https://youtu.be/s5S2Ed5T-dc?t=662
fn get_window_details(request: &MetadataMap) -> (DateTime<Utc>, DateTime<Utc>) {
    let (st, et) = (
        request
            .get(WIN_START_TIME)
            .unwrap_or_else(|| panic!("expected key {}", WIN_START_TIME))
            .to_str()
            .unwrap()
            .to_string()
            .parse::<i64>()
            .unwrap(),
        request
            .get(WIN_END_TIME)
            .unwrap_or_else(|| panic!("expected key {}", WIN_END_TIME))
            .to_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
    );

    (
        Utc.timestamp_millis_opt(st).unwrap(),
        Utc.timestamp_millis_opt(et).unwrap(),
    )
}

#[async_trait]
impl<T> proto::reduce_server::Reduce for ReduceService<T>
where
    T: Reducer + Send + Sync + 'static,
{
    type ReduceFnStream = ReceiverStream<Result<proto::ReduceResponse, Status>>;
    async fn reduce_fn(
        &self,
        request: Request<tonic::Streaming<proto::ReduceRequest>>,
    ) -> Result<Response<Self::ReduceFnStream>, Status> {
        // get gRPC window from metadata
        let (start_win, end_win) = get_window_details(request.metadata());
        let md = Metadata::new(IntervalWindow::new(start_win, end_win));

        let mut key_to_tx: HashMap<String, Sender<ReduceRequest>> = HashMap::new();

        // we will be creating a set of tasks for this stream
        let mut set = JoinSet::new();

        let mut stream = request.into_inner();

        while let Some(rr) = stream.message().await.unwrap() {
            let task_name = rr.keys.join(KEY_JOIN_DELIMITER);

            if let Some(tx) = key_to_tx.get(&task_name) {
                tx.send(rr.into()).await.unwrap();
            } else {
                // channel to send data to the user's reduce handle
                let (tx, rx) = mpsc::channel::<ReduceRequest>(1);

                // since we are calling this in a loop, we need make sure that there is reference counting
                // and the lifetime of self is more than the async function.
                // try Arc<Self> https://doc.rust-lang.org/reference/items/associated-items.html#methods ?
                let v = Arc::clone(&self.handler);

                // spawn task for each unique key
                let keys = rr.keys.clone();
                let reduce_md = md.clone();
                set.spawn(async move { v.reduce(keys, rx, reduce_md).await });

                // write data into the channel
                tx.send(rr.into()).await.unwrap();

                // save the key and for future look up as long as the stream is active
                key_to_tx.insert(task_name, tx);
            }
        }

        // close all the tx channels to tasks to close their corresponding rx
        key_to_tx.clear();

        // channel to respond to numaflow main car as it expects streaming results.
        let (tx, rx) = mpsc::channel::<Result<proto::ReduceResponse, Status>>(1);

        // start the result streamer
        tokio::spawn(async move {
            while let Some(res) = set.join_next().await {
                let messages = res.unwrap();
                let mut datum_responses = vec![];
                for message in messages {
                    datum_responses.push(proto::reduce_response::Result {
                        keys: message.keys,
                        value: message.value,
                        tags: message.tags,
                    });
                }
                // stream it out to the client
                tx.send(Ok(proto::ReduceResponse {
                    results: datum_responses,
                }))
                .await
                .unwrap();
            }
        });

        // return the rx as the streaming endpoint
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}

/// gRPC server to start a reduce service
#[derive(Debug)]
pub struct Server<T> {
    sock_addr: PathBuf,
    max_message_size: usize,
    server_info_file: PathBuf,
    svc: Option<T>,
}

impl<T> Server<T> {
    /// Create a new Server with the given reduce service
    pub fn new(reduce_svc: T) -> Self {
        Server {
            sock_addr: DEFAULT_SOCK_ADDR.into(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            server_info_file: DEFAULT_SERVER_INFO_FILE.into(),
            svc: Some(reduce_svc),
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    /// Default value is `/var/run/numaflow/reduce.sock`
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.sock_addr = file.into();
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections. Default value is `/var/run/numaflow/reduce.sock`
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

    /// Change the file in which numaflow server information is stored on start up to the new value. Default value is `/var/run/numaflow/reducer-server-info`
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.server_info_file = file.into();
        self
    }

    /// Get the path to the file where numaflow server info is stored. Default value is `/var/run/numaflow/reducer-server-info`
    pub fn server_info_file(&self) -> &std::path::Path {
        self.server_info_file.as_path()
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown<F>(
        &mut self,
        shutdown: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
        where
            T: Reducer + Send + Sync + 'static,
            F: Future<Output = ()>,
    {
        let listener = shared::create_listener_stream(&self.sock_addr, &self.server_info_file)?;
        let handler = Arc::new(self.svc.take().unwrap());
        let reduce_svc = ReduceService { handler };
        let reduce_svc = proto::reduce_server::ReduceServer::new(reduce_svc)
            .max_encoding_message_size(self.max_message_size)
            .max_decoding_message_size(self.max_message_size);

        tonic::transport::Server::builder()
            .add_service(reduce_svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await
            .map_err(Into::into)
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the signal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
        where
            T: Reducer + Send + Sync + 'static,
    {
        self.start_with_shutdown(shared::shutdown_signal()).await
    }
}
