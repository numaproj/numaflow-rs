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
const DROP: &str = "U+005C__DROP__";

/// Numaflow Reduce Proto definitions.
pub mod proto {
    tonic::include_proto!("reduce.v1");
}

struct ReduceService<C> {
    creator: C,
}

/// `ReducerCreator` is a trait for creating a new instance of a `Reducer`.
pub trait ReducerCreator {
    /// Each type that implements `ReducerCreator` must also specify an associated type `R` that implements the `Reducer` trait.
    /// The `create` method is used to create a new instance of this `Reducer` type.
    ///
    /// # Example
    ///
    /// Below is an example of how to implement the `ReducerCreator` trait for a specific type `MyReducerCreator`.
    /// `MyReducerCreator` creates instances of `MyReducer`, which is a type that implements the `Reducer` trait.
    ///
    /// ```rust
    /// use numaflow::reduce::{Reducer, ReducerCreator, ReduceRequest, Metadata, Message};
    /// use tokio::sync::mpsc::Receiver;
    /// use tonic::async_trait;
    ///
    /// pub struct MyReducer;
    ///
    /// #[async_trait]
    /// impl Reducer for MyReducer {
    ///     async fn reduce(
    ///         &self,
    ///         keys: Vec<String>,
    ///         mut input: Receiver<ReduceRequest>,
    ///         md: &Metadata,
    ///     ) -> Vec<Message> {
    ///         // Implementation of the reduce method goes here.
    ///         vec![]
    ///     }
    /// }
    ///
    /// pub struct MyReducerCreator;
    ///
    /// impl ReducerCreator for MyReducerCreator {
    ///     type R = MyReducer;
    ///
    ///     fn create(&self) -> Self::R {
    ///         MyReducer
    ///     }
    /// }
    /// ```
    type R: Reducer + Send + Sync + 'static;
    fn create(&self) -> Self::R;
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
    /// let handler_creator = counter::CounterCreator{};
    ///     reduce::Server::new(handler_creator).start().await?;
    ///     Ok(())
    /// }
    /// mod counter {
    ///     use numaflow::reduce::{Message, ReduceRequest};
    ///     use numaflow::reduce::{Reducer, Metadata};
    ///     use tokio::sync::mpsc::Receiver;
    ///     use tonic::async_trait;
    /// use numaflow::reduce::proto::reduce_server::Reduce;
    ///     pub(crate) struct Counter {}
    ///
    ///     pub(crate) struct CounterCreator {}
    ///
    ///    impl numaflow::reduce::ReducerCreator for CounterCreator {
    ///        type R = Counter;
    ///
    ///        fn create(&self) -> Self::R {
    ///           Counter::new()
    ///       }
    ///     }
    ///
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
    ///             md: &Metadata,
    ///         ) -> Vec<Message> {
    ///             let mut counter = 0;
    ///             // the loop exits when input is closed which will happen only on close of book.
    ///             while (input.recv().await).is_some() {
    ///                 counter += 1;
    ///             }
    ///             let message=Message::new(counter.to_string().into_bytes()).tags(vec![]).keys(keys.clone());
    ///             vec![message]
    ///         }
    ///     }
    /// }
    ///```
    /// [Window]: https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/windowing/
    async fn reduce(
        &self,
        keys: Vec<String>,
        input: mpsc::Receiver<ReduceRequest>,
        md: &Metadata,
    ) -> Vec<Message>;
}

/// IntervalWindow is the start and end boundary of the window.
pub struct IntervalWindow {
    // start time of the window
    pub start_time: DateTime<Utc>,
    // end time of the window
    pub end_time: DateTime<Utc>,
}

impl IntervalWindow {
    fn new(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> Self {
        Self {
            start_time,
            end_time,
        }
    }
}

impl Metadata {
    pub fn new(interval_window: IntervalWindow) -> Self {
        Self { interval_window }
    }
}

/// Metadata are additional information passed into the [`Reducer::reduce`].
pub struct Metadata {
    pub interval_window: IntervalWindow,
}

/// Message is the response from the user's [`Reducer::reduce`].
#[derive(Debug, PartialEq)]
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection. It is mainly used in creating a partition in [`Reducer::reduce`].
    pub keys: Option<Vec<String>>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags:Option<Vec<String>>,
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
    /// use numaflow::reduce::Message;
    /// let message = Message::new(vec![1, 2, 3, 4]);
    /// ```
    pub fn new(value :Vec<u8>) -> Self {
       Self{
           value,
           keys:None,
           tags:None

       }
    }
    /// Marks the message to be dropped by adding a special "DROP" tag.
    ///
    /// This function guarantees that the tags vector is initialized if it was previously `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::reduce::Message;
    /// let mut message = Message::new(vec![1, 2, 3]);
    /// let dropped_message = Message::message_to_drop(message);
    /// ```
    pub fn message_to_drop(mut message:Message) -> Message {
        if message.tags.is_none() {
            message.tags = Some(Vec::new());
        }
        message.tags.as_mut().unwrap().push(DROP.parse().unwrap());
        message
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
    ///  use numaflow::reduce::Message;
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
    ///  use numaflow::reduce::Message;
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
    /// use numaflow::reduce::Message;
    /// let message = Message::new(vec![1, 2, 3]).value(vec![4, 5, 6]);
    /// ```
    pub fn value(mut self, value: Vec<u8>) -> Self {
        self.value = value;
        self
    }
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
impl<C> proto::reduce_server::Reduce for ReduceService<C>
where
    C: ReducerCreator + Send + Sync + 'static,
{
    type ReduceFnStream = ReceiverStream<Result<proto::ReduceResponse, Status>>;
    async fn reduce_fn(
        &self,
        request: Request<tonic::Streaming<proto::ReduceRequest>>,
    ) -> Result<Response<Self::ReduceFnStream>, Status> {
        // get gRPC window from metadata
        let (start_win, end_win) = get_window_details(request.metadata());
        let md = Arc::new(Metadata::new(IntervalWindow::new(start_win, end_win)));

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
                let handler = self.creator.create();
                let m = Arc::clone(&md);

                // spawn task for each unique key
                let keys = rr.keys.clone();
                set.spawn(async move { handler.reduce(keys, rx, m.as_ref()).await });

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
                        keys: message.keys.unwrap_or_default(),
                        value: message.value,
                        tags: message.tags.unwrap_or_default(),
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
pub struct Server<C> {
    sock_addr: PathBuf,
    max_message_size: usize,
    server_info_file: PathBuf,
    creator: Option<C>,
}

impl<C> Server<C> {
    /// Create a new Server with the given reduce service
    pub fn new(creator: C) -> Self {
        Server {
            sock_addr: DEFAULT_SOCK_ADDR.into(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            server_info_file: DEFAULT_SERVER_INFO_FILE.into(),
            creator: Some(creator),
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
        F: Future<Output = ()>,
        C: ReducerCreator + Send + Sync + 'static,
    {
        let listener = shared::create_listener_stream(&self.sock_addr, &self.server_info_file)?;
        let creator = self.creator.take().unwrap();
        let reduce_svc = ReduceService { creator };
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
        C: ReducerCreator + Send + Sync + 'static,
    {
        self.start_with_shutdown(shared::shutdown_signal()).await
    }
}
