use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{async_trait, Request, Response, Status};

use crate::error::Error;
use crate::error::Error::ReduceError;
use crate::error::ErrorKind::{InternalError, UserDefinedError};
use crate::shared;
use crate::shared::prost_timestamp_from_utc;

const KEY_JOIN_DELIMITER: &str = ":";
const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_SOCK_ADDR: &str = "/var/run/numaflow/reduce.sock";
const DEFAULT_SERVER_INFO_FILE: &str = "/var/run/numaflow/reducer-server-info";
const DROP: &str = "U+005C__DROP__";

/// Numaflow Reduce Proto definitions.
pub mod proto {
    tonic::include_proto!("reduce.v1");
}

struct ReduceService<C> {
    creator: Arc<C>,
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
    ///             while input.recv().await.is_some() {
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
#[derive(Default, Clone)]
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
    pub tags: Option<Vec<String>>,
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
    /// use numaflow::reduce::Message;
    /// let dropped_message = Message::message_to_drop();
    /// ```
    pub fn message_to_drop() -> crate::map::Message {
        crate::map::Message {
            keys: None,
            value: vec![],
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

// TODO: improve error handling, avoid panics and make sure the errors are propagated to the client.
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
        // Clone the creator and response_stream since we need to move them into the spawned task
        let creator = Arc::clone(&self.creator);
        let (response_tx, response_rx) = channel::<Result<proto::ReduceResponse, Status>>(1);

        // Spawn a new task to handle the incoming ReduceRequests from the client
        tokio::spawn(async move {
            // Create a new TaskManager to manage the reduce tasks.
            let (error_tx, mut error_rx) = channel::<Error>(1);
            let mut task_manager = TaskManager::new(creator, response_tx.clone(), error_tx);

            // Spawn a new task to handle errors
            tokio::spawn(async move {
                while let Some(error) = error_rx.recv().await {
                    response_tx.send(Err(error.into())).await.unwrap();
                }
            });

            let mut stream = request.into_inner();
            while let Some(rr) = stream.next().await {
                match rr {
                    Ok(rr) => {
                        let keys = rr.payload.as_ref().unwrap().keys.clone();
                        let task_name = keys.join(KEY_JOIN_DELIMITER);

                        if task_manager.tasks.contains_key(&task_name) {
                            let _ = task_manager.append_task(keys, rr).await;
                        } else {
                            let _ = task_manager.create_task(keys, rr).await;
                        }
                    }
                    Err(e) => {
                        return Err(Status::internal(format!("Stream error: {}", e)));
                    }
                }
            }

            task_manager.close_all_tasks().await;
            Ok(())
        });

        // return the rx as the streaming endpoint
        Ok(Response::new(ReceiverStream::new(response_rx)))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}

/// The `Task` struct represents a task in the reduce service.
/// It is responsible for invoking the user's reducer and sending the response back to the client.
struct Task {
    tx: Sender<ReduceRequest>,
    error_tx: Sender<Error>,
    handle: tokio::task::JoinHandle<()>,
}

impl Task {
    /// Creates a new `Task` with the given reducer, keys, metadata, and response sender.
    /// It starts the reducer in a new task and returns a `Task` struct that can be used to send `ReduceRequest`s to the reducer.
    async fn new<R: Reducer + Send + Sync + 'static>(
        reducer: R,
        keys: Vec<String>,
        md: Metadata,
        response_tx: Sender<Result<proto::ReduceResponse, Status>>,
        error_tx: Sender<Error>,
    ) -> Self {
        let (tx, rx) = channel::<ReduceRequest>(1);

        let handle = tokio::spawn(async move {
            let messages = reducer.reduce(keys, rx, &md).await;
            for message in messages {
                response_tx
                    .send(Ok(proto::ReduceResponse {
                        result: Some(proto::reduce_response::Result {
                            keys: message.keys.unwrap_or_default(),
                            value: message.value,
                            tags: message.tags.unwrap_or_default(),
                        }),
                        window: Some(proto::Window {
                            start: prost_timestamp_from_utc(md.interval_window.start_time),
                            end: prost_timestamp_from_utc(md.interval_window.end_time),
                            slot: "slot-0".to_string(),
                        }),
                        eof: false,
                    }))
                    .await
                    .unwrap();
            }
        });

        Self {
            tx,
            handle,
            error_tx,
        }
    }

    /// Sends a `ReduceRequest` to the task.
    async fn send(&self, rr: ReduceRequest) {
        self.tx
            .send(rr)
            .await
            .expect("send failed, receiver dropped")
    }

    /// Closes the task and waits for it to finish.
    async fn close(self) {
        // drop the sender to close the task and wait for the task to finish
        drop(self.tx);

        // if join handle fails, send an error to the error stream
        match self.handle.await {
            Ok(_) => {}
            Err(e) => {
                let _ = self
                    .error_tx
                    .send(ReduceError(UserDefinedError(format!(" {}", e))))
                    .await;
            }
        }
    }
}

/// The `TaskManager` struct manages tasks in the reduce service.
/// It stores a map of keys to tasks, and is responsible for creating, appending, and closing tasks.
/// It also sends an EOF message to the response stream when all tasks are closed.
struct TaskManager<C> {
    tasks: HashMap<String, Task>,
    response_stream: Sender<Result<proto::ReduceResponse, Status>>,
    error_stream: Sender<Error>,
    creator: Arc<C>,
    window: IntervalWindow,
}

impl<C> TaskManager<C>
where
    C: ReducerCreator + Send + Sync + 'static,
{
    /// Creates a new `TaskManager` with the given `ReducerCreator` and response stream.
    fn new(
        creator: Arc<C>,
        response_stream: Sender<Result<proto::ReduceResponse, Status>>,
        error_stream: Sender<Error>,
    ) -> Self {
        Self {
            tasks: HashMap::new(),
            response_stream,
            error_stream,
            creator,
            window: IntervalWindow::default(),
        }
    }

    /// Creates a new task with the given keys and `ReduceRequest`.
    /// It creates a new reducer, starts it in a new task, and adds the task to the task manager.
    async fn create_task(&mut self, keys: Vec<String>, rr: proto::ReduceRequest) {
        let (reduce_request, interval_window) = match self.validate_and_extract(rr).await {
            Some(value) => value,
            None => return,
        };

        self.window = interval_window.clone();

        // Create a new reducer
        let reducer = self.creator.create();

        // Create Metadata with the extracted start and end time
        let md = Metadata::new(interval_window);

        // Create a new Task and add it to the TaskManager
        let task = Task::new(
            reducer,
            keys.clone(),
            md,
            self.response_stream.clone(),
            self.error_stream.clone(),
        )
        .await;

        self.tasks.insert(keys.join(KEY_JOIN_DELIMITER), task);

        // send the request inside the proto payload to the task
        // if the task does not exist, send an error to the stream
        if let Some(task) = self.tasks.get(&keys.join(KEY_JOIN_DELIMITER)) {
            task.send(reduce_request).await;
        } else {
            self.handle_error(ReduceError(InternalError("Task not found".to_string())))
                .await;
        }
    }

    /// Appends a `ReduceRequest` to an existing task with the given keys.
    async fn append_task(&mut self, keys: Vec<String>, rr: proto::ReduceRequest) {
        let (reduce_request, _) = match self.validate_and_extract(rr).await {
            Some(value) => value,
            None => return,
        };

        // Get the task name from the keys
        let task_name = keys.join(KEY_JOIN_DELIMITER);

        // If the task exists, send the ReduceRequest to the task
        if let Some(task) = self.tasks.get(&task_name) {
            task.send(reduce_request).await;
        } else {
            self.handle_error(ReduceError(InternalError("Task not found".to_string())))
                .await;
        }
    }

    // Validates the ReduceRequest and extracts the payload and window information.
    // If the ReduceRequest is invalid, it sends an error to the response stream and returns None.
    async fn validate_and_extract(
        &self,
        rr: proto::ReduceRequest,
    ) -> Option<(ReduceRequest, IntervalWindow)> {
        // Extract the payload and window information from the ReduceRequest
        let (payload, windows) = match (rr.payload, rr.operation) {
            (Some(payload), Some(operation)) => (payload, operation.windows),
            _ => {
                self.handle_error(ReduceError(InternalError(
                    "Invalid ReduceRequest".to_string(),
                )))
                .await;
                return None;
            }
        };

        // Check if there is exactly one window in the ReduceRequest
        if windows.len() != 1 {
            self.handle_error(ReduceError(InternalError(
                "Exactly one window is required".to_string(),
            )))
            .await;
            return None;
        }

        // Extract the start and end time from the window
        let window = &windows[0];
        let (start_time, end_time) = (
            shared::utc_from_timestamp(window.start.clone()),
            shared::utc_from_timestamp(window.end.clone()),
        );

        // Create the IntervalWindow
        let interval_window = IntervalWindow::new(start_time, end_time);

        // Create the ReduceRequest
        let reduce_request = ReduceRequest {
            keys: payload.keys,
            value: payload.value,
            watermark: shared::utc_from_timestamp(payload.watermark),
            eventtime: shared::utc_from_timestamp(payload.event_time),
        };

        Some((reduce_request, interval_window))
    }

    /// Closes all tasks in the task manager and sends an EOF message to the response stream.
    async fn close_all_tasks(&mut self) {
        for (_, task) in self.tasks.drain() {
            Task::close(task).await;
        }

        // after all the tasks have been closed, send an EOF message to the response stream

        self.response_stream
            .send(Ok(proto::ReduceResponse {
                result: None,
                window: Some(proto::Window {
                    start: prost_timestamp_from_utc(self.window.start_time),
                    end: prost_timestamp_from_utc(self.window.end_time),
                    slot: "slot-0".to_string(),
                }),
                eof: true,
            }))
            .await
            .unwrap();
    }

    async fn handle_error(&self, error: Error) {
        let _ = self.error_stream.send(error).await;
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
        let reduce_svc = ReduceService {
            creator: Arc::new(creator),
        };
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

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};
    use std::path::PathBuf;

    use prost_types::Timestamp;
    use tempfile::TempDir;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::Request;
    use tonic::transport::Uri;
    use tower::service_fn;

    use crate::reduce;
    use crate::reduce::proto::reduce_client::ReduceClient;

    struct Sum;
    #[tonic::async_trait]
    impl reduce::Reducer for Sum {
        async fn reduce(
            &self,
            _keys: Vec<String>,
            mut input: mpsc::Receiver<reduce::ReduceRequest>,
            _md: &reduce::Metadata,
        ) -> Vec<reduce::Message> {
            let mut sum = 0;
            while let Some(rr) = input.recv().await {
                sum += std::str::from_utf8(&rr.value)
                    .unwrap()
                    .parse::<i32>()
                    .unwrap();
            }
            vec![reduce::Message::new(sum.to_string().into_bytes())]
        }
    }

    struct SumCreator;
    impl reduce::ReducerCreator for SumCreator {
        type R = Sum;
        fn create(&self) -> Sum {
            Sum {}
        }
    }

    async fn setup_server<C: reduce::ReducerCreator + Send + Sync + 'static>(
        creator: C,
    ) -> Result<(reduce::Server<C>, PathBuf, PathBuf), Box<dyn Error>> {
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("reduce.sock");
        let server_info_file = tmp_dir.path().join("reducer-server-info");

        let server = reduce::Server::new(creator)
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file)
            .with_max_message_size(10240);

        Ok((server, sock_file, server_info_file))
    }

    async fn setup_client(
        sock_file: PathBuf,
    ) -> Result<ReduceClient<tonic::transport::Channel>, Box<dyn Error>> {
        // https://github.com/hyperium/tonic/blob/master/examples/src/uds/client.rs
        let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(service_fn(move |_: Uri| {
                // Connect to an Uds socket
                let sock_file = sock_file.clone();
                tokio::net::UnixStream::connect(sock_file)
            }))
            .await?;

        let client = ReduceClient::new(channel);

        Ok(client)
    }

    #[tokio::test]
    async fn test_server_start() -> Result<(), Box<dyn Error>> {
        let (mut server, sock_file, server_info_file) = setup_server(SumCreator).await?;

        assert_eq!(server.max_message_size(), 10240);
        assert_eq!(server.server_info_file(), server_info_file);
        assert_eq!(server.socket_file(), sock_file);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let shutdown = async {
            shutdown_rx.await.unwrap();
        };

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Check if the server has started
        assert!(!task.is_finished(), "gRPC server should be running");

        // Send shutdown signal
        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");

        // Check if the server has stopped within 1 second
        for _ in 0..100 {
            if task.is_finished() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(task.is_finished(), "gRPC server should have stopped");

        Ok(())
    }

    #[tokio::test]
    async fn valid_input() -> Result<(), Box<dyn Error>> {
        let (mut server, sock_file, _) = setup_server(SumCreator).await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let shutdown = async {
            shutdown_rx.await.unwrap();
        };

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = setup_client(sock_file).await?;

        let (tx, rx) = mpsc::channel(1);

        // Spawn a task to send ReduceRequests to the channel
        tokio::spawn(async move {
            let data = vec![("key1".to_string(), 1..=10), ("key2".to_string(), 1..=9)];

            for (key, range) in data {
                for i in range {
                    let rr = reduce::proto::ReduceRequest {
                        payload: Some(reduce::proto::reduce_request::Payload {
                            keys: vec![key.clone()],
                            value: i.to_string().as_bytes().to_vec(),
                            watermark: None,
                            event_time: None,
                            headers: Default::default(),
                        }),
                        operation: Some(reduce::proto::reduce_request::WindowOperation {
                            event: 0,
                            windows: vec![reduce::proto::Window {
                                start: Some(Timestamp {
                                    seconds: 60000,
                                    nanos: 0,
                                }),
                                end: Some(Timestamp {
                                    seconds: 120000,
                                    nanos: 0,
                                }),
                                slot: "slot-0".to_string(),
                            }],
                        }),
                    };

                    tx.send(rr).await.unwrap();
                }
            }
        });

        // Convert the receiver end of the channel into a stream
        let stream = ReceiverStream::new(rx);

        // Create a tonic::Request from the stream
        let request = Request::new(stream);

        // Send the request to the server
        let resp = client.reduce_fn(request).await?;

        let mut response_stream = resp.into_inner();
        let mut responses = Vec::new();

        while let Some(response) = response_stream.message().await? {
            responses.push(response);
        }

        // since we are sending two different keys, we should get two responses + 1 EOF
        assert_eq!(responses.len(), 3);

        for (i, response) in responses.iter().enumerate() {
            if let Some(window) = response.window.as_ref() {
                if let Some(start) = window.start.as_ref() {
                    assert_eq!(start.seconds, 60000);
                }
                if let Some(end) = window.end.as_ref() {
                    assert_eq!(end.seconds, 120000);
                }
            }

            if let Some(result) = response.result.as_ref() {
                if result.keys == vec!["key1".to_string()] {
                    assert_eq!(result.value, 55.to_string().into_bytes());
                } else if result.keys == vec!["key2".to_string()] {
                    assert_eq!(result.value, 45.to_string().into_bytes());
                }
            }

            // Check if this is the last message in the stream
            // The last message should have eof set to true
            if i == responses.len() - 1 {
                assert_eq!(response.eof, true);
            } else {
                assert_eq!(response.eof, false);
            }
        }

        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");

        Ok(())
    }

    #[tokio::test]
    async fn invalid_input() -> Result<(), Box<dyn Error>> {
        let (mut server, sock_file, _) = setup_server(SumCreator).await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let shutdown = async {
            shutdown_rx.await.unwrap();
        };

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = setup_client(sock_file).await?;

        let (tx, rx) = mpsc::channel(1);

        // Spawn a task to send ReduceRequests to the channel
        tokio::spawn(async move {
            let rr = reduce::proto::ReduceRequest {
                payload: Some(reduce::proto::reduce_request::Payload {
                    keys: vec!["key1".to_string()],
                    value: vec![],
                    watermark: None,
                    event_time: None,
                    headers: Default::default(),
                }),
                operation: Some(reduce::proto::reduce_request::WindowOperation {
                    event: 0,
                    windows: vec![
                        reduce::proto::Window {
                            start: Some(Timestamp {
                                seconds: 60000,
                                nanos: 0,
                            }),
                            end: Some(Timestamp {
                                seconds: 120000,
                                nanos: 0,
                            }),
                            slot: "slot-0".to_string(),
                        },
                        reduce::proto::Window {
                            start: Some(Timestamp {
                                seconds: 60000,
                                nanos: 0,
                            }),
                            end: Some(Timestamp {
                                seconds: 120000,
                                nanos: 0,
                            }),
                            slot: "slot-0".to_string(),
                        },
                    ],
                }),
            };

            tx.send(rr).await.unwrap();
        });

        // Convert the receiver end of the channel into a stream
        let stream = ReceiverStream::new(rx);

        // Create a tonic::Request from the stream
        let request = Request::new(stream);

        // Send the request to the server
        let resp = client.reduce_fn(request).await?;

        let mut response_stream = resp.into_inner();

        while let Err(e) = response_stream.message().await {
            assert!(e.to_string().contains("Exactly one window is required"));
            break;
        }

        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");

        Ok(())
    }

    struct PanicReducer;
    #[tonic::async_trait]
    impl reduce::Reducer for PanicReducer {
        async fn reduce(
            &self,
            _keys: Vec<String>,
            _input: mpsc::Receiver<reduce::ReduceRequest>,
            _md: &reduce::Metadata,
        ) -> Vec<reduce::Message> {
            panic!("Panic in reduce method");
        }
    }

    struct PanicReducerCreator;
    impl reduce::ReducerCreator for PanicReducerCreator {
        type R = PanicReducer;
        fn create(&self) -> PanicReducer {
            PanicReducer {}
        }
    }

    #[tokio::test]
    async fn panic_in_reduce() -> Result<(), Box<dyn Error>> {
        let (mut server, sock_file, _) = setup_server(PanicReducerCreator).await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let shutdown = async {
            shutdown_rx.await.unwrap();
        };

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = setup_client(sock_file).await?;

        let (tx, rx) = mpsc::channel(1);

        // Spawn a task to send ReduceRequests to the channel
        tokio::spawn(async move {
            let rr = reduce::proto::ReduceRequest {
                payload: Some(reduce::proto::reduce_request::Payload {
                    keys: vec!["key1".to_string()],
                    value: vec![],
                    watermark: None,
                    event_time: None,
                    headers: Default::default(),
                }),
                operation: Some(reduce::proto::reduce_request::WindowOperation {
                    event: 0,
                    windows: vec![reduce::proto::Window {
                        start: Some(Timestamp {
                            seconds: 60000,
                            nanos: 0,
                        }),
                        end: Some(Timestamp {
                            seconds: 120000,
                            nanos: 0,
                        }),
                        slot: "slot-0".to_string(),
                    }],
                }),
            };

            tx.send(rr).await.unwrap();
        });

        // Convert the receiver end of the channel into a stream
        let stream = ReceiverStream::new(rx);

        // Create a tonic::Request from the stream
        let request = Request::new(stream);

        // Send the request to the server
        let resp = client.reduce_fn(request).await?;

        let mut response_stream = resp.into_inner();

        while let Err(e) = response_stream.message().await {
            assert_eq!(e.code(), tonic::Code::Unknown);
            break;
        }

        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");

        Ok(())
    }
}
