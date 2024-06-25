use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
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
    shutdown_tx: Sender<()>,
    cancellation_token: CancellationToken, // used to cancel all the tasks
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
#[derive(Default, Clone, Debug)]
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

#[derive(Debug)]
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
    /// Headers for the message.
    pub headers: HashMap<String, String>,
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
        // Clone the creator and shutdown_tx to be used in the spawned tasks.
        let creator = Arc::clone(&self.creator);
        let shutdown_tx = self.shutdown_tx.clone();

        // Create a channel to send the response back to the grpc client.
        let (grpc_response_tx, grpc_response_rx) =
            channel::<Result<proto::ReduceResponse, Status>>(1);

        // Internal response channel which will be used by the task set and tasks to send the response after
        // executing the user defined function. It's a result type so in case of error, we can send the error
        // back to the client.
        //
        // NOTE: we are using a separate channel instead of the grpc_response_tx because in case of errors,
        // we have to do graceful shutdown.
        let (response_tx, mut response_rx) = channel::<Result<proto::ReduceResponse, Error>>(1);

        // Start a task executor to handle the incoming ReduceRequests from the client, returns a tx to send
        // commands to the task executor and an oneshot tx to abort all the tasks.
        let (task_tx, abort_tx) = TaskSet::start_task_executor(creator, response_tx.clone());

        // Spawn a new task to listen to the response channel and send the response back to the grpc client.
        // In case of error, it propagates the error back to the client in grpc status format and sends a shutdown
        // signal to the grpc server. It also listens to the cancellation signal and aborts all the tasks.
        let response_task_token = self.cancellation_token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = response_rx.recv() => {
                        match result {
                            Some(Ok(response)) => {
                                let eof = response.eof;
                                grpc_response_tx
                                    .send(Ok(response))
                                    .await
                                    .expect("send to grpc response channel failed");
                                if eof {
                                    break;
                                }
                            }
                            Some(Err(error)) => {
                                grpc_response_tx
                                    .send(Err(Status::internal(error.to_string())))
                                    .await
                                    .expect("send to grpc response channel failed");
                                // Send a shutdown signal to the grpc server.
                                shutdown_tx.send(()).await.expect("shutdown_tx send failed");
                            }
                            None => {
                                // response_rx is closed, break the loop
                                break;
                            }
                        }
                    }
                    _ = response_task_token.cancelled() => {
                        // Send an abort signal to the task executor to abort all the tasks.
                        abort_tx.send(()).expect("task_tx send failed");
                        break;
                    }
                }
            }
        });

        let request_cancel_token = self.cancellation_token.clone();
        // Spawn a new task to handle the incoming ReduceRequests from the client
        tokio::spawn(async move {
            let mut stream = request.into_inner();
            loop {
                tokio::select! {
                    reduce_request = stream.next() => {
                        match reduce_request {
                            Some(Ok(rr)) => {
                                task_tx
                                    .send(TaskCommand::HandleReduceRequest(rr))
                                    .await
                                    .expect("task_tx send failed");
                            }
                            Some(Err(e)) => {
                                response_tx
                                    .send(Err(ReduceError(InternalError(format!(
                                        "Failed to receive request: {}",
                                        e
                                    )))))
                                    .await
                                    .expect("error_tx send failed");
                            }
                            None => break,
                        }
                    }
                    _ = request_cancel_token.cancelled() => {
                        break;
                    }
                }
            }
            task_tx
                .send(TaskCommand::Close)
                .await
                .expect("task_tx send failed");
        });

        // return the rx as the streaming endpoint
        Ok(Response::new(ReceiverStream::new(grpc_response_rx)))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}

// The `Task` struct represents a task in the reduce service. It is responsible for executing the
// user defined function. We will a separate task for each keyed window. The task will be created
// when the first message for a given key arrives and will be closed when the window is closed.
struct Task {
    udf_tx: Sender<ReduceRequest>,
    response_tx: Sender<Result<proto::ReduceResponse, Error>>,
    done_rx: oneshot::Receiver<()>,
    handle: tokio::task::JoinHandle<()>,
}

impl Task {
    // Creates a new task with the given reducer, keys, metadata, and response channel.
    async fn new<R: Reducer + Send + Sync + 'static>(
        reducer: R,
        keys: Vec<String>,
        md: Metadata,
        response_tx: Sender<Result<proto::ReduceResponse, Error>>,
    ) -> Self {
        let (udf_tx, udf_rx) = channel::<ReduceRequest>(1);
        let (done_tx, done_rx) = oneshot::channel();

        let udf_response_tx = response_tx.clone();
        let task_handle = tokio::spawn(async move {
            let messages = reducer.reduce(keys, udf_rx, &md).await;
            for message in messages {
                let send_result = udf_response_tx
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
                    .await;

                if let Err(e) = send_result {
                    let _ = udf_response_tx
                        .send(Err(ReduceError(InternalError(format!(
                            "Failed to send response back: {}",
                            e
                        )))))
                        .await;
                    return;
                }
            }
        });

        // We spawn a separate task to await the handle so that in case of any unhandled errors in the user-defined
        // code will immediately be propagated to the client.
        let handler_tx = response_tx.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = task_handle.await {
                let _ = handler_tx
                    .send(Err(ReduceError(UserDefinedError(format!(" {}", e)))))
                    .await;
            }

            // Send a message indicating that the task has finished
            let _ = done_tx.send(());
        });

        // We store the task handle so that we can abort the task if needed, we only need the second task handle because
        // if the second task is aborted, the first task's handle will be dropped and the task will be aborted.
        Self {
            udf_tx,
            response_tx,
            done_rx,
            handle,
        }
    }

    // Sends the request to the user defined function's input channel.
    async fn send(&self, rr: ReduceRequest) {
        if let Err(e) = self.udf_tx.send(rr).await {
            self.response_tx
                .send(Err(ReduceError(InternalError(format!(
                    "Failed to send message to task: {}",
                    e
                )))))
                .await
                .expect("failed to send message to error channel");
        }
    }

    // Closes the task and waits for it to finish.
    async fn close(self) {
        // drop the sender to close the task
        drop(self.udf_tx);

        // Wait for the task to finish
        let _ = self.done_rx.await;
    }

    // Aborts the task.
    async fn abort(self) {
        self.handle.abort();
    }
}

// The `TaskSet` struct represents a set of tasks that are executing the user defined function. It is responsible
// for creating new tasks, writing messages to the tasks, closing the tasks, and aborting the tasks.
struct TaskSet<C> {
    tasks: HashMap<String, Task>,
    response_tx: Sender<Result<proto::ReduceResponse, Error>>,
    creator: Arc<C>,
    window: IntervalWindow,
}

enum TaskCommand {
    HandleReduceRequest(proto::ReduceRequest),
    Close,
}

impl<C> TaskSet<C>
where
    C: ReducerCreator + Send + Sync + 'static,
{
    // Starts a new task executor which listens to incoming commands and executes them.
    // returns a tx to send commands to the task executor and oneshot tx to abort all
    // the tasks to gracefully shut down the task executor.
    fn start_task_executor(
        creator: Arc<C>,
        response_tx: Sender<Result<proto::ReduceResponse, Error>>,
    ) -> (Sender<TaskCommand>, oneshot::Sender<()>) {
        let (task_tx, mut task_rx) = channel::<TaskCommand>(1);
        let (abort_tx, mut abort_rx) = oneshot::channel();

        let mut task_set = TaskSet {
            tasks: HashMap::new(),
            response_tx,
            creator,
            window: IntervalWindow::default(),
        };

        // Start a new task to listen to incoming commands and execute them, it will also listen to the abort signal.
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    cmd = task_rx.recv() => {
                        match cmd {
                            Some(TaskCommand::HandleReduceRequest(rr)) => {
                               // Extract the keys from the ReduceRequest.
                                let keys = match rr.payload.as_ref() {
                                    Some(payload) => payload.keys.clone(),
                                    None => {
                                        task_set
                                            .handle_error(ReduceError(InternalError(
                                                "Invalid ReduceRequest".to_string(),
                                            )))
                                            .await;
                                        continue;
                                    }
                                };

                                // Check if the task already exists, if it does, write the ReduceRequest to the task,
                                // otherwise create a new task and write the ReduceRequest to the task.
                                if task_set.tasks.contains_key(&keys.join(KEY_JOIN_DELIMITER)) {
                                    task_set.write_to_task(keys, rr).await;
                                } else {
                                    task_set.create_and_write(keys, rr).await;
                                }
                            }
                            Some(TaskCommand::Close) => task_set.close().await,
                            None => break,
                        }
                    }
                    _ = &mut abort_rx => {
                        task_set.abort().await;
                        break;
                    }
                }
            }
        });

        (task_tx, abort_tx)
    }

    // Creates a new task with the given keys and `ReduceRequest`.
    // It creates a new reducer and assigns it to the task to execute the user defined function.
    async fn create_and_write(&mut self, keys: Vec<String>, rr: proto::ReduceRequest) {
        let (reduce_request, interval_window) = match self.validate_and_extract(rr).await {
            Some(value) => value,
            None => return,
        };

        self.window = interval_window.clone();

        // Create a new reducer
        let reducer = self.creator.create();

        // Create Metadata with the extracted start and end time
        let md = Metadata::new(interval_window);

        // Create a new Task with the reducer, keys, and metadata
        let task = Task::new(reducer, keys.clone(), md, self.response_tx.clone()).await;

        // track the task in the task set
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

    // Writes the ReduceRequest to the task with the given keys.
    async fn write_to_task(&mut self, keys: Vec<String>, rr: proto::ReduceRequest) {
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
            headers: Default::default(),
        };

        Some((reduce_request, interval_window))
    }

    // Closes all tasks in the task set and sends an EOF message to the response stream.
    async fn close(&mut self) {
        for (_, task) in self.tasks.drain() {
            task.close().await;
        }

        // after all the tasks have been closed, send an EOF message to the response stream
        let send_eof = self
            .response_tx
            .send(Ok(proto::ReduceResponse {
                result: None,
                window: Some(proto::Window {
                    start: prost_timestamp_from_utc(self.window.start_time),
                    end: prost_timestamp_from_utc(self.window.end_time),
                    slot: "slot-0".to_string(),
                }),
                eof: true,
            }))
            .await;

        if let Err(e) = send_eof {
            self.handle_error(ReduceError(InternalError(format!(
                "Failed to send EOF message: {}",
                e
            ))))
            .await;
        }
    }

    // Aborts all tasks in the task set.
    async fn abort(&mut self) {
        for (_, task) in self.tasks.drain() {
            task.abort().await;
        }
    }

    // Sends an error to the response stream.
    async fn handle_error(&self, error: Error) {
        self.response_tx
            .send(Err(error))
            .await
            .expect("error_tx send failed");
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
    pub async fn start_with_shutdown(
        &mut self,
        user_shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        C: ReducerCreator + Send + Sync + 'static,
    {
        let listener = shared::create_listener_stream(&self.sock_addr, &self.server_info_file)?;
        let creator = self.creator.take().unwrap();
        let (internal_shutdown_tx, internal_shutdown_rx) = channel(1);
        let cln_token = CancellationToken::new();
        let reduce_svc = ReduceService {
            creator: Arc::new(creator),
            shutdown_tx: internal_shutdown_tx,
            cancellation_token: cln_token.clone(),
        };
        let reduce_svc = proto::reduce_server::ReduceServer::new(reduce_svc)
            .max_encoding_message_size(self.max_message_size)
            .max_decoding_message_size(self.max_message_size);

        let shutdown =
            shared::shutdown_signal(internal_shutdown_rx, Some(user_shutdown_rx), cln_token);

        tonic::transport::Server::builder()
            .add_service(reduce_svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await?;

        Ok(())
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the signal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        C: ReducerCreator + Send + Sync + 'static,
    {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        self.start_with_shutdown(shutdown_rx).await
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::{error::Error, time::Duration};

    use prost_types::Timestamp;
    use tempfile::TempDir;
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::sleep;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::Uri;
    use tonic::Request;
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

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Check if the server has started
        assert!(!task.is_finished(), "gRPC server should be running");

        // Send shutdown signal
        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");

        // Check if the server has stopped within 100 ms
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if task.is_finished() {
                break;
            }
        }
        assert!(task.is_finished(), "gRPC server is still running");

        Ok(())
    }

    #[tokio::test]
    async fn valid_input() -> Result<(), Box<dyn Error>> {
        let (mut server, sock_file, _) = setup_server(SumCreator).await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

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
                assert!(response.eof);
            } else {
                assert!(!response.eof);
            }
        }

        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");

        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if task.is_finished() {
                break;
            }
        }
        assert!(task.is_finished(), "gRPC server is still running");

        Ok(())
    }

    #[tokio::test]
    async fn invalid_input() -> Result<(), Box<dyn Error>> {
        let (mut server, sock_file, _) = setup_server(SumCreator).await?;

        let (_shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = setup_client(sock_file).await?;

        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn a task to send ReduceRequests to the channel
        let _sender_task = tokio::spawn(async move {
            for _ in 0..10 {
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

                tx.send(rr).unwrap();
                sleep(Duration::from_millis(10)).await;
            }
        });

        // Send the request to the server
        let resp = client
            .reduce_fn(Request::new(
                tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
            ))
            .await;

        let mut response_stream = resp.unwrap().into_inner();

        if let Err(e) = response_stream.message().await {
            assert_eq!(e.code(), tonic::Code::Internal);
        }

        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if task.is_finished() {
                break;
            }
        }

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

        let (_shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = setup_client(sock_file.clone()).await?;

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

            loop {
                tx.send(rr.clone()).await.unwrap();
                sleep(Duration::from_millis(10)).await;
            }
        });

        // Convert the receiver end of the channel into a stream
        let stream = ReceiverStream::new(rx);

        // Create a tonic::Request from the stream
        let request = Request::new(stream);

        // Send the request to the server
        let resp = client.reduce_fn(request).await?;

        let mut response_stream = resp.into_inner();

        while let Err(e) = response_stream.message().await {
            assert_eq!(e.code(), tonic::Code::Internal);
        }

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
