use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::{Sender, channel};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, async_trait};

use crate::error::{Error, ErrorKind};
pub use crate::proto::reduce as proto;
use crate::shared;
use shared::{ContainerType, build_panic_status, get_panic_info, prost_timestamp_from_utc};
use tracing::error;

/// Default socket address for reduce stream service
pub const SOCK_ADDR: &str = "/var/run/numaflow/reducestream.sock";

/// Default server info file for reduce stream service
pub const SERVER_INFO_FILE: &str = "/var/run/numaflow/reducestreamer-server-info";

const KEY_JOIN_DELIMITER: &str = ":";

struct ReduceStreamService<C> {
    creator: Arc<C>,
    shutdown_tx: Sender<()>,
    cancellation_token: CancellationToken,
}

/// `ReduceStreamerCreator` is a trait for creating a new instance of a `ReduceStreamer`.
pub trait ReduceStreamerCreator {
    /// Each type that implements `ReduceStreamerCreator` must also specify an associated type `R` that implements the `ReduceStreamer` trait.
    /// The `create` method is used to create a new instance of this `ReduceStreamer` type.
    ///
    /// # Example
    ///
    /// Below is an example of how to implement the `ReduceStreamerCreator` trait for a specific type `MyReduceStreamerCreator`.
    /// `MyReduceStreamerCreator` creates instances of `MyReduceStreamer`, which is a type that implements the `ReduceStreamer` trait.
    ///
    /// ```rust
    /// use numaflow::reducestream::{ReduceStreamer, ReduceStreamerCreator, ReduceStreamRequest, Metadata, Message};
    /// use tokio::sync::mpsc::{Receiver, Sender};
    /// use tonic::async_trait;
    ///
    /// pub struct MyReduceStreamer;
    ///
    /// #[async_trait]
    /// impl ReduceStreamer for MyReduceStreamer {
    ///     async fn reducestream(
    ///         &self,
    ///         keys: Vec<String>,
    ///         mut input: Receiver<ReduceStreamRequest>,
    ///         output: Sender<Message>,
    ///         md: &Metadata,
    ///     ) {
    ///         // Implementation of the reducestream method goes here.
    ///     }
    /// }
    ///
    /// pub struct MyReduceStreamerCreator;
    ///
    /// impl ReduceStreamerCreator for MyReduceStreamerCreator {
    ///     type R = MyReduceStreamer;
    ///
    ///     fn create(&self) -> Self::R {
    ///         MyReduceStreamer
    ///     }
    /// }
    /// ```
    type R: ReduceStreamer + Send + Sync + 'static;
    fn create(&self) -> Self::R;
}

/// ReduceStreamer trait for implementing Reduce Stream handler.
#[async_trait]
pub trait ReduceStreamer {
    /// reducestream is provided with a set of keys, an input channel of [`ReduceStreamRequest`],
    /// an output channel for streaming [`Message`] results, and [`Metadata`].
    /// Unlike reduce which returns a Vec of messages, reducestream allows you to stream results
    /// as they are produced by sending them to the output channel.
    ///
    /// Reduce stream is a stateful operation and the input channel is for the collection of keys
    /// and for that time [Window].
    /// You can read more about reduce [here](https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/reduce/).
    ///
    /// # Example
    ///
    /// Below is a reduce stream code to emit a running count as elements arrive for a given set of keys and window.
    ///
    /// ```no_run
    /// use numaflow::reducestream;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     let handler_creator = streaming_counter::StreamingCounterCreator{};
    ///     reducestream::Server::new(handler_creator).start().await?;
    ///     Ok(())
    /// }
    /// mod streaming_counter {
    ///     use numaflow::reducestream::{Message, ReduceStreamRequest};
    ///     use numaflow::reducestream::{ReduceStreamer, Metadata};
    ///     use tokio::sync::mpsc::{Receiver, Sender};
    ///     use tonic::async_trait;
    ///
    ///     pub(crate) struct StreamingCounter {}
    ///
    ///     pub(crate) struct StreamingCounterCreator {}
    ///
    ///     impl numaflow::reducestream::ReduceStreamerCreator for StreamingCounterCreator {
    ///         type R = StreamingCounter;
    ///
    ///         fn create(&self) -> Self::R {
    ///             StreamingCounter::new()
    ///         }
    ///     }
    ///
    ///     impl StreamingCounter {
    ///         pub(crate) fn new() -> Self {
    ///             Self {}
    ///         }
    ///     }
    ///
    ///     #[async_trait]
    ///     impl ReduceStreamer for StreamingCounter {
    ///         async fn reducestream(
    ///             &self,
    ///             keys: Vec<String>,
    ///             mut input: Receiver<ReduceStreamRequest>,
    ///             output: Sender<Message>,
    ///             _md: &Metadata,
    ///         ) {
    ///             let mut counter = 0;
    ///             // Stream results as we process each element
    ///             while input.recv().await.is_some() {
    ///                 counter += 1;
    ///                 let message = Message::new(counter.to_string().into_bytes())
    ///                     .with_tags(vec![])
    ///                     .with_keys(keys.clone());
    ///                 // Send intermediate results
    ///                 if output.send(message).await.is_err() {
    ///                     break; // Client disconnected
    ///                 }
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    /// [Window]: https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/windowing/
    async fn reducestream(
        &self,
        keys: Vec<String>,
        input: mpsc::Receiver<ReduceStreamRequest>,
        output: mpsc::Sender<Message>,
        md: &Metadata,
    );
}

// Re-export types from reduce module
pub use crate::reduce::{IntervalWindow, Message, Metadata};

/// Incoming request into the reducer handler of [`ReduceStreamer`].
/// This is an alias for the ReduceRequest from the reduce module.
pub type ReduceStreamRequest = crate::reduce::ReduceRequest;

#[async_trait]
impl<C> proto::reduce_server::Reduce for ReduceStreamService<C>
where
    C: ReduceStreamerCreator + Send + Sync + 'static,
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

        // Spawn a new task to handle the incoming ReduceRequests from the client
        let reader_handle = tokio::spawn(async move {
            let mut stream = request.into_inner();
            loop {
                match stream.next().await {
                    Some(Ok(rr)) => {
                        task_tx
                            .send(TaskCommand::HandleReduceRequest(rr))
                            .await
                            .expect("task_tx send failed");
                    }
                    Some(Err(e)) => {
                        response_tx
                            .send(Err(Error::ReduceError(ErrorKind::InternalError(format!(
                                "Failed to receive request: {}",
                                e
                            )))))
                            .await
                            .expect("error_tx send failed");
                        break;
                    }
                    // COB
                    None => {
                        task_tx
                            .send(TaskCommand::Close)
                            .await
                            .expect("task_tx send failed");
                        break;
                    }
                }
            }
        });

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
                                 // all the tasks are done (COB has happened and we have closed the tx for the tasks)
                                if eof {
                                    break;
                                }
                            }
                            Some(Err(error)) => {
                                error!("Error from task: {:?}", error);
                                grpc_response_tx
                                    .send(Err(error.into_status()))
                                    .await
                                    .expect("send to grpc response channel failed");
                                // stop reading new messages from the stream.
                                reader_handle.abort();
                                // Send a shutdown signal to the grpc server.
                                shutdown_tx.send(()).await.expect("shutdown_tx send failed");
                            }
                            None => {
                                // we break at eof, None should not happen
                                unreachable!()
                            }
                        }
                    }
                    _ = response_task_token.cancelled() => {
                        // stop reading new messages from stream.
                        reader_handle.abort();
                        // Send an abort signal to the task executor to abort all the tasks.
                        abort_tx.send(()).expect("task_tx send failed");
                        break;
                    }
                }
            }
        });

        // return the rx as the streaming endpoint
        Ok(Response::new(ReceiverStream::new(grpc_response_rx)))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}

// The `Task` struct represents a task in the reduce stream service. It is responsible for executing the
// user defined function. We will have a separate task for each keyed window. The task will be created
// when the first message for a given key arrives and will be closed when the window is closed.
struct Task {
    udf_tx: Sender<ReduceStreamRequest>,
    response_tx: Sender<Result<proto::ReduceResponse, Error>>,
    done_rx: oneshot::Receiver<()>,
    handle: tokio::task::JoinHandle<()>,
}

// we only have one slot
const SLOT_0: &str = "slot-0";

impl Task {
    // Creates a new task with the given reducer, keys, metadata, and response channel.
    async fn new<R: ReduceStreamer + Send + Sync + 'static>(
        reducer: R,
        keys: Vec<String>,
        md: Metadata,
        response_tx: Sender<Result<proto::ReduceResponse, Error>>,
    ) -> Self {
        let (udf_tx, udf_rx) = channel::<ReduceStreamRequest>(1);
        let (user_output_tx, mut user_output_rx) = channel::<Message>(1);
        let (done_tx, done_rx) = oneshot::channel();

        // Spawn the user's reducestream function
        let udf_keys = keys.clone();
        let udf_md = md.clone();
        let udf_task = tokio::spawn(async move {
            reducer
                .reducestream(udf_keys, udf_rx, user_output_tx, &udf_md)
                .await;
        });

        // Spawn a task to forward messages from user's output channel to gRPC response channel
        let forward_response_tx = response_tx.clone();
        let forward_start = md.interval_window.start_time;
        let forward_end = md.interval_window.end_time;
        let forwarder_task = tokio::spawn(async move {
            while let Some(message) = user_output_rx.recv().await {
                let send_result = forward_response_tx
                    .send(Ok(proto::ReduceResponse {
                        result: Some(proto::reduce_response::Result {
                            keys: message.keys.unwrap_or_default(),
                            value: message.value,
                            tags: message.tags.unwrap_or_default(),
                        }),
                        window: Some(proto::Window {
                            start: prost_timestamp_from_utc(forward_start),
                            end: prost_timestamp_from_utc(forward_end),
                            slot: SLOT_0.to_string(),
                        }),
                        eof: false,
                    }))
                    .await;

                if let Err(e) = send_result {
                    let _ = forward_response_tx
                        .send(Err(Error::ReduceError(ErrorKind::InternalError(format!(
                            "Failed to send response back: {}",
                            e
                        )))))
                        .await;
                    return;
                }
            }
        });

        // We spawn a separate task to await both the UDF and forwarder tasks
        // This ensures any unhandled errors in the user-defined code are propagated to the client
        let handler_tx = response_tx.clone();
        let handle = tokio::spawn(async move {
            // Wait for the UDF task to complete
            if let Err(e) = udf_task.await {
                error!("Failed to run reducestream function: {e:?}");

                // Check if this is a panic or a regular error
                if let Some(panic_info) = get_panic_info() {
                    // This is a panic - send detailed panic information
                    let status = build_panic_status(&panic_info);
                    let _ = handler_tx.send(Err(Error::GrpcStatus(status))).await;
                } else {
                    // This is a non-panic error
                    let _ = handler_tx
                        .send(Err(Error::ReduceError(ErrorKind::UserDefinedError(
                            format!("Reduce stream task execution failed: {}", e),
                        ))))
                        .await;
                }
                // Abort the forwarder task if UDF fails
                forwarder_task.abort();
            } else {
                // UDF completed successfully, wait for forwarder to finish
                let _ = forwarder_task.await;
            }

            // Send a message indicating that the task has finished
            let _ = done_tx.send(());
        });

        Self {
            udf_tx,
            response_tx,
            done_rx,
            handle,
        }
    }

    // Sends the request to the user defined function's input channel.
    async fn send(&self, rr: ReduceStreamRequest) {
        if let Err(e) = self.udf_tx.send(rr).await {
            self.response_tx
                .send(Err(Error::ReduceError(ErrorKind::InternalError(format!(
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

    // Aborts the task by calling abort on join handler.
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
    C: ReduceStreamerCreator + Send + Sync + 'static,
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
                                            .handle_error(Error::ReduceError(ErrorKind::InternalError(
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
                            // COB
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
        // validate
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
            self.handle_error(Error::ReduceError(ErrorKind::InternalError(
                "Task not found".to_string(),
            )))
            .await;
        }
    }

    // Writes the ReduceRequest to the task with the given keys.
    async fn write_to_task(&mut self, keys: Vec<String>, rr: proto::ReduceRequest) {
        // validate the request
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
            self.handle_error(Error::ReduceError(ErrorKind::InternalError(
                "Task not found".to_string(),
            )))
            .await;
        }
    }

    // Validates the ReduceRequest and extracts the payload and window information.
    // If the ReduceRequest is invalid, it sends an error to the response stream and returns None.
    async fn validate_and_extract(
        &self,
        rr: proto::ReduceRequest,
    ) -> Option<(ReduceStreamRequest, IntervalWindow)> {
        // Extract the payload and window information from the ReduceRequest
        let (payload, windows) = match (rr.payload, rr.operation) {
            (Some(payload), Some(operation)) => (payload, operation.windows),
            _ => {
                self.handle_error(Error::ReduceError(ErrorKind::InternalError(
                    "Invalid ReduceRequest".to_string(),
                )))
                .await;
                return None;
            }
        };

        // Check if there is exactly one window in the ReduceRequest
        if windows.len() != 1 {
            self.handle_error(Error::ReduceError(ErrorKind::InternalError(
                "Exactly one window is required".to_string(),
            )))
            .await;
            return None;
        }

        // Extract the start and end time from the window
        let window = &windows[0];
        let (start_time, end_time) = (
            shared::utc_from_timestamp(window.start),
            shared::utc_from_timestamp(window.end),
        );

        // Create the IntervalWindow
        let interval_window = IntervalWindow {
            start_time,
            end_time,
        };

        // Create the ReduceStreamRequest (which is an alias for ReduceRequest)
        let reduce_request = crate::reduce::ReduceRequest {
            keys: payload.keys,
            value: payload.value,
            watermark: shared::utc_from_timestamp(payload.watermark),
            eventtime: shared::utc_from_timestamp(payload.event_time),
            headers: payload.headers,
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
            self.handle_error(Error::ReduceError(ErrorKind::InternalError(format!(
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

/// gRPC server to start a reduce stream service
#[derive(Debug)]
pub struct Server<C> {
    inner: shared::Server<C>,
}

impl<C> shared::ServerExtras<C> for Server<C> {
    fn transform_inner<F>(self, f: F) -> Self
    where
        F: FnOnce(shared::Server<C>) -> shared::Server<C>,
    {
        Self {
            inner: f(self.inner),
        }
    }

    fn inner_ref(&self) -> &shared::Server<C> {
        &self.inner
    }
}

impl<C> Server<C> {
    /// Create a new Server with the given reduce stream service
    pub fn new(creator: C) -> Self {
        Self {
            inner: shared::Server::new(
                creator,
                ContainerType::ReduceStream,
                SOCK_ADDR,
                SERVER_INFO_FILE,
            ),
        }
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown(
        self,
        user_shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        C: ReduceStreamerCreator + Send + Sync + 'static,
    {
        self.inner
            .start_with_shutdown(
                user_shutdown_rx,
                |creator, max_message_size, shutdown_tx, cln_token| {
                    let reduce_stream_svc = ReduceStreamService {
                        creator: Arc::new(creator),
                        shutdown_tx,
                        cancellation_token: cln_token,
                    };

                    let reduce_stream_svc =
                        proto::reduce_server::ReduceServer::new(reduce_stream_svc)
                            .max_encoding_message_size(max_message_size)
                            .max_decoding_message_size(max_message_size);

                    tonic::transport::Server::builder().add_service(reduce_stream_svc)
                },
            )
            .await
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates
    /// graceful shutdown of gRPC server when either one of the signal arrives.
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        C: ReduceStreamerCreator + Send + Sync + 'static,
    {
        self.inner
            .start(|creator, max_message_size, shutdown_tx, cln_token| {
                let reduce_stream_svc = ReduceStreamService {
                    creator: Arc::new(creator),
                    shutdown_tx,
                    cancellation_token: cln_token,
                };

                let reduce_stream_svc = proto::reduce_server::ReduceServer::new(reduce_stream_svc)
                    .max_encoding_message_size(max_message_size)
                    .max_decoding_message_size(max_message_size);

                tonic::transport::Server::builder().add_service(reduce_stream_svc)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::shared::ServerExtras;
    use std::path::PathBuf;
    use std::{error::Error, time::Duration};

    use prost_types::Timestamp;
    use tempfile::TempDir;
    use tokio::net::UnixStream;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::Request;
    use tonic::transport::Uri;
    use tower::service_fn;

    use crate::reducestream;
    use crate::reducestream::proto::reduce_client::ReduceClient;

    struct StreamingSum;

    #[tonic::async_trait]
    impl reducestream::ReduceStreamer for StreamingSum {
        async fn reducestream(
            &self,
            keys: Vec<String>,
            mut input: mpsc::Receiver<reducestream::ReduceStreamRequest>,
            output: mpsc::Sender<reducestream::Message>,
            _md: &reducestream::Metadata,
        ) {
            let mut sum = 0;
            while let Some(rr) = input.recv().await {
                sum += std::str::from_utf8(&rr.value)
                    .unwrap()
                    .parse::<i32>()
                    .unwrap();
                // Stream intermediate results
                let message = reducestream::Message::new(sum.to_string().into_bytes())
                    .with_keys(keys.clone());
                if output.send(message).await.is_err() {
                    break;
                }
            }
        }
    }

    struct StreamingSumCreator;

    impl reducestream::ReduceStreamerCreator for StreamingSumCreator {
        type R = StreamingSum;
        fn create(&self) -> StreamingSum {
            StreamingSum {}
        }
    }

    async fn setup_server<C: reducestream::ReduceStreamerCreator + Send + Sync + 'static>(
        creator: C,
    ) -> Result<(reducestream::Server<C>, PathBuf, PathBuf), Box<dyn Error>> {
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("reducestream.sock");
        let server_info_file = tmp_dir.path().join("reducestreamer-server-info");

        let server = reducestream::Server::new(creator)
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file)
            .with_max_message_size(10240);

        Ok((server, sock_file, server_info_file))
    }

    async fn setup_client(
        sock_file: PathBuf,
    ) -> Result<ReduceClient<tonic::transport::Channel>, Box<dyn Error>> {
        let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(service_fn(move |_: Uri| {
                let sock_file = sock_file.clone();
                async move {
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                        UnixStream::connect(sock_file).await?,
                    ))
                }
            }))
            .await?;

        let client = ReduceClient::new(channel);

        Ok(client)
    }

    #[tokio::test]
    async fn test_server_start() -> Result<(), Box<dyn Error>> {
        let (server, sock_file, server_info_file) = setup_server(StreamingSumCreator).await?;

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
    async fn streaming_sum_test() -> Result<(), Box<dyn Error>> {
        let (server, sock_file, _) = setup_server(StreamingSumCreator).await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = setup_client(sock_file).await?;

        let (tx, rx) = mpsc::channel(1);

        // Spawn a task to send ReduceRequests to the channel
        tokio::spawn(async move {
            for i in 1..=5 {
                let rr = reducestream::proto::ReduceRequest {
                    payload: Some(reducestream::proto::reduce_request::Payload {
                        keys: vec!["key1".to_string()],
                        value: i.to_string().as_bytes().to_vec(),
                        watermark: None,
                        event_time: None,
                        headers: Default::default(),
                    }),
                    operation: Some(reducestream::proto::reduce_request::WindowOperation {
                        event: 0,
                        windows: vec![reducestream::proto::Window {
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

        // We should get 5 streaming responses (1, 3, 6, 10, 15) + 1 EOF
        assert_eq!(responses.len(), 6);

        // Verify the streaming sums
        let expected_sums = vec![1, 3, 6, 10, 15];
        for (i, response) in responses.iter().take(5).enumerate() {
            if let Some(result) = response.result.as_ref() {
                let value = std::str::from_utf8(&result.value)
                    .unwrap()
                    .parse::<i32>()
                    .unwrap();
                assert_eq!(value, expected_sums[i]);
            }
        }

        // Check the last message is EOF
        assert!(responses[5].eof);

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
}
