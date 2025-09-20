pub use crate::proto::session_reduce as proto;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::panic;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming, async_trait};
use tracing::{error, info};

use crate::error::{Error, ErrorKind};
use crate::session_reduce::proto::session_reduce_request::window_operation::Event;
use crate::shared;
use shared::{ContainerType, build_panic_status, get_panic_info};

const KEY_JOIN_DELIMITER: &str = ":";

const SOCK_ADDR: &str = "/var/run/numaflow/sessionreduce.sock";
const SERVER_INFO_FILE: &str = "/var/run/numaflow/sessionreducer-server-info";
const CHANNEL_SIZE: usize = 100;

/// SessionReducer is the trait which has to be implemented to do a session reduce operation.
#[async_trait]
pub trait SessionReducer {
    /// SessionReduce applies a session reduce function to a request stream and streams the results.
    async fn session_reduce(
        &self,
        keys: Vec<String>,
        request_stream: mpsc::Receiver<SessionReduceRequest>,
        response_stream: mpsc::Sender<Message>,
    );

    /// Accumulator returns the accumulator for the session reducer, will be invoked when this session is merged
    /// with another session.
    async fn accumulator(&self) -> Vec<u8>;

    /// MergeAccumulator merges the accumulator for the session reducer, will be invoked when another session is merged
    /// with this session.
    async fn merge_accumulator(&self, accumulator: Vec<u8>);
}

/// SessionReducerCreator is the interface which can be used to create a session reducer.
pub trait SessionReducerCreator {
    type R: SessionReducer + Send + Sync + 'static;
    /// Create creates a session reducer, will be invoked once for every keyed window.
    fn create(&self) -> Self::R;
}

/// Message is the response from the user's [`SessionReducer::reduce`].
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
    /// use numaflow::session_reduce::Message;
    /// let message = Message::new(vec![1, 2, 3, 4]);
    /// ```
    pub fn new(value: Vec<u8>) -> Self {
        Self {
            value,
            keys: None,
            tags: None,
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
    ///  use numaflow::session_reduce::Message;
    /// let message = Message::new(vec![1, 2, 3]).with_keys(vec!["key1".to_string(), "key2".to_string()]);
    /// ```
    pub fn with_keys(mut self, keys: Vec<String>) -> Self {
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
    ///  use numaflow::session_reduce::Message;
    /// let message = Message::new(vec![1, 2, 3]).with_tags(vec!["tag1".to_string(), "tag2".to_string()]);
    /// ```
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = Some(tags);
        self
    }
}

/// Incoming request into the reducer handler of [`SessionReducer`].
pub struct SessionReduceRequest {
    /// Set of keys in the (key, value) terminology of map/reduce paradigm.
    pub keys: Vec<String>,
    /// The value in the (key, value) terminology of map/reduce paradigm.
    pub value: Vec<u8>,
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this time.
    pub watermark: DateTime<Utc>,
    /// Time of the element as seen at source or aligned after a reduce operation.
    pub event_time: DateTime<Utc>,
    /// Headers for the message.
    pub headers: HashMap<String, String>,
}

/// Commands that can be sent to the task manager
#[derive(Debug)]
enum TaskManagerCommand {
    CreateTask {
        keyed_window: proto::KeyedWindow,
        payload: Option<proto::session_reduce_request::Payload>,
        response_tx: oneshot::Sender<Result<(), Error>>,
    },
    AppendToTask {
        keyed_window: proto::KeyedWindow,
        payload: Option<proto::session_reduce_request::Payload>,
        response_tx: oneshot::Sender<Result<(), Error>>,
    },
    CloseTask {
        keyed_windows: Vec<proto::KeyedWindow>,
    },
    MergeTasks {
        keyed_windows: Vec<proto::KeyedWindow>,
        payload: Option<proto::session_reduce_request::Payload>,
        response_tx: oneshot::Sender<Result<(), Error>>,
    },
    ExpandTask {
        old_window: proto::KeyedWindow,
        new_window: proto::KeyedWindow,
        payload: Option<proto::session_reduce_request::Payload>,
        response_tx: oneshot::Sender<Result<(), Error>>,
    },
    WaitAll {
        response_tx: oneshot::Sender<()>,
    },
    Shutdown,
}

/// Represents a session reduce task for a specific keyed window
struct SessionReduceTask {
    keyed_window: Arc<RwLock<proto::KeyedWindow>>,
    session_reducer: Arc<dyn SessionReducer + Send + Sync>,
    input_tx: mpsc::Sender<SessionReduceRequest>,
    done_rx: oneshot::Receiver<()>,
    handle: tokio::task::JoinHandle<()>,
    merged: Arc<AtomicBool>,
}

impl SessionReduceTask {
    async fn new<R: SessionReducer + Send + Sync + 'static>(
        keyed_window: proto::KeyedWindow,
        session_reducer: R,
        response_tx: mpsc::Sender<Result<proto::SessionReduceResponse, Error>>,
    ) -> Self {
        let (input_tx, input_rx) = mpsc::channel::<SessionReduceRequest>(CHANNEL_SIZE);
        let (output_tx, mut output_rx) = mpsc::channel::<Message>(CHANNEL_SIZE);
        let (done_tx, done_rx) = oneshot::channel();

        let keyed_window_arc = Arc::new(RwLock::new(keyed_window.clone()));
        let session_reducer_arc = Arc::new(session_reducer);
        let merged = Arc::new(AtomicBool::new(false));

        // Spawn the main task that runs the user's session reduce function
        let task_join_handler = tokio::spawn({
            let keys = keyed_window.keys.clone();
            let window = keyed_window_arc.clone();
            let merged_flag = merged.clone();
            let response_sender = response_tx.clone();
            let reducer = session_reducer_arc.clone();

            async move {
                // Spawn a task to handle output messages
                let output_handle = tokio::spawn({
                    let window = window.clone();
                    let merged_flag = merged_flag.clone();
                    let response_sender = response_sender.clone();

                    async move {
                        while let Some(message) = output_rx.recv().await {
                            if !merged_flag.load(Ordering::Relaxed) {
                                let window_guard = window.read().await;
                                let response = proto::SessionReduceResponse {
                                    result: Some(proto::session_reduce_response::Result {
                                        keys: message.keys.unwrap_or_default(),
                                        value: message.value,
                                        tags: message.tags.unwrap_or_default(),
                                    }),
                                    keyed_window: Some(window_guard.clone()),
                                    eof: false,
                                };

                                if let Err(e) = response_sender.send(Ok(response)).await {
                                    error!("Failed to send response: {}", e);
                                    return;
                                }
                            }
                        }

                        // Send EOF if not merged
                        if !merged_flag.load(Ordering::Relaxed) {
                            let window_guard = window.read().await;
                            let eof_response = proto::SessionReduceResponse {
                                result: None,
                                keyed_window: Some(window_guard.clone()),
                                eof: true,
                            };
                            let _ = response_sender.send(Ok(eof_response)).await;
                        }
                    }
                });

                // Execute the user's session reduce function
                reducer.session_reduce(keys, input_rx, output_tx).await;

                // Wait for output task to complete
                let _ = output_handle.await;
            }
        });

        // We spawn a separate task to await the join handler so that in case of any unhandled errors in the user-defined
        // code will immediately be propagated to the client.
        let handler_tx = response_tx.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = task_join_handler.await {
                // Check if this is a panic or a regular error
                if let Some(panic_info) = get_panic_info() {
                    // This is a panic - send detailed panic information
                    let status = build_panic_status(&panic_info);
                    let _ = handler_tx.send(Err(Error::GrpcStatus(status))).await;
                } else {
                    // This is a non-panic error
                    let _ = handler_tx
                        .send(Err(Error::SessionReduceError(ErrorKind::UserDefinedError(
                            format!("Session reduce task execution failed: {}", e),
                        ))))
                        .await;
                }
            }

            // Send a message indicating that the task has finished
            let _ = done_tx.send(());
        });

        Self {
            keyed_window: keyed_window_arc,
            session_reducer: session_reducer_arc,
            input_tx,
            done_rx,
            handle,
            merged,
        }
    }

    /// Send data to the task
    async fn send(&self, request: SessionReduceRequest) -> Result<(), Error> {
        self.input_tx.send(request).await.map_err(|e| {
            Error::SessionReduceError(ErrorKind::InternalError(format!(
                "Failed to send to task: {}",
                e
            )))
        })
    }

    /// Close the task input (blocking)
    async fn close(self) {
        drop(self.input_tx);
        let _ = self.done_rx.await;
    }

    /// Abort the task
    async fn abort(self) {
        self.handle.abort();
    }

    /// Mark task as merged
    fn mark_merged(&self) {
        self.merged.store(true, Ordering::Relaxed);
    }

    /// Update the keyed window (used during expand operation)
    async fn update_keyed_window(&self, new_window: proto::KeyedWindow) {
        let mut window = self.keyed_window.write().await;
        *window = new_window;
    }

    /// Get accumulator from the session reducer
    async fn get_accumulator(&self) -> Vec<u8> {
        self.session_reducer.accumulator().await
    }

    /// Merge accumulator into the session reducer
    async fn merge_accumulator(&self, accumulator: Vec<u8>) {
        self.session_reducer.merge_accumulator(accumulator).await;
    }
}

/// Task manager that handles session reduce tasks using actor pattern
struct SessionReduceTaskManager<C> {
    creator: Arc<C>,
    tasks: HashMap<String, SessionReduceTask>,
    response_tx: mpsc::Sender<Result<proto::SessionReduceResponse, Error>>,
    shutdown_tx: mpsc::Sender<()>,
    is_shutdown: Arc<AtomicBool>,
}

impl<C> SessionReduceTaskManager<C>
where
    C: SessionReducerCreator + Send + Sync + 'static,
{
    fn new(
        creator: Arc<C>,
        response_tx: mpsc::Sender<Result<proto::SessionReduceResponse, Error>>,
        shutdown_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            creator,
            tasks: HashMap::new(),
            response_tx,
            shutdown_tx,
            is_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the task manager actor
    fn start(mut self) -> mpsc::Sender<TaskManagerCommand> {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<TaskManagerCommand>(CHANNEL_SIZE);

        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    TaskManagerCommand::CreateTask {
                        keyed_window,
                        payload,
                        response_tx,
                    } => {
                        let result = self.handle_create_task(keyed_window, payload).await;
                        let _ = response_tx.send(result);
                    }
                    TaskManagerCommand::AppendToTask {
                        keyed_window,
                        payload,
                        response_tx,
                    } => {
                        let result = self.handle_append_to_task(keyed_window, payload).await;
                        let _ = response_tx.send(result);
                    }
                    TaskManagerCommand::CloseTask { keyed_windows } => {
                        self.handle_close_task(keyed_windows).await;
                    }
                    TaskManagerCommand::MergeTasks {
                        keyed_windows,
                        payload,
                        response_tx,
                    } => {
                        let result = self.handle_merge_tasks(keyed_windows, payload).await;
                        let _ = response_tx.send(result);
                    }
                    TaskManagerCommand::ExpandTask {
                        old_window,
                        new_window,
                        payload,
                        response_tx,
                    } => {
                        let result = self
                            .handle_expand_task(old_window, new_window, payload)
                            .await;
                        let _ = response_tx.send(result);
                    }
                    TaskManagerCommand::WaitAll { response_tx } => {
                        self.handle_wait_all().await;
                        let _ = response_tx.send(());
                    }
                    TaskManagerCommand::Shutdown => {
                        self.handle_shutdown().await;
                        break;
                    }
                }
            }
        });

        cmd_tx
    }

    /// Creates and starts a new session reduce task
    async fn handle_create_task(
        &mut self,
        keyed_window: proto::KeyedWindow,
        payload: Option<proto::session_reduce_request::Payload>,
    ) -> Result<(), Error> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(Error::SessionReduceError(ErrorKind::InternalError(
                "Task manager is shutdown".to_string(),
            )));
        }

        let key = generate_key(&keyed_window);

        if self.tasks.contains_key(&key) {
            return Err(Error::SessionReduceError(ErrorKind::InternalError(
                format!("Task already exists for key: {}", key),
            )));
        }

        // Create new session reducer
        let session_reducer = self.creator.create();

        // Create new task
        let task =
            SessionReduceTask::new(keyed_window, session_reducer, self.response_tx.clone()).await;

        // Send payload if present
        if let Some(payload) = payload {
            let session_request = SessionReduceRequest::from(payload);
            task.send(session_request).await?;
        }

        self.tasks.insert(key, task);
        Ok(())
    }

    async fn handle_append_to_task(
        &mut self,
        keyed_window: proto::KeyedWindow,
        payload: Option<proto::session_reduce_request::Payload>,
    ) -> Result<(), Error> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(Error::SessionReduceError(ErrorKind::InternalError(
                "Task manager is shutdown".to_string(),
            )));
        }

        let key = generate_key(&keyed_window);

        // If task doesn't exist, create it
        if !self.tasks.contains_key(&key) {
            return self.handle_create_task(keyed_window, payload).await;
        }

        // Send payload if present
        if let Some(payload) = payload {
            let session_request = SessionReduceRequest::from(payload);
            if let Some(task) = self.tasks.get(&key) {
                task.send(session_request).await?;
            }
        }

        Ok(())
    }

    async fn handle_close_task(&mut self, keyed_windows: Vec<proto::KeyedWindow>) {
        for keyed_window in &keyed_windows {
            let key = generate_key(keyed_window);
            if let Some(task) = self.tasks.remove(&key) {
                task.close().await;
            }
        }
    }

    /// Merge multiple tasks into a single task
    async fn handle_merge_tasks(
        &mut self,
        keyed_windows: Vec<proto::KeyedWindow>,
        payload: Option<proto::session_reduce_request::Payload>,
    ) -> Result<(), Error> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(Error::SessionReduceError(ErrorKind::InternalError(
                "Task manager is shutdown".to_string(),
            )));
        }

        if keyed_windows.is_empty() {
            return Err(Error::SessionReduceError(ErrorKind::InternalError(
                "Merge operation requires at least one window".to_string(),
            )));
        }

        // Collect tasks and accumulators, and determine the merged window bounds
        let (tasks_to_merge, accumulators, merged_window) =
            self.collect_merge_data(&keyed_windows).await?;
        let merged_key = generate_key(&merged_window);

        // Close all tasks being merged
        for task in tasks_to_merge {
            task.close().await;
        }

        // Create and initialize the merged task
        let session_reducer = self.creator.create();
        let merged_task =
            SessionReduceTask::new(merged_window, session_reducer, self.response_tx.clone()).await;

        // Merge all accumulators into the new task
        for accumulator in accumulators {
            merged_task.merge_accumulator(accumulator).await;
        }

        // Send payload if present
        if let Some(payload) = payload {
            let session_request = SessionReduceRequest::from(payload);
            merged_task.send(session_request).await?;
        }

        self.tasks.insert(merged_key, merged_task);
        Ok(())
    }

    /// Creates the window to be created after a merge operation, and collects the tasks and accumulators
    /// to be merged with the newly created window.
    async fn collect_merge_data(
        &mut self,
        keyed_windows: &[proto::KeyedWindow],
    ) -> Result<(Vec<SessionReduceTask>, Vec<Vec<u8>>, proto::KeyedWindow), Error> {
        let mut tasks_to_merge = Vec::new();
        let mut accumulators = Vec::new();
        let mut merged_window = keyed_windows[0].clone();

        for keyed_window in keyed_windows {
            let key = generate_key(keyed_window);
            if let Some(task) = self.tasks.remove(&key) {
                // Update merged window bounds
                self.update_window_bounds(&mut merged_window, keyed_window);

                // Collect accumulator before marking as merged
                accumulators.push(task.get_accumulator().await);

                // Mark task as merged and add to the list
                task.mark_merged();
                tasks_to_merge.push(task);
            } else {
                return Err(Error::SessionReduceError(ErrorKind::InternalError(
                    format!("Task not found for merge operation: {}", key),
                )));
            }
        }

        Ok((tasks_to_merge, accumulators, merged_window))
    }

    /// Update the bounds of the merged window
    fn update_window_bounds(
        &self,
        merged_window: &mut proto::KeyedWindow,
        keyed_window: &proto::KeyedWindow,
    ) {
        let start = keyed_window.start.unwrap();
        let end = keyed_window.end.unwrap();
        let merged_start = merged_window.start.unwrap();
        let merged_end = merged_window.end.unwrap();

        if start.seconds < merged_start.seconds
            || (start.seconds == merged_start.seconds && start.nanos < merged_start.nanos)
        {
            merged_window.start = keyed_window.start;
        }

        if end.seconds > merged_end.seconds
            || (end.seconds == merged_end.seconds && end.nanos > merged_end.nanos)
        {
            merged_window.end = keyed_window.end;
        }
    }

    /// Expands the keyed window of a task
    async fn handle_expand_task(
        &mut self,
        old_window: proto::KeyedWindow,
        new_window: proto::KeyedWindow,
        payload: Option<proto::session_reduce_request::Payload>,
    ) -> Result<(), Error> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(Error::SessionReduceError(ErrorKind::InternalError(
                "Task manager is shutdown".to_string(),
            )));
        }

        let old_key = generate_key(&old_window);
        let new_key = generate_key(&new_window);

        let Some(task) = self.tasks.remove(&old_key) else {
            return Err(Error::SessionReduceError(ErrorKind::InternalError(
                format!("Task not found for expand operation: {}", old_key,),
            )));
        };

        // Update the task's keyed window
        task.update_keyed_window(new_window.clone()).await;

        // Send payload if present
        if let Some(payload) = payload {
            let session_request = SessionReduceRequest::from(payload);
            task.send(session_request).await?;
        }

        // Re-insert with new key
        self.tasks.insert(new_key, task);

        Ok(())
    }

    /// Wait for all tasks to complete
    async fn handle_wait_all(&mut self) {
        let tasks: Vec<_> = self.tasks.drain().collect();
        for (_, task) in tasks {
            task.close().await;
        }
    }

    /// Shutdown the task manager
    async fn handle_shutdown(&mut self) {
        self.is_shutdown.store(true, Ordering::Relaxed);
        let tasks: Vec<_> = self.tasks.drain().collect();
        for (_, task) in tasks {
            task.abort().await;
        }
        let _ = self.shutdown_tx.send(()).await;
    }
}

/// Session reduce service implementation
struct SessionReduceService<C> {
    creator: Arc<C>,
    shutdown_tx: mpsc::Sender<()>,
    cancellation_token: CancellationToken,
}

#[async_trait]
impl<C> proto::session_reduce_server::SessionReduce for SessionReduceService<C>
where
    C: SessionReducerCreator + Send + Sync + 'static,
{
    type SessionReduceFnStream = ReceiverStream<Result<proto::SessionReduceResponse, Status>>;

    async fn session_reduce_fn(
        &self,
        request: Request<Streaming<proto::SessionReduceRequest>>,
    ) -> Result<Response<Self::SessionReduceFnStream>, Status> {
        let creator = Arc::clone(&self.creator);
        let shutdown_tx = self.shutdown_tx.clone();
        let cancellation_token = self.cancellation_token.child_token();

        // Create response channel for gRPC client
        let (grpc_response_tx, grpc_response_rx) =
            mpsc::channel::<Result<proto::SessionReduceResponse, Status>>(CHANNEL_SIZE);

        // Internal response channel for task manager
        let (response_tx, mut response_rx) =
            mpsc::channel::<Result<proto::SessionReduceResponse, Error>>(CHANNEL_SIZE);

        // Start task manager
        let task_manager = SessionReduceTaskManager::new(creator, response_tx, shutdown_tx.clone());

        let request_cancellation_token = cancellation_token.clone();
        // Spawn task to handle incoming requests
        tokio::spawn(async move {
            let mut stream = request.into_inner();

            let task_manager_tx = task_manager.start();

            loop {
                tokio::select! {
                    // Listen for incoming requests
                    result = stream.next() => {
                        match result {
                            Some(Ok(req)) => {
                                if let Err(e) = handle_session_reduce_request(req, &task_manager_tx).await {
                                    error!("Error handling request: {}", e);
                                    let _ = task_manager_tx.send(TaskManagerCommand::Shutdown).await;
                                    break;
                                }
                            }
                            Some(Err(e)) => {
                                error!("Error receiving request: {}", e);
                                let _ = task_manager_tx.send(TaskManagerCommand::Shutdown).await;
                                break;
                            }
                            None => {
                                // End of stream
                                break;
                            }
                        }
                    }
                    // Listen for cancellation from the main cancellation token
                    _ = request_cancellation_token.cancelled() => {
                        info!("Request task cancelled by main cancellation token");
                        break;
                    }
                }
            }

            info!("Request stream closed, waiting for all tasks to finish");
            // End of stream - wait for all tasks to complete
            let (wait_tx, wait_rx) = oneshot::channel();
            let _ = task_manager_tx
                .send(TaskManagerCommand::WaitAll {
                    response_tx: wait_tx,
                })
                .await;
            let _ = wait_rx.await;
            info!("All tasks finished");
        });

        // Spawn task to forward responses and handle errors
        let response_cancellation_token = cancellation_token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = response_rx.recv() => {
                        match result {
                            Some(Ok(response)) => {
                                if grpc_response_tx.send(Ok(response)).await.is_err() {
                                    break;
                                }
                            }
                            Some(Err(error)) => {
                                error!("Error from task manager: {}", error);
                                let _ = grpc_response_tx.send(Err(error.into_status())).await;
                                // Signal request task to stop due to error
                                response_cancellation_token.cancel();
                                break;
                            }
                            None => break,
                        }
                    }
                    _ = response_cancellation_token.cancelled() => {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(grpc_response_rx)))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}

/// Handle individual session reduce request
async fn handle_session_reduce_request(
    request: proto::SessionReduceRequest,
    task_manager_tx: &mpsc::Sender<TaskManagerCommand>,
) -> Result<(), Error> {
    let operation = request.operation.as_ref().ok_or_else(|| {
        Error::SessionReduceError(ErrorKind::InternalError("Missing operation".to_string()))
    })?;

    match Event::try_from(operation.event) {
        Ok(Event::Open) => {
            // Validate operation has exactly one window
            if operation.keyed_windows.len() != 1 {
                return Err(Error::SessionReduceError(ErrorKind::InternalError(
                    format!(
                        "Open operation requires exactly one window, got {}",
                        operation.keyed_windows.len(),
                    ),
                )));
            }

            let (response_tx, response_rx) = oneshot::channel();
            task_manager_tx
                .send(TaskManagerCommand::CreateTask {
                    keyed_window: operation.keyed_windows[0].clone(),
                    payload: request.payload,
                    response_tx,
                })
                .await
                .map_err(|e| {
                    Error::SessionReduceError(ErrorKind::InternalError(format!(
                        "Failed to send create task command: {}",
                        e,
                    )))
                })?;
            response_rx.await.map_err(|e| {
                Error::SessionReduceError(ErrorKind::InternalError(format!(
                    "Failed to receive create task response: {}",
                    e,
                )))
            })?
        }
        Ok(Event::Append) => {
            // Validate operation has exactly one window
            if operation.keyed_windows.len() != 1 {
                return Err(Error::SessionReduceError(ErrorKind::InternalError(
                    format!(
                        "Append operation requires exactly one window, got {}",
                        operation.keyed_windows.len(),
                    ),
                )));
            }

            let (response_tx, response_rx) = oneshot::channel();
            task_manager_tx
                .send(TaskManagerCommand::AppendToTask {
                    keyed_window: operation.keyed_windows[0].clone(),
                    payload: request.payload,
                    response_tx,
                })
                .await
                .map_err(|e| {
                    Error::SessionReduceError(ErrorKind::InternalError(format!(
                        "Failed to send append task command: {}",
                        e,
                    )))
                })?;
            response_rx.await.map_err(|e| {
                Error::SessionReduceError(ErrorKind::InternalError(format!(
                    "Failed to receive append task response: {}",
                    e,
                )))
            })?
        }
        Ok(Event::Close) => {
            task_manager_tx
                .send(TaskManagerCommand::CloseTask {
                    keyed_windows: operation.keyed_windows.clone(),
                })
                .await
                .map_err(|e| {
                    Error::SessionReduceError(ErrorKind::InternalError(format!(
                        "Failed to send close task command: {}",
                        e,
                    )))
                })?;
            Ok(())
        }
        Ok(Event::Merge) => {
            // Validate operation has at least one window
            if operation.keyed_windows.is_empty() {
                return Err(Error::SessionReduceError(ErrorKind::InternalError(
                    "Merge operation requires at least one window".to_string(),
                )));
            }

            let (response_tx, response_rx) = oneshot::channel();
            task_manager_tx
                .send(TaskManagerCommand::MergeTasks {
                    keyed_windows: operation.keyed_windows.clone(),
                    payload: request.payload,
                    response_tx,
                })
                .await
                .map_err(|e| {
                    Error::SessionReduceError(ErrorKind::InternalError(format!(
                        "Failed to send merge tasks command: {}",
                        e,
                    )))
                })?;
            response_rx.await.map_err(|e| {
                Error::SessionReduceError(ErrorKind::InternalError(format!(
                    "Failed to receive merge tasks response: {}",
                    e,
                )))
            })?
        }
        Ok(Event::Expand) => {
            // Validate operation has exactly two windows (old and new)
            if operation.keyed_windows.len() != 2 {
                return Err(Error::SessionReduceError(ErrorKind::InternalError(
                    "Expand operation requires exactly two windows (old and new)".to_string(),
                )));
            }

            let (response_tx, response_rx) = oneshot::channel();
            task_manager_tx
                .send(TaskManagerCommand::ExpandTask {
                    old_window: operation.keyed_windows[0].clone(),
                    new_window: operation.keyed_windows[1].clone(),
                    payload: request.payload,
                    response_tx,
                })
                .await
                .map_err(|e| {
                    Error::SessionReduceError(ErrorKind::InternalError(format!(
                        "Failed to send expand task command: {}",
                        e,
                    )))
                })?;
            response_rx.await.map_err(|e| {
                Error::SessionReduceError(ErrorKind::InternalError(format!(
                    "Failed to receive expand task response: {}",
                    e,
                )))
            })?
        }
        Err(_) => Err(Error::SessionReduceError(ErrorKind::InternalError(
            format!("Unknown operation event: {}", operation.event,),
        ))),
    }
}

/// Generate unique key for a keyed window
fn generate_key(keyed_window: &proto::KeyedWindow) -> String {
    let start = keyed_window.start.as_ref().unwrap().seconds;
    let end = keyed_window.end.as_ref().unwrap().seconds;

    format!(
        "{}:{}:{}",
        start,
        end,
        keyed_window.keys.join(KEY_JOIN_DELIMITER)
    )
}

impl From<proto::session_reduce_request::Payload> for SessionReduceRequest {
    fn from(payload: proto::session_reduce_request::Payload) -> Self {
        Self {
            keys: payload.keys,
            value: payload.value,
            watermark: payload
                .watermark
                .map(|ts| shared::utc_from_timestamp(Some(ts)))
                .unwrap_or_else(Utc::now),
            event_time: payload
                .event_time
                .map(|ts| shared::utc_from_timestamp(Some(ts)))
                .unwrap_or_else(Utc::now),
            headers: payload.headers,
        }
    }
}

/// gRPC server for session reduce service
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
    /// Create a new Server with the given session reduce service
    pub fn new(creator: C) -> Self {
        Self {
            inner: shared::Server::new(
                creator,
                ContainerType::SessionReduce,
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
        C: SessionReducerCreator + Send + Sync + 'static,
    {
        self.inner
            .start_with_shutdown(
                user_shutdown_rx,
                |creator, max_message_size, shutdown_tx, cln_token| {
                    let session_reduce_svc = SessionReduceService {
                        creator: Arc::new(creator),
                        shutdown_tx,
                        cancellation_token: cln_token,
                    };

                    let session_reduce_svc =
                        proto::session_reduce_server::SessionReduceServer::new(session_reduce_svc)
                            .max_encoding_message_size(max_message_size)
                            .max_decoding_message_size(max_message_size);

                    tonic::transport::Server::builder().add_service(session_reduce_svc)
                },
            )
            .await
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates
    /// graceful shutdown of gRPC server when either one of the signal arrives.
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        C: SessionReducerCreator + Send + Sync + 'static,
    {
        self.inner
            .start(|creator, max_message_size, shutdown_tx, cln_token| {
                let session_reduce_svc = SessionReduceService {
                    creator: Arc::new(creator),
                    shutdown_tx,
                    cancellation_token: cln_token,
                };

                let session_reduce_svc =
                    proto::session_reduce_server::SessionReduceServer::new(session_reduce_svc)
                        .max_encoding_message_size(max_message_size)
                        .max_decoding_message_size(max_message_size);

                tonic::transport::Server::builder().add_service(session_reduce_svc)
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
    use tokio::time::sleep;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::Request;
    use tonic::transport::Uri;
    use tower::service_fn;

    use crate::session_reduce;
    use crate::session_reduce::proto::session_reduce_client::SessionReduceClient;

    struct Sum;

    #[tonic::async_trait]
    impl session_reduce::SessionReducer for Sum {
        async fn session_reduce(
            &self,
            keys: Vec<String>,
            mut input: mpsc::Receiver<session_reduce::SessionReduceRequest>,
            output: mpsc::Sender<session_reduce::Message>,
        ) {
            let mut sum = 0;
            while let Some(rr) = input.recv().await {
                sum += std::str::from_utf8(&rr.value)
                    .unwrap()
                    .parse::<i32>()
                    .unwrap();
            }
            let _ = output
                .send(session_reduce::Message::new(sum.to_string().into_bytes()).with_keys(keys))
                .await;
        }

        async fn accumulator(&self) -> Vec<u8> {
            vec![]
        }

        async fn merge_accumulator(&self, _accumulator: Vec<u8>) {
            // No-op for this simple test
        }
    }

    struct SumCreator;

    impl session_reduce::SessionReducerCreator for SumCreator {
        type R = Sum;
        fn create(&self) -> Sum {
            Sum {}
        }
    }

    async fn setup_server<C: session_reduce::SessionReducerCreator + Send + Sync + 'static>(
        creator: C,
    ) -> Result<(session_reduce::Server<C>, PathBuf, PathBuf), Box<dyn Error>> {
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sessionreduce.sock");
        let server_info_file = tmp_dir.path().join("sessionreducer-server-info");

        let server = session_reduce::Server::new(creator)
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file)
            .with_max_message_size(10240);

        Ok((server, sock_file, server_info_file))
    }

    async fn setup_client(
        sock_file: PathBuf,
    ) -> Result<SessionReduceClient<tonic::transport::Channel>, Box<dyn Error>> {
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

        let client = SessionReduceClient::new(channel);

        Ok(client)
    }

    #[tokio::test]
    async fn test_server_start() -> Result<(), Box<dyn Error>> {
        let (server, sock_file, server_info_file) = setup_server(SumCreator).await?;

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
    async fn test_session_reduce_operations() -> Result<(), Box<dyn Error>> {
        let (server, sock_file, _) = setup_server(SumCreator).await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = setup_client(sock_file).await?;

        let (tx, rx) = mpsc::channel(1);

        // Spawn a task to send SessionReduceRequests to the channel
        tokio::spawn(async move {
            // Test CREATE operation
            let create_request = session_reduce::proto::SessionReduceRequest {
                payload: Some(session_reduce::proto::session_reduce_request::Payload {
                    keys: vec!["key1".to_string()],
                    value: "5".as_bytes().to_vec(),
                    watermark: Some(Timestamp {
                        seconds: 60000,
                        nanos: 0,
                    }),
                    event_time: Some(Timestamp {
                        seconds: 60000,
                        nanos: 0,
                    }),
                    headers: Default::default(),
                }),
                operation: Some(session_reduce::proto::session_reduce_request::WindowOperation {
                    event: session_reduce::proto::session_reduce_request::window_operation::Event::Open as i32,
                    keyed_windows: vec![session_reduce::proto::KeyedWindow {
                        start: Some(Timestamp {
                            seconds: 60000,
                            nanos: 0,
                        }),
                        end: Some(Timestamp {
                            seconds: 120000,
                            nanos: 0,
                        }),
                        slot: "slot-0".to_string(),
                        keys: vec!["key1".to_string()],
                    }],
                }),
            };

            tx.send(create_request).await.unwrap();

            // Test APPEND operation
            let append_request = session_reduce::proto::SessionReduceRequest {
                payload: Some(session_reduce::proto::session_reduce_request::Payload {
                    keys: vec!["key1".to_string()],
                    value: "10".as_bytes().to_vec(),
                    watermark: Some(Timestamp {
                        seconds: 60000,
                        nanos: 0,
                    }),
                    event_time: Some(Timestamp {
                        seconds: 60000,
                        nanos: 0,
                    }),
                    headers: Default::default(),
                }),
                operation: Some(session_reduce::proto::session_reduce_request::WindowOperation {
                    event: session_reduce::proto::session_reduce_request::window_operation::Event::Append as i32,
                    keyed_windows: vec![session_reduce::proto::KeyedWindow {
                        start: Some(Timestamp {
                            seconds: 60000,
                            nanos: 0,
                        }),
                        end: Some(Timestamp {
                            seconds: 120000,
                            nanos: 0,
                        }),
                        slot: "slot-0".to_string(),
                        keys: vec!["key1".to_string()],
                    }],
                }),
            };

            tx.send(append_request).await.unwrap();

            // Test CLOSE operation
            let close_request = session_reduce::proto::SessionReduceRequest {
                payload: None,
                operation: Some(session_reduce::proto::session_reduce_request::WindowOperation {
                    event: session_reduce::proto::session_reduce_request::window_operation::Event::Close as i32,
                    keyed_windows: vec![session_reduce::proto::KeyedWindow {
                        start: Some(Timestamp {
                            seconds: 60000,
                            nanos: 0,
                        }),
                        end: Some(Timestamp {
                            seconds: 120000,
                            nanos: 0,
                        }),
                        slot: "slot-0".to_string(),
                        keys: vec!["key1".to_string()],
                    }],
                }),
            };

            tx.send(close_request).await.unwrap();
        });

        // Convert the receiver end of the channel into a stream
        let stream = ReceiverStream::new(rx);

        // Create a tonic::Request from the stream
        let request = Request::new(stream);

        // Send the request to the server
        let resp = client.session_reduce_fn(request).await?;

        let mut response_stream = resp.into_inner();
        let mut responses = Vec::new();

        while let Some(response) = response_stream.message().await? {
            responses.push(response.clone());
        }

        // We should get at least one response with the sum (5 + 10 = 15) and one EOF
        assert!(!responses.is_empty());

        let mut found_result = false;
        let mut found_eof = false;

        for response in responses {
            if let Some(result) = response.result.as_ref() {
                assert_eq!(result.keys, vec!["key1".to_string()]);
                assert_eq!(result.value, "15".as_bytes().to_vec());
                found_result = true;
            }

            if response.eof {
                found_eof = true;
            }

            if let Some(keyed_window) = response.keyed_window.as_ref() {
                assert_eq!(keyed_window.keys, vec!["key1".to_string()]);
                if let Some(start) = keyed_window.start.as_ref() {
                    assert_eq!(start.seconds, 60000);
                }
                if let Some(end) = keyed_window.end.as_ref() {
                    assert_eq!(end.seconds, 120000);
                }
            }
        }

        assert!(found_result, "Should have received a result");
        assert!(found_eof, "Should have received EOF");

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
    async fn test_invalid_input() -> Result<(), Box<dyn Error>> {
        let (server, sock_file, _) = setup_server(SumCreator).await?;

        let (_shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = setup_client(sock_file).await?;

        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn a task to send invalid SessionReduceRequests to the channel
        let _sender_task = tokio::spawn(async move {
            for _ in 0..10 {
                let rr = session_reduce::proto::SessionReduceRequest {
                    payload: Some(session_reduce::proto::session_reduce_request::Payload {
                        keys: vec!["key1".to_string()],
                        value: vec![],
                        watermark: None,
                        event_time: None,
                        headers: Default::default(),
                    }),
                    operation: Some(session_reduce::proto::session_reduce_request::WindowOperation {
                        event: session_reduce::proto::session_reduce_request::window_operation::Event::Open as i32,
                        keyed_windows: vec![
                            session_reduce::proto::KeyedWindow {
                                start: Some(Timestamp {
                                    seconds: 60000,
                                    nanos: 0,
                                }),
                                end: Some(Timestamp {
                                    seconds: 120000,
                                    nanos: 0,
                                }),
                                slot: "slot-0".to_string(),
                                keys: vec!["key1".to_string()],
                            },
                            session_reduce::proto::KeyedWindow {
                                start: Some(Timestamp {
                                    seconds: 60000,
                                    nanos: 0,
                                }),
                                end: Some(Timestamp {
                                    seconds: 120000,
                                    nanos: 0,
                                }),
                                slot: "slot-1".to_string(),
                                keys: vec!["key1".to_string()],
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
            .session_reduce_fn(Request::new(
                tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
            ))
            .await;

        let mut response_stream = resp.unwrap().into_inner();

        if let Err(e) = response_stream.message().await {
            assert_eq!(e.code(), tonic::Code::Internal);
            assert!(e.message().contains("exactly one window"));
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

    #[cfg(feature = "test-panic")]
    mod panic_tests {
        use super::*;

        struct PanicSessionReducer;

        #[tonic::async_trait]
        impl session_reduce::SessionReducer for PanicSessionReducer {
            async fn session_reduce(
                &self,
                _keys: Vec<String>,
                _input: mpsc::Receiver<session_reduce::SessionReduceRequest>,
                _output: mpsc::Sender<session_reduce::Message>,
            ) {
                panic!("Panic in session reduce method");
            }

            async fn accumulator(&self) -> Vec<u8> {
                vec![]
            }

            async fn merge_accumulator(&self, _accumulator: Vec<u8>) {
                // No-op
            }
        }

        struct PanicSessionReducerCreator;

        impl session_reduce::SessionReducerCreator for PanicSessionReducerCreator {
            type R = PanicSessionReducer;
            fn create(&self) -> PanicSessionReducer {
                PanicSessionReducer {}
            }
        }

        #[tokio::test]
        async fn test_panic_in_session_reduce() -> Result<(), Box<dyn Error>> {
            let (server, sock_file, _) = setup_server(PanicSessionReducerCreator).await?;

            let (_shutdown_tx, shutdown_rx) = oneshot::channel();

            let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

            tokio::time::sleep(Duration::from_millis(50)).await;

            let mut client = setup_client(sock_file.clone()).await?;

            let (tx, rx) = mpsc::channel(1);

            // Spawn a task to send SessionReduceRequests to the channel
            tokio::spawn(async move {
                let rr = session_reduce::proto::SessionReduceRequest {
                payload: Some(session_reduce::proto::session_reduce_request::Payload {
                    keys: vec!["key1".to_string()],
                    value: vec![],
                    watermark: None,
                    event_time: None,
                    headers: Default::default(),
                }),
                operation: Some(session_reduce::proto::session_reduce_request::WindowOperation {
                    event: session_reduce::proto::session_reduce_request::window_operation::Event::Open as i32,
                    keyed_windows: vec![session_reduce::proto::KeyedWindow {
                        start: Some(Timestamp {
                            seconds: 60000,
                            nanos: 0,
                        }),
                        end: Some(Timestamp {
                            seconds: 120000,
                            nanos: 0,
                        }),
                        slot: "slot-0".to_string(),
                        keys: vec!["key1".to_string()],
                    }],
                }),
            };

                // Send the first request which will cause a panic
                let _ = tx.send(rr.clone()).await;

                // Try to send more requests, but expect them to fail after the panic
                for _ in 1..10 {
                    if tx.send(rr.clone()).await.is_err() {
                        // Channel closed due to panic, which is expected
                        break;
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            });

            // Convert the receiver end of the channel into a stream
            let stream = ReceiverStream::new(rx);

            // Create a tonic::Request from the stream
            let request = Request::new(stream);

            // Send the request to the server
            let resp = client.session_reduce_fn(request).await?;

            let mut response_stream = resp.into_inner();

            if let Err(e) = response_stream.message().await {
                assert_eq!(e.code(), tonic::Code::Internal);
                assert!(e.message().contains("UDF_EXECUTION_ERROR"))
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

    #[tokio::test]
    async fn test_merge_operations() -> Result<(), Box<dyn Error>> {
        let (server, sock_file, _) = setup_server(SumCreator).await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = setup_client(sock_file).await?;

        let (tx, rx) = mpsc::channel(1);

        // Spawn a task to send SessionReduceRequests to the channel
        tokio::spawn(async move {
            // Create first window
            let create_request1 = session_reduce::proto::SessionReduceRequest {
                payload: Some(session_reduce::proto::session_reduce_request::Payload {
                    keys: vec!["key1".to_string()],
                    value: "5".as_bytes().to_vec(),
                    watermark: Some(Timestamp {
                        seconds: 60000,
                        nanos: 0,
                    }),
                    event_time: Some(Timestamp {
                        seconds: 60000,
                        nanos: 0,
                    }),
                    headers: Default::default(),
                }),
                operation: Some(session_reduce::proto::session_reduce_request::WindowOperation {
                    event: session_reduce::proto::session_reduce_request::window_operation::Event::Open as i32,
                    keyed_windows: vec![session_reduce::proto::KeyedWindow {
                        start: Some(Timestamp {
                            seconds: 60000,
                            nanos: 0,
                        }),
                        end: Some(Timestamp {
                            seconds: 90000,
                            nanos: 0,
                        }),
                        slot: "slot-0".to_string(),
                        keys: vec!["key1".to_string()],
                    }],
                }),
            };

            tx.send(create_request1).await.unwrap();

            // Create second window
            let create_request2 = session_reduce::proto::SessionReduceRequest {
                payload: Some(session_reduce::proto::session_reduce_request::Payload {
                    keys: vec!["key1".to_string()],
                    value: "10".as_bytes().to_vec(),
                    watermark: Some(Timestamp {
                        seconds: 90000,
                        nanos: 0,
                    }),
                    event_time: Some(Timestamp {
                        seconds: 90000,
                        nanos: 0,
                    }),
                    headers: Default::default(),
                }),
                operation: Some(session_reduce::proto::session_reduce_request::WindowOperation {
                    event: session_reduce::proto::session_reduce_request::window_operation::Event::Open as i32,
                    keyed_windows: vec![session_reduce::proto::KeyedWindow {
                        start: Some(Timestamp {
                            seconds: 90000,
                            nanos: 0,
                        }),
                        end: Some(Timestamp {
                            seconds: 120000,
                            nanos: 0,
                        }),
                        slot: "slot-1".to_string(),
                        keys: vec!["key1".to_string()],
                    }],
                }),
            };

            tx.send(create_request2).await.unwrap();

            // Merge the two windows
            let merge_request = session_reduce::proto::SessionReduceRequest {
                payload: None,
                operation: Some(session_reduce::proto::session_reduce_request::WindowOperation {
                    event: session_reduce::proto::session_reduce_request::window_operation::Event::Merge as i32,
                    keyed_windows: vec![
                        session_reduce::proto::KeyedWindow {
                            start: Some(Timestamp {
                                seconds: 60000,
                                nanos: 0,
                            }),
                            end: Some(Timestamp {
                                seconds: 90000,
                                nanos: 0,
                            }),
                            slot: "slot-0".to_string(),
                            keys: vec!["key1".to_string()],
                        },
                        session_reduce::proto::KeyedWindow {
                            start: Some(Timestamp {
                                seconds: 90000,
                                nanos: 0,
                            }),
                            end: Some(Timestamp {
                                seconds: 120000,
                                nanos: 0,
                            }),
                            slot: "slot-1".to_string(),
                            keys: vec!["key1".to_string()],
                        },
                    ],
                }),
            };

            tx.send(merge_request).await.unwrap();

            // Close the merged window
            let close_request = session_reduce::proto::SessionReduceRequest {
                payload: None,
                operation: Some(session_reduce::proto::session_reduce_request::WindowOperation {
                    event: session_reduce::proto::session_reduce_request::window_operation::Event::Close as i32,
                    keyed_windows: vec![session_reduce::proto::KeyedWindow {
                        start: Some(Timestamp {
                            seconds: 60000,
                            nanos: 0,
                        }),
                        end: Some(Timestamp {
                            seconds: 120000,
                            nanos: 0,
                        }),
                        slot: "slot-merged".to_string(),
                        keys: vec!["key1".to_string()],
                    }],
                }),
            };

            tx.send(close_request).await.unwrap();
        });

        // Convert the receiver end of the channel into a stream
        let stream = ReceiverStream::new(rx);

        // Create a tonic::Request from the stream
        let request = Request::new(stream);

        // Send the request to the server
        let resp = client.session_reduce_fn(request).await?;

        let mut response_stream = resp.into_inner();
        let mut responses = Vec::new();

        while let Some(response) = response_stream.message().await? {
            responses.push(response);
        }

        // We should get responses from the merge operation
        assert!(!responses.is_empty());

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
