pub use crate::proto::accumulator as proto;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming, async_trait};
use tracing::{error, info};

use crate::accumulator::proto::accumulator_request::window_operation::Event;
use crate::error::{Error, ErrorKind};
use crate::shared;
use shared::{ContainerType, ServerConfig, SocketCleanup};

const KEY_JOIN_DELIMITER: &str = ":";

const SOCK_ADDR: &str = "/var/run/numaflow/accumulator.sock";
const SERVER_INFO_FILE: &str = "/var/run/numaflow/accumulator-server-info";
const CHANNEL_SIZE: usize = 100;

/// Accumulator is the interface which can be used to implement the accumulator operation.
#[async_trait]
pub trait Accumulator {
    /// Accumulate can read unordered from the input stream and emit the ordered data to the output stream.
    /// Once the watermark (WM) of the output stream progresses, the data in WAL until that WM will be garbage collected.
    /// NOTE: A message can be silently dropped if need be, and it will be cleared from the WAL when the WM progresses.
    async fn accumulate(
        &self,
        input: mpsc::Receiver<AccumulatorRequest>,
        output: mpsc::Sender<Message>,
    );
}

/// AccumulatorCreator is the interface which is used to create an Accumulator.
pub trait AccumulatorCreator {
    type A: Accumulator + Send + Sync + 'static;
    /// Create is called for every key and will be closed after the keyed stream is idle for the timeout duration.
    fn create(&self) -> Self::A;
}

/// Message is used to wrap the data return by Accumulator functions
#[derive(Debug, PartialEq)]
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection.
    keys: Option<Vec<String>>,
    /// Value is the value passed to the next vertex.
    value: Vec<u8>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    tags: Option<Vec<String>>,
    /// ID is used for deduplication. Read-only, set from the input datum.
    id: String,
    /// Headers for the message. Read-only, set from the input datum.
    headers: HashMap<String, String>,
    /// Time of the element as seen at source or aligned after a reduce operation. Read-only, set from the input datum.
    event_time: DateTime<Utc>,
    /// Watermark represented by time is a guarantee that we will not see an element older than this time. Read-only, set from the input datum.
    watermark: DateTime<Utc>,
}

/// Represents a message that can be modified and forwarded.
impl Message {
    /// Creates a new message from the input datum. It's advised to use the same input datum for creating the
    /// message, only use a custom implementation if you know what you are doing.
    ///
    /// # Arguments
    ///
    /// * `request` - The input AccumulatorRequest to create the message from.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::accumulator::{Message, AccumulatorRequest};
    /// use chrono::Utc;
    /// use std::collections::HashMap;
    ///
    /// let request = AccumulatorRequest {
    ///     keys: vec!["key1".to_string()],
    ///     value: vec![1, 2, 3, 4],
    ///     watermark: Utc::now(),
    ///     event_time: Utc::now(),
    ///     headers: HashMap::new(),
    ///     id: "msg1".to_string(),
    /// };
    /// let message = Message::from_accumulator_request(request);
    /// ```
    pub fn from_accumulator_request(request: AccumulatorRequest) -> Self {
        Self {
            keys: Some(request.keys),
            value: request.value,
            tags: None,
            id: request.id,
            headers: request.headers,
            event_time: request.event_time,
            watermark: request.watermark,
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
    /// use numaflow::accumulator::{Message, AccumulatorRequest};
    /// use chrono::Utc;
    /// use std::collections::HashMap;
    ///
    /// let request = AccumulatorRequest {
    ///     keys: vec!["original_key".to_string()],
    ///     value: vec![1, 2, 3],
    ///     watermark: Utc::now(),
    ///     event_time: Utc::now(),
    ///     headers: HashMap::new(),
    ///     id: "msg1".to_string(),
    /// };
    /// let message = Message::from_accumulator_request(request).with_keys(vec!["key1".to_string(), "key2".to_string()]);
    /// ```
    pub fn with_keys(mut self, keys: Vec<String>) -> Self {
        self.keys = Some(keys);
        self
    }

    /// Sets or replaces the value associated with this message.
    ///
    /// # Arguments
    ///
    /// * `value` - A vector of bytes representing the message's payload.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::accumulator::{Message, AccumulatorRequest};
    /// use chrono::Utc;
    /// use std::collections::HashMap;
    ///
    /// let request = AccumulatorRequest {
    ///     keys: vec!["key1".to_string()],
    ///     value: vec![1, 2, 3],
    ///     watermark: Utc::now(),
    ///     event_time: Utc::now(),
    ///     headers: HashMap::new(),
    ///     id: "msg1".to_string(),
    /// };
    /// let message = Message::from_accumulator_request(request).with_value(vec![4, 5, 6]);
    /// ```
    pub fn with_value(mut self, value: Vec<u8>) -> Self {
        self.value = value;
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
    /// use numaflow::accumulator::{Message, AccumulatorRequest};
    /// use chrono::Utc;
    /// use std::collections::HashMap;
    ///
    /// let request = AccumulatorRequest {
    ///     keys: vec!["key1".to_string()],
    ///     value: vec![1, 2, 3],
    ///     watermark: Utc::now(),
    ///     event_time: Utc::now(),
    ///     headers: HashMap::new(),
    ///     id: "msg1".to_string(),
    /// };
    /// let message = Message::from_accumulator_request(request).with_tags(vec!["tag1".to_string(), "tag2".to_string()]);
    /// ```
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Returns the keys associated with this message.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::accumulator::{Message, AccumulatorRequest};
    /// use chrono::Utc;
    /// use std::collections::HashMap;
    ///
    /// let request = AccumulatorRequest {
    ///     keys: vec!["key1".to_string()],
    ///     value: vec![1, 2, 3],
    ///     watermark: Utc::now(),
    ///     event_time: Utc::now(),
    ///     headers: HashMap::new(),
    ///     id: "msg1".to_string(),
    /// };
    /// let message = Message::from_accumulator_request(request);
    /// assert_eq!(message.keys(), &Some(vec!["key1".to_string()]));
    /// ```
    pub fn keys(&self) -> &Option<Vec<String>> {
        &self.keys
    }

    /// Returns the value associated with this message.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::accumulator::{Message, AccumulatorRequest};
    /// use chrono::Utc;
    /// use std::collections::HashMap;
    ///
    /// let request = AccumulatorRequest {
    ///     keys: vec!["key1".to_string()],
    ///     value: vec![1, 2, 3],
    ///     watermark: Utc::now(),
    ///     event_time: Utc::now(),
    ///     headers: HashMap::new(),
    ///     id: "msg1".to_string(),
    /// };
    /// let message = Message::from_accumulator_request(request);
    /// assert_eq!(message.value(), &vec![1, 2, 3]);
    /// ```
    pub fn value(&self) -> &Vec<u8> {
        &self.value
    }

    /// Returns the tags associated with this message.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::accumulator::{Message, AccumulatorRequest};
    /// use chrono::Utc;
    /// use std::collections::HashMap;
    ///
    /// let request = AccumulatorRequest {
    ///     keys: vec!["key1".to_string()],
    ///     value: vec![1, 2, 3],
    ///     watermark: Utc::now(),
    ///     event_time: Utc::now(),
    ///     headers: HashMap::new(),
    ///     id: "msg1".to_string(),
    /// };
    /// let message = Message::from_accumulator_request(request);
    /// assert_eq!(message.tags(), &None);
    /// ```
    pub fn tags(&self) -> &Option<Vec<String>> {
        &self.tags
    }

    /// Returns the ID associated with this message.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::accumulator::{Message, AccumulatorRequest};
    /// use chrono::Utc;
    /// use std::collections::HashMap;
    ///
    /// let request = AccumulatorRequest {
    ///     keys: vec!["key1".to_string()],
    ///     value: vec![1, 2, 3],
    ///     watermark: Utc::now(),
    ///     event_time: Utc::now(),
    ///     headers: HashMap::new(),
    ///     id: "msg1".to_string(),
    /// };
    /// let message = Message::from_accumulator_request(request);
    /// assert_eq!(message.id(), "msg1");
    /// ```
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the headers associated with this message.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::accumulator::{Message, AccumulatorRequest};
    /// use chrono::Utc;
    /// use std::collections::HashMap;
    ///
    /// let mut headers = HashMap::new();
    /// headers.insert("header1".to_string(), "value1".to_string());
    /// let request = AccumulatorRequest {
    ///     keys: vec!["key1".to_string()],
    ///     value: vec![1, 2, 3],
    ///     watermark: Utc::now(),
    ///     event_time: Utc::now(),
    ///     headers: headers.clone(),
    ///     id: "msg1".to_string(),
    /// };
    /// let message = Message::from_accumulator_request(request);
    /// assert_eq!(message.headers(), &headers);
    /// ```
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    /// Returns the event time associated with this message.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::accumulator::{Message, AccumulatorRequest};
    /// use chrono::Utc;
    /// use std::collections::HashMap;
    ///
    /// let event_time = Utc::now();
    /// let request = AccumulatorRequest {
    ///     keys: vec!["key1".to_string()],
    ///     value: vec![1, 2, 3],
    ///     watermark: Utc::now(),
    ///     event_time,
    ///     headers: HashMap::new(),
    ///     id: "msg1".to_string(),
    /// };
    /// let message = Message::from_accumulator_request(request);
    /// assert_eq!(message.event_time(), event_time);
    /// ```
    pub fn event_time(&self) -> DateTime<Utc> {
        self.event_time
    }

    /// Returns the watermark associated with this message.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::accumulator::{Message, AccumulatorRequest};
    /// use chrono::Utc;
    /// use std::collections::HashMap;
    ///
    /// let watermark = Utc::now();
    /// let request = AccumulatorRequest {
    ///     keys: vec!["key1".to_string()],
    ///     value: vec![1, 2, 3],
    ///     watermark,
    ///     event_time: Utc::now(),
    ///     headers: HashMap::new(),
    ///     id: "msg1".to_string(),
    /// };
    /// let message = Message::from_accumulator_request(request);
    /// assert_eq!(message.watermark(), watermark);
    /// ```
    pub fn watermark(&self) -> DateTime<Utc> {
        self.watermark
    }
}

/// Incoming request into the accumulator handler of [`Accumulator`].
#[derive(Debug, Clone)]
pub struct AccumulatorRequest {
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
    /// ID for deduplication.
    pub id: String,
}

/// Commands that can be sent to the task manager
#[derive(Debug)]
enum TaskManagerCommand {
    CreateTask {
        keyed_window: proto::KeyedWindow,
        payload: Option<proto::Payload>,
        response_tx: oneshot::Sender<Result<(), Error>>,
    },
    AppendToTask {
        keyed_window: proto::KeyedWindow,
        payload: Option<proto::Payload>,
        response_tx: oneshot::Sender<Result<(), Error>>,
    },
    CloseTask {
        keyed_window: proto::KeyedWindow,
    },
    Shutdown,
}

/// Represents an accumulator task for a specific keyed window
struct AccumulatorTask {
    input_tx: mpsc::Sender<AccumulatorRequest>,
    done_rx: oneshot::Receiver<()>,
    handle: tokio::task::JoinHandle<()>,
}

impl AccumulatorTask {
    async fn new<A: Accumulator + Send + Sync + 'static>(
        keyed_window: proto::KeyedWindow,
        accumulator: A,
        response_tx: mpsc::Sender<Result<proto::AccumulatorResponse, Error>>,
    ) -> Self {
        let (input_tx, input_rx) = mpsc::channel::<AccumulatorRequest>(CHANNEL_SIZE);
        let (output_tx, mut output_rx) = mpsc::channel::<Message>(CHANNEL_SIZE);
        let (done_tx, done_rx) = oneshot::channel();

        // Clone response_tx before moving into closures
        let handler_tx = response_tx.clone();

        // Spawn the main task that runs the user's accumulate function
        let task_join_handler = tokio::spawn(async move {
            // Spawn a task to handle output messages
            let output_task_response_tx = response_tx.clone();
            let output_handle = tokio::spawn(async move {
                let mut latest_watermark =
                    DateTime::<Utc>::from_timestamp_millis(-1).unwrap_or_else(Utc::now);

                while let Some(message) = output_rx.recv().await {
                    // Update latest watermark if message has a newer one
                    // For accumulator, we need to track watermark progression
                    latest_watermark = message.watermark().max(latest_watermark);

                    let window = proto::KeyedWindow {
                        start: keyed_window.start,
                        end: shared::prost_timestamp_from_utc(latest_watermark),
                        slot: keyed_window.slot.clone(),
                        keys: keyed_window.keys.clone(),
                    };

                    let response = proto::AccumulatorResponse::from((&message, window));

                    if let Err(e) = output_task_response_tx.send(Ok(response)).await {
                        error!("Failed to send response: {}", e);
                        return;
                    }
                }

                // Send EOF response
                let eof_response = proto::AccumulatorResponse {
                    payload: None,
                    window: Some(proto::KeyedWindow {
                        start: keyed_window.start,
                        end: shared::prost_timestamp_from_utc(latest_watermark),
                        slot: keyed_window.slot.clone(),
                        keys: keyed_window.keys.clone(),
                    }),
                    tags: vec![],
                    eof: true,
                };
                let _ = output_task_response_tx.send(Ok(eof_response)).await;
            });

            // Execute the user's accumulate function
            accumulator.accumulate(input_rx, output_tx).await;

            // Wait for output task to complete
            let _ = output_handle.await;
        });

        // We spawn a separate task to await the join handler so that in case of any unhandled errors in the user-defined
        // code will immediately be propagated to the client.
        let handle = tokio::spawn(async move {
            if let Err(e) = task_join_handler.await {
                let _ = handler_tx
                    .send(Err(Error::AccumulatorError(ErrorKind::UserDefinedError(
                        format!("Task error: {}", e),
                    ))))
                    .await;
            }

            // Send a message indicating that the task has finished
            let _ = done_tx.send(());
        });

        Self {
            input_tx,
            done_rx,
            handle,
        }
    }

    /// Send data to the task
    async fn send(&self, request: AccumulatorRequest) -> Result<(), Error> {
        self.input_tx.send(request).await.map_err(|e| {
            Error::AccumulatorError(ErrorKind::InternalError(format!(
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
}

/// Task manager that handles accumulator tasks using actor pattern
struct AccumulatorTaskManager<C> {
    creator: Arc<C>,
    tasks: HashMap<String, AccumulatorTask>,
    response_tx: mpsc::Sender<Result<proto::AccumulatorResponse, Error>>,
    is_shutdown: Arc<AtomicBool>,
}

impl<C> AccumulatorTaskManager<C>
where
    C: AccumulatorCreator + Send + Sync + 'static,
{
    fn new(
        creator: Arc<C>,
        response_tx: mpsc::Sender<Result<proto::AccumulatorResponse, Error>>,
    ) -> Self {
        Self {
            creator,
            tasks: HashMap::new(),
            response_tx,
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
                    TaskManagerCommand::CloseTask { keyed_window } => {
                        self.handle_close_task(keyed_window).await;
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

    /// Creates and starts a new accumulator task
    async fn handle_create_task(
        &mut self,
        keyed_window: proto::KeyedWindow,
        payload: Option<proto::Payload>,
    ) -> Result<(), Error> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(Error::AccumulatorError(ErrorKind::InternalError(
                "Task manager is shutdown".to_string(),
            )));
        }

        let key = generate_key(&keyed_window);

        if self.tasks.contains_key(&key) {
            return Err(Error::AccumulatorError(ErrorKind::InternalError(format!(
                "Task already exists for key: {}",
                key
            ))));
        }

        // Create new accumulator
        let accumulator = self.creator.create();

        // Create new task
        let task = AccumulatorTask::new(keyed_window, accumulator, self.response_tx.clone()).await;

        // Send payload if present
        if let Some(payload) = payload {
            let accumulator_request: AccumulatorRequest = payload.into();
            task.send(accumulator_request).await?;
        }

        self.tasks.insert(key, task);
        Ok(())
    }

    /// Append to an existing task
    async fn handle_append_to_task(
        &mut self,
        keyed_window: proto::KeyedWindow,
        payload: Option<proto::Payload>,
    ) -> Result<(), Error> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(Error::AccumulatorError(ErrorKind::InternalError(
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
            let accumulator_request = AccumulatorRequest::from(payload);
            if let Some(task) = self.tasks.get(&key) {
                task.send(accumulator_request).await?;
            }
        }

        Ok(())
    }

    /// Close the task (closes input channel and waits for task to finish)
    async fn handle_close_task(&mut self, keyed_window: proto::KeyedWindow) {
        let key = generate_key(&keyed_window);
        if let Some(task) = self.tasks.remove(&key) {
            task.close().await;
        }
    }

    /// Shutdown the accumulator task manager and abort all tasks
    async fn handle_shutdown(&mut self) {
        self.is_shutdown.store(true, Ordering::Relaxed);
        let tasks: Vec<_> = self.tasks.drain().collect();
        for (_, task) in tasks {
            task.abort().await;
        }
    }
}

/// Accumulator service implementation
struct AccumulatorService<C> {
    creator: Arc<C>,
    shutdown_tx: mpsc::Sender<()>,
    cancellation_token: CancellationToken,
}

#[async_trait]
impl<C> proto::accumulator_server::Accumulator for AccumulatorService<C>
where
    C: AccumulatorCreator + Send + Sync + 'static,
{
    type AccumulateFnStream = ReceiverStream<Result<proto::AccumulatorResponse, Status>>;

    async fn accumulate_fn(
        &self,
        request: Request<Streaming<proto::AccumulatorRequest>>,
    ) -> Result<Response<Self::AccumulateFnStream>, Status> {
        let creator = Arc::clone(&self.creator);
        let shutdown_tx = self.shutdown_tx.clone();
        let cancellation_token = self.cancellation_token.child_token();

        // Create response channel for gRPC client
        let (grpc_response_tx, grpc_response_rx) =
            mpsc::channel::<Result<proto::AccumulatorResponse, Status>>(CHANNEL_SIZE);

        // Internal response channel for task manager
        let (response_tx, mut response_rx) =
            mpsc::channel::<Result<proto::AccumulatorResponse, Error>>(CHANNEL_SIZE);

        // Start task manager
        let task_manager = AccumulatorTaskManager::new(creator, response_tx);

        let request_shutdown_tx = shutdown_tx.clone();
        let request_cancellation_token = cancellation_token.clone();
        // Spawn task to handle incoming requests
        tokio::spawn(async move {
            let mut stream = request.into_inner();

            let task_manager_tx = task_manager.start();

            loop {
                tokio::select! {
                    // Listen for incoming requests
                    result = tokio_stream::StreamExt::next(&mut stream) => {
                        match result {
                            Some(Ok(req)) => {
                                if let Err(e) = handle_accumulator_request(req, &task_manager_tx).await {
                                    let _ = request_shutdown_tx.send(()).await;
                                    error!("Error handling accumulator request: {}", e);
                                    break;
                                }
                            }
                            Some(Err(e)) => {
                                error!("Error receiving accumulator request: {}", e);
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

            // Shutdown task manager
            let _ = task_manager_tx.send(TaskManagerCommand::Shutdown).await;
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
                                    // Client disconnected, signal request task to stop
                                    response_cancellation_token.cancel();
                                    break;
                                }
                            }
                            Some(Err(error)) => {
                                error!("Error from accumulator task manager: {}", error);
                                let _ = grpc_response_tx.send(Err(Status::internal(error.to_string()))).await;
                                // Signal request task to stop due to error
                                response_cancellation_token.cancel();
                                let _ = shutdown_tx.send(()).await;
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

/// Handle individual accumulator request
async fn handle_accumulator_request(
    request: proto::AccumulatorRequest,
    task_manager_tx: &mpsc::Sender<TaskManagerCommand>,
) -> Result<(), Error> {
    let operation = request.operation.as_ref().ok_or_else(|| {
        Error::AccumulatorError(ErrorKind::InternalError("Missing operation".to_string()))
    })?;

    let keyed_window = operation
        .keyed_window
        .as_ref()
        .ok_or_else(|| {
            Error::AccumulatorError(ErrorKind::InternalError("Missing keyed window".to_string()))
        })?
        .clone();

    match Event::try_from(operation.event) {
        Ok(Event::Open) => {
            let (response_tx, response_rx) = oneshot::channel();
            task_manager_tx
                .send(TaskManagerCommand::CreateTask {
                    keyed_window,
                    payload: request.payload,
                    response_tx,
                })
                .await
                .map_err(|e| {
                    Error::AccumulatorError(ErrorKind::InternalError(format!(
                        "Failed to send create task command: {}",
                        e
                    )))
                })?;
            response_rx.await.map_err(|e| {
                Error::AccumulatorError(ErrorKind::InternalError(format!(
                    "Failed to receive create task response: {}",
                    e
                )))
            })?
        }
        Ok(Event::Append) => {
            let (response_tx, response_rx) = oneshot::channel();
            task_manager_tx
                .send(TaskManagerCommand::AppendToTask {
                    keyed_window,
                    payload: request.payload,
                    response_tx,
                })
                .await
                .map_err(|e| {
                    Error::AccumulatorError(ErrorKind::InternalError(format!(
                        "Failed to send append task command: {}",
                        e
                    )))
                })?;
            response_rx.await.map_err(|e| {
                Error::AccumulatorError(ErrorKind::InternalError(format!(
                    "Failed to receive append task response: {}",
                    e
                )))
            })?
        }
        Ok(Event::Close) => {
            task_manager_tx
                .send(TaskManagerCommand::CloseTask { keyed_window })
                .await
                .map_err(|e| {
                    Error::AccumulatorError(ErrorKind::InternalError(format!(
                        "Failed to send close task command: {}",
                        e
                    )))
                })?;
            Ok(())
        }
        Err(_) => Err(Error::AccumulatorError(ErrorKind::InternalError(format!(
            "Unknown operation event: {}",
            operation.event
        )))),
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

/// Implement From trait for converting proto::Payload to AccumulatorRequest
impl From<proto::Payload> for AccumulatorRequest {
    fn from(payload: proto::Payload) -> Self {
        AccumulatorRequest {
            keys: payload.keys,
            value: payload.value,
            watermark: payload
                .watermark
                .map(|ts| shared::utc_from_timestamp(Some(ts)))
                .expect("watermark should be set"),
            event_time: payload
                .event_time
                .map(|ts| shared::utc_from_timestamp(Some(ts)))
                .expect("event_time should be set"),
            headers: payload.headers,
            id: payload.id,
        }
    }
}

/// Implement From trait for converting Message to proto::Payload
impl From<&Message> for proto::Payload {
    fn from(message: &Message) -> Self {
        proto::Payload {
            keys: message.keys().clone().unwrap_or_default(),
            value: message.value().clone(),
            event_time: shared::prost_timestamp_from_utc(message.event_time()),
            watermark: shared::prost_timestamp_from_utc(message.watermark()),
            id: message.id().to_string(),
            headers: message.headers().clone(),
        }
    }
}

/// Implement From trait for converting (Message, KeyedWindow, tags) to proto::AccumulatorResponse
impl From<(&Message, proto::KeyedWindow)> for proto::AccumulatorResponse {
    fn from((message, window): (&Message, proto::KeyedWindow)) -> Self {
        proto::AccumulatorResponse {
            tags: message.tags().clone().unwrap_or_default(),
            payload: Some(proto::Payload::from(message)),
            window: Some(window),
            eof: false,
        }
    }
}

/// gRPC server for accumulator service
#[derive(Debug)]
pub struct Server<C> {
    config: ServerConfig,
    creator: Option<C>,
    _cleanup: SocketCleanup,
}

impl<C> Server<C> {
    /// Create a new Server with the given accumulator service
    pub fn new(creator: C) -> Self {
        let config = ServerConfig::new(SOCK_ADDR, SERVER_INFO_FILE);
        let cleanup = SocketCleanup::new(SOCK_ADDR.into(), SERVER_INFO_FILE.into());

        Server {
            config,
            creator: Some(creator),
            _cleanup: cleanup,
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    /// Default value is `/var/run/numaflow/accumulator.sock`
    pub fn with_socket_file(mut self, file: impl Into<std::path::PathBuf>) -> Self {
        let file_path = file.into();
        self.config = self.config.with_socket_file(&file_path);
        self._cleanup = SocketCleanup::new(file_path, self.config.server_info_file().to_path_buf());
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections.
    pub fn socket_file(&self) -> &std::path::Path {
        self.config.socket_file()
    }

    /// Set the maximum size of an encoded and decoded gRPC message. The value of `message_size` is in bytes.
    /// Default value is 64MB.
    pub fn with_max_message_size(mut self, message_size: usize) -> Self {
        self.config = self.config.with_max_message_size(message_size);
        self
    }

    /// Get the maximum size of an encoded and decoded gRPC message in bytes. Default value is 64MB.
    pub fn max_message_size(&self) -> usize {
        self.config.max_message_size()
    }

    /// Change the file in which numaflow server information is stored on start up to the new value.
    /// Default value is `/var/run/numaflow/accumulator-server-info`
    pub fn with_server_info_file(mut self, file: impl Into<std::path::PathBuf>) -> Self {
        let file_path = file.into();
        self.config = self.config.with_server_info_file(&file_path);
        self._cleanup = SocketCleanup::new(self.config.socket_file().to_path_buf(), file_path);
        self
    }

    /// Get the path to the file where numaflow server info is stored.
    pub fn server_info_file(&self) -> &std::path::Path {
        self.config.server_info_file()
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown(
        &mut self,
        user_shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        C: AccumulatorCreator + Send + Sync + 'static,
    {
        let info = crate::shared::ServerInfo::new(ContainerType::Accumulator);
        let listener = shared::create_listener_stream(
            self.config.socket_file(),
            self.config.server_info_file(),
            info,
        )?;
        let creator = self.creator.take().unwrap();
        let (internal_shutdown_tx, internal_shutdown_rx) = mpsc::channel(1);
        let cln_token = CancellationToken::new();

        let accumulator_svc = AccumulatorService {
            creator: Arc::new(creator),
            shutdown_tx: internal_shutdown_tx,
            cancellation_token: cln_token.clone(),
        };

        let accumulator_svc = proto::accumulator_server::AccumulatorServer::new(accumulator_svc)
            .max_encoding_message_size(self.config.max_message_size())
            .max_decoding_message_size(self.config.max_message_size());

        let shutdown =
            shared::shutdown_signal(internal_shutdown_rx, Some(user_shutdown_rx), cln_token);

        tonic::transport::Server::builder()
            .add_service(accumulator_svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await?;

        Ok(())
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates
    /// graceful shutdown of gRPC server when either one of the signal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        C: AccumulatorCreator + Send + Sync + 'static,
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
    use tokio::net::UnixStream;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::Request;
    use tonic::transport::Uri;
    use tower::service_fn;

    use crate::accumulator;
    use crate::accumulator::proto::accumulator_client::AccumulatorClient;

    struct Sum;

    #[tonic::async_trait]
    impl accumulator::Accumulator for Sum {
        async fn accumulate(
            &self,
            mut input: mpsc::Receiver<accumulator::AccumulatorRequest>,
            output: mpsc::Sender<accumulator::Message>,
        ) {
            let mut sum = 0;
            let mut keys = Vec::new();
            let mut last_request: Option<accumulator::AccumulatorRequest> = None;
            while let Some(rr) = input.recv().await {
                if keys.is_empty() {
                    keys = rr.keys.clone();
                }
                sum += std::str::from_utf8(&rr.value)
                    .unwrap()
                    .parse::<i32>()
                    .unwrap();
                last_request = Some(rr);
            }

            // Create a message from the last request and update the value and keys
            if let Some(request) = last_request {
                let message = accumulator::Message::from_accumulator_request(request)
                    .with_value(sum.to_string().into_bytes())
                    .with_keys(keys);
                let _ = output.send(message).await;
            }
        }
    }

    struct SumCreator;

    impl accumulator::AccumulatorCreator for SumCreator {
        type A = Sum;
        fn create(&self) -> Sum {
            Sum {}
        }
    }

    async fn setup_server<C: accumulator::AccumulatorCreator + Send + Sync + 'static>(
        creator: C,
    ) -> Result<(accumulator::Server<C>, PathBuf, PathBuf), Box<dyn Error>> {
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("accumulator.sock");
        let server_info_file = tmp_dir.path().join("accumulator-server-info");

        let server = accumulator::Server::new(creator)
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file)
            .with_max_message_size(10240);

        Ok((server, sock_file, server_info_file))
    }

    async fn setup_client(
        sock_file: PathBuf,
    ) -> Result<AccumulatorClient<tonic::transport::Channel>, Box<dyn Error>> {
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

        let client = AccumulatorClient::new(channel);

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
    async fn test_accumulator_operations() -> Result<(), Box<dyn Error>> {
        let (mut server, sock_file, _) = setup_server(SumCreator).await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = setup_client(sock_file).await?;

        let (tx, rx) = mpsc::channel(1);

        // Spawn a task to send AccumulatorRequests to the channel
        tokio::spawn(async move {
            // Test CREATE operation
            let create_request = accumulator::proto::AccumulatorRequest {
                payload: Some(accumulator::proto::Payload {
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
                    id: "msg1".to_string(),
                    headers: Default::default(),
                }),
                operation: Some(accumulator::proto::accumulator_request::WindowOperation {
                    event: accumulator::proto::accumulator_request::window_operation::Event::Open
                        as i32,
                    keyed_window: Some(accumulator::proto::KeyedWindow {
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
                    }),
                }),
            };

            tx.send(create_request).await.unwrap();

            // Test APPEND operation
            let append_request = accumulator::proto::AccumulatorRequest {
                payload: Some(accumulator::proto::Payload {
                    keys: vec!["key1".to_string()],
                    value: "10".as_bytes().to_vec(),
                    watermark: Some(Timestamp {
                        seconds: 80000,
                        nanos: 0,
                    }),
                    event_time: Some(Timestamp {
                        seconds: 90000,
                        nanos: 0,
                    }),
                    id: "msg2".to_string(),
                    headers: Default::default(),
                }),
                operation: Some(accumulator::proto::accumulator_request::WindowOperation {
                    event: accumulator::proto::accumulator_request::window_operation::Event::Append
                        as i32,
                    keyed_window: Some(accumulator::proto::KeyedWindow {
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
                    }),
                }),
            };

            tx.send(append_request).await.unwrap();

            // Test CLOSE operation
            let close_request = accumulator::proto::AccumulatorRequest {
                payload: None,
                operation: Some(accumulator::proto::accumulator_request::WindowOperation {
                    event: accumulator::proto::accumulator_request::window_operation::Event::Close
                        as i32,
                    keyed_window: Some(accumulator::proto::KeyedWindow {
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
                    }),
                }),
            };

            tx.send(close_request).await.unwrap();
        });

        // Convert the receiver end of the channel into a stream
        let stream = ReceiverStream::new(rx);

        // Create a tonic::Request from the stream
        let request = Request::new(stream);

        // Send the request to the server
        let resp = client.accumulate_fn(request).await?;

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
            if let Some(payload) = response.payload.as_ref() {
                assert_eq!(payload.keys, vec!["key1".to_string()]);
                assert_eq!(payload.value, "15".as_bytes().to_vec());
                found_result = true;
            }

            if response.eof {
                found_eof = true;
            }

            if let Some(keyed_window) = response.window.as_ref() {
                assert_eq!(keyed_window.keys, vec!["key1".to_string()]);
                if let Some(start) = keyed_window.start.as_ref() {
                    assert_eq!(start.seconds, 60000);
                }
                if let Some(end) = keyed_window.end.as_ref() {
                    assert_eq!(end.seconds, 80000);
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
    async fn test_from_traits() -> Result<(), Box<dyn Error>> {
        use crate::accumulator::{AccumulatorRequest, Message, proto};
        use crate::shared;
        use chrono::Utc;
        use std::collections::HashMap;

        // Test From<proto::Payload> for AccumulatorRequest
        let proto_payload = proto::Payload {
            keys: vec!["key1".to_string()],
            value: vec![1, 2, 3],
            event_time: shared::prost_timestamp_from_utc(Utc::now()),
            watermark: shared::prost_timestamp_from_utc(Utc::now()),
            id: "test-id".to_string(),
            headers: HashMap::new(),
        };

        let accumulator_request = AccumulatorRequest::from(proto_payload.clone());
        assert_eq!(accumulator_request.keys, proto_payload.keys);
        assert_eq!(accumulator_request.value, proto_payload.value);
        assert_eq!(accumulator_request.id, proto_payload.id);

        // Test From<&Message> for proto::Payload
        let mut message = Message::from_accumulator_request(accumulator_request);
        let tags = vec!["tag1".to_string()];
        message = message.with_tags(tags.clone());

        let proto_payload_from_message = proto::Payload::from(&message);
        assert_eq!(
            proto_payload_from_message.keys,
            message.keys().clone().unwrap_or_default()
        );
        assert_eq!(proto_payload_from_message.value, *message.value());
        assert_eq!(proto_payload_from_message.id, message.id());

        // Test From<(Message, KeyedWindow, tags, eof)> for proto::AccumulatorResponse
        let keyed_window = proto::KeyedWindow {
            start: shared::prost_timestamp_from_utc(Utc::now()),
            end: shared::prost_timestamp_from_utc(Utc::now()),
            slot: "slot-0".to_string(),
            keys: vec!["key1".to_string()],
        };

        let response = proto::AccumulatorResponse::from((&message, keyed_window.clone()));
        assert!(!response.eof);
        assert!(response.payload.is_some());
        assert_eq!(response.tags, tags);
        assert_eq!(response.window, Some(keyed_window.clone()));

        Ok(())
    }

    #[tokio::test]
    async fn test_message_from_datum() -> Result<(), Box<dyn Error>> {
        use crate::accumulator::{AccumulatorRequest, Message};
        use chrono::Utc;
        use std::collections::HashMap;

        // Create a sample AccumulatorRequest
        let mut headers = HashMap::new();
        headers.insert("header1".to_string(), "value1".to_string());
        headers.insert("header2".to_string(), "value2".to_string());

        let event_time = Utc::now();
        let watermark = Utc::now();

        let datum = AccumulatorRequest {
            keys: vec!["key1".to_string(), "key2".to_string()],
            value: vec![1, 2, 3, 4],
            watermark,
            event_time,
            headers: headers.clone(),
            id: "test-id".to_string(),
        };

        // Create message from datum
        let message = Message::from_accumulator_request(datum);

        // Test getter methods
        assert_eq!(
            message.keys(),
            &Some(vec!["key1".to_string(), "key2".to_string()])
        );
        assert_eq!(message.value(), &vec![1, 2, 3, 4]);
        assert_eq!(message.tags(), &None);
        assert_eq!(message.id(), "test-id");
        assert_eq!(message.headers(), &headers);
        assert_eq!(message.event_time(), event_time);
        assert_eq!(message.watermark(), watermark);

        // Test builder methods
        let updated_message = message
            .with_keys(vec!["new_key".to_string()])
            .with_value(vec![5, 6, 7])
            .with_tags(vec!["tag1".to_string(), "tag2".to_string()]);

        // Verify updates
        assert_eq!(updated_message.keys(), &Some(vec!["new_key".to_string()]));
        assert_eq!(updated_message.value(), &vec![5, 6, 7]);
        assert_eq!(
            updated_message.tags(),
            &Some(vec!["tag1".to_string(), "tag2".to_string()])
        );

        // Verify read-only fields remain unchanged
        assert_eq!(updated_message.id(), "test-id");
        assert_eq!(updated_message.headers(), &headers);
        assert_eq!(updated_message.event_time(), event_time);
        assert_eq!(updated_message.watermark(), watermark);

        Ok(())
    }
}
