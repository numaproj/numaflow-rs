use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::{async_trait, Request, Response, Status};

use crate::proto::side_input as proto;
use crate::shared::{
    self, shutdown_signal, ContainerType, ServerConfig, ServiceError, SocketCleanup,
    DEFAULT_SIDEINPUT_SERVER_INFO_FILE, DEFAULT_SIDEINPUT_SOCK_ADDR,
};

struct SideInputService<T> {
    handler: Arc<T>,
    shutdown_tx: mpsc::Sender<()>,
    cancellation_token: CancellationToken,
}

/// The `SideInputer` trait defines a method for retrieving side input data.
#[async_trait]
pub trait SideInputer {
    ///
    /// # Example
    ///
    /// It defines a method retrieve_sideinput that returns current time as a byte vector or None,
    /// alternating on each call.
    /// More about side input can be found [here](https://numaflow.numaproj.io/specifications/side-inputs/).
    ///
    /// Implementing the `SideInputer` trait for a `SideInputHandler` struct:
    ///
    /// ```no_run
    /// use std::time::{SystemTime, UNIX_EPOCH};
    /// use numaflow::sideinput::SideInputer;
    /// use tonic::{async_trait};
    /// use std::sync::Mutex;
    /// use numaflow::sideinput;
    ///
    /// struct SideInputHandler {
    ///     counter: Mutex<u32>,
    /// }
    ///
    /// impl SideInputHandler {
    ///     pub fn new() -> Self {
    ///         SideInputHandler {
    ///             counter: Mutex::new(0),
    ///         }
    ///     }
    /// }
    ///
    /// #[async_trait]
    /// impl SideInputer for SideInputHandler {
    ///
    ///     async fn retrieve_sideinput(& self) -> Option<Vec<u8>> {
    ///         let current_time = SystemTime::now()
    ///             .duration_since(UNIX_EPOCH)
    ///             .expect("Time went backwards");
    ///         let message = format!("an example: {:?}", current_time);
    ///
    ///         let mut counter = self.counter.lock().unwrap();
    ///         *counter = (*counter + 1) % 10;
    ///         if *counter % 2 == 0 {
    ///             None
    ///         } else {
    ///             Some(message.into_bytes())
    ///         }
    ///     }
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///
    /// let side_input_handler = SideInputHandler::new();
    ///     sideinput::Server::new(side_input_handler).start().await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// The `retrieve_sideinput` method is implemented to return an `Option<Vec<u8>>`. In this example, the method returns a message containing the current time if the counter is odd, and `None` if the counter is even.
    async fn retrieve_sideinput(&self) -> Option<Vec<u8>>;
}

#[async_trait]
impl<T> proto::side_input_server::SideInput for SideInputService<T>
where
    T: SideInputer + Send + Sync + 'static,
{
    async fn retrieve_side_input(
        &self,
        _: Request<()>,
    ) -> Result<Response<proto::SideInputResponse>, Status> {
        let handler = Arc::clone(&self.handler);
        let shutdown_tx = self.shutdown_tx.clone();
        let handle = tokio::spawn(async move { handler.retrieve_sideinput().await });

        tokio::select! {
            msg = handle => {
                match msg {
                    Ok(Some(value)) => {
                        Ok(Response::new(proto::SideInputResponse {
                            value,
                            no_broadcast: false,
                        }))
                    }
                    Ok(None) => {
                        Ok(Response::new(proto::SideInputResponse {
                            value: Vec::new(),
                            no_broadcast: true,
                        }))
                    }
                    Err(e) => {
                        shutdown_tx.send(()).await.expect("Failed to send shutdown signal");
                        Err(Status::internal(Server::<()>::user_error(e.to_string()).to_string()))
                    }
                }
            }
            _ = self.cancellation_token.cancelled() => {
                Err(Status::internal(Server::<()>::internal_error("Server is shutting down").to_string()))
            },
        }
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}

/// gRPC server to start a side input service
#[derive(Debug)]
pub struct Server<T> {
    config: ServerConfig,
    svc: Option<T>,
    _cleanup: SocketCleanup,
}

impl<T> Server<T> {
    /// Create a new Server with the given side input service
    pub fn new(sideinput_svc: T) -> Self {
        let config = ServerConfig::new(
            DEFAULT_SIDEINPUT_SOCK_ADDR,
            DEFAULT_SIDEINPUT_SERVER_INFO_FILE,
        );
        let cleanup = SocketCleanup::new(config.sock_addr.clone());

        Self {
            config,
            svc: Some(sideinput_svc),
            _cleanup: cleanup,
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    /// Default value is `/var/run/numaflow/sideinput.sock`
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.config.sock_addr = file.into();
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections. Default value is `/var/run/numaflow/sideinput.sock`
    pub fn socket_file(&self) -> &std::path::Path {
        self.config.sock_addr.as_path()
    }

    /// Set the maximum size of an encoded and decoded gRPC message. The value of `message_size` is in bytes. Default value is 64MB.
    pub fn with_max_message_size(mut self, message_size: usize) -> Self {
        self.config.max_message_size = message_size;
        self
    }

    /// Get the maximum size of an encoded and decoded gRPC message in bytes. Default value is 64MB.
    pub fn max_message_size(&self) -> usize {
        self.config.max_message_size
    }

    /// Change the file in which numaflow server information is stored on start up to the new value. Default value is `/var/run/numaflow/sideinput-server-info`
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.config.server_info_file = file.into();
        self
    }

    /// Get the path to the file where numaflow server info is stored. Default value is `/var/run/numaflow/sideinput-server-info`
    pub fn server_info_file(&self) -> &std::path::Path {
        self.config.server_info_file.as_path()
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown(
        &mut self,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: SideInputer + Send + Sync + 'static,
    {
        let info = shared::ServerInfo::new(ContainerType::SideInput);
        let listener = shared::create_listener_stream(
            &self.config.sock_addr,
            &self.config.server_info_file,
            info,
        )?;
        let handler = self.svc.take().unwrap();
        let (internal_shutdown_tx, internal_shutdown_rx) = mpsc::channel(1);
        let cln_token = CancellationToken::new();

        let sideinput_svc = SideInputService {
            handler: Arc::new(handler),
            shutdown_tx: internal_shutdown_tx,
            cancellation_token: cln_token.clone(),
        };
        let sideinput_svc = proto::side_input_server::SideInputServer::new(sideinput_svc)
            .max_encoding_message_size(self.config.max_message_size)
            .max_decoding_message_size(self.config.max_message_size);

        let shutdown = shutdown_signal(internal_shutdown_rx, Some(shutdown_rx));

        // will call cancel_token.cancel() on drop of _drop_guard
        let _drop_guard = cln_token.drop_guard();

        tonic::transport::Server::builder()
            .add_service(sideinput_svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await?;

        Ok(())
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the signal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: SideInputer + Send + Sync + 'static,
    {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        self.start_with_shutdown(shutdown_rx).await
    }
}

/// Implementation of ServiceError trait for Sideinput service
impl<T> ServiceError for Server<T> {
    fn service_name() -> &'static str {
        "sideinput"
    }
}
