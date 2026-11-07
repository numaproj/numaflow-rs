use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, async_trait};

use crate::shared;
use crate::{
    error::{Error, ErrorKind},
    proto::side_input as proto,
};
use shared::{ContainerType, ServerConfig, SocketCleanup, shutdown_signal};

/// Default socket address for sideinput service
pub const SOCK_ADDR: &str = "/var/run/numaflow/sideinput.sock";
/// Default server info file for sideinput service
pub const SERVER_INFO_FILE: &str = "/var/run/numaflow/sideinput-server-info";

/// Default directory path for storing side-input files
pub const DIR_PATH: &str = "/var/numaflow/side-inputs";

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
                        Err(Status::internal(Error::SideInputError(ErrorKind::UserDefinedError(e.to_string())).to_string()))
                    }
                }
            }
            _ = self.cancellation_token.cancelled() => {
                Err(Status::cancelled(Error::SideInputError(ErrorKind::InternalError("Server is shutting down".to_string())).to_string()))
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
        let config = ServerConfig::new(SOCK_ADDR, SERVER_INFO_FILE);
        let cleanup = SocketCleanup::new(SOCK_ADDR.into(), SERVER_INFO_FILE.into());

        Self {
            config,
            svc: Some(sideinput_svc),
            _cleanup: cleanup,
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    /// Default value is `/var/run/numaflow/sideinput.sock`
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        let file_path = file.into();
        self.config = self.config.with_socket_file(&file_path);
        self._cleanup = SocketCleanup::new(file_path, self.config.server_info_file().to_path_buf());
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections. Default value is `/var/run/numaflow/sideinput.sock`
    pub fn socket_file(&self) -> &std::path::Path {
        self.config.socket_file()
    }

    /// Set the maximum size of an encoded and decoded gRPC message. The value of `message_size` is in bytes. Default value is 64MB.
    pub fn with_max_message_size(mut self, message_size: usize) -> Self {
        self.config = self.config.with_max_message_size(message_size);
        self
    }

    /// Get the maximum size of an encoded and decoded gRPC message in bytes. Default value is 64MB.
    pub fn max_message_size(&self) -> usize {
        self.config.max_message_size()
    }

    /// Change the file in which numaflow server information is stored on start up to the new value. Default value is `/var/run/numaflow/sideinput-server-info`
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        let file_path = file.into();
        self.config = self.config.with_server_info_file(&file_path);
        self._cleanup = SocketCleanup::new(self.config.socket_file().to_path_buf(), file_path);
        self
    }

    /// Get the path to the file where numaflow server info is stored. Default value is `/var/run/numaflow/sideinput-server-info`
    pub fn server_info_file(&self) -> &std::path::Path {
        self.config.server_info_file()
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
            self.config.socket_file(),
            self.config.server_info_file(),
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
            .max_encoding_message_size(self.config.max_message_size())
            .max_decoding_message_size(self.config.max_message_size());

        let shutdown = shutdown_signal(internal_shutdown_rx, Some(shutdown_rx), cln_token);

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

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::PathBuf;
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::net::UnixStream;
    use tokio::sync::oneshot;
    use tonic::transport::{Channel, Uri};
    use tower::service_fn;

    use crate::proto::side_input::side_input_client::SideInputClient;
    use crate::sideinput;

    struct FixedSideInput(Option<Vec<u8>>);

    #[tonic::async_trait]
    impl sideinput::SideInputer for FixedSideInput {
        async fn retrieve_sideinput(&self) -> Option<Vec<u8>> {
            self.0.clone()
        }
    }

    async fn connect(sock_file: PathBuf) -> Result<SideInputClient<Channel>, Box<dyn Error>> {
        // https://github.com/hyperium/tonic/blob/master/examples/src/uds/client.rs
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
        Ok(SideInputClient::new(channel))
    }

    #[tokio::test]
    async fn sideinput_server() -> Result<(), Box<dyn Error>> {
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sideinput.sock");
        let server_info_file = tmp_dir.path().join("sideinput-server-info");

        let mut server = sideinput::Server::new(FixedSideInput(Some(b"side-value".to_vec())))
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file)
            .with_max_message_size(10240);

        assert_eq!(server.max_message_size(), 10240);
        assert_eq!(server.server_info_file(), server_info_file);
        assert_eq!(server.socket_file(), sock_file);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = connect(sock_file).await?;

        let ready = client.is_ready(()).await?.into_inner();
        assert!(ready.ready);

        let resp = client.retrieve_side_input(()).await?.into_inner();
        assert_eq!(resp.value, b"side-value".to_vec());
        assert!(!resp.no_broadcast);

        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");
        Ok(())
    }

    #[tokio::test]
    async fn sideinput_server_no_broadcast() -> Result<(), Box<dyn Error>> {
        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sideinput.sock");
        let server_info_file = tmp_dir.path().join("sideinput-server-info");

        let mut server = sideinput::Server::new(FixedSideInput(None))
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = connect(sock_file).await?;
        let resp = client.retrieve_side_input(()).await?.into_inner();
        assert!(resp.value.is_empty());
        assert!(resp.no_broadcast);

        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");
        Ok(())
    }

    #[cfg(feature = "test-panic")]
    #[tokio::test]
    async fn sideinput_server_panic() -> Result<(), Box<dyn Error>> {
        struct PanicSideInput;
        #[tonic::async_trait]
        impl sideinput::SideInputer for PanicSideInput {
            async fn retrieve_sideinput(&self) -> Option<Vec<u8>> {
                panic!("Panic in side input");
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("sideinput.sock");
        let server_info_file = tmp_dir.path().join("sideinput-server-info");

        let mut server = sideinput::Server::new(PanicSideInput)
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file);

        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = connect(sock_file).await?;
        let err = client
            .retrieve_side_input(())
            .await
            .expect_err("Expected error from server");
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(
            err.message().contains("Panic in side input"),
            "Expected the panic message, got: {}",
            err.message()
        );

        // server should shut down gracefully because there was a panic in the handler.
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
