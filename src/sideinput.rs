use std::future::Future;
use std::path::PathBuf;

use tonic::{async_trait, Request, Response, Status};

use crate::shared;

const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_SOCK_ADDR: &str = "/var/run/numaflow/sideinput.sock";
const DEFAULT_SERVER_INFO_FILE: &str = "/var/run/numaflow/sideinput-server-info";

mod proto {
    tonic::include_proto!("sideinput.v1");
}

struct SideInputService<T> {
    handler: T,
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
        let msg = self.handler.retrieve_sideinput().await;
        let si = match msg {
            Some(value) => proto::SideInputResponse {
                value,
                no_broadcast: false,
            },
            None => proto::SideInputResponse {
                value: Vec::new(),
                no_broadcast: true,
            },
        };

        Ok(Response::new(si))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}

/// gRPC server to start a side input service
#[derive(Debug)]
pub struct Server<T> {
    sock_addr: PathBuf,
    max_message_size: usize,
    server_info_file: PathBuf,
    svc: Option<T>,
}

impl<T> Server<T> {
    /// Create a new Server with the given side input service
    pub fn new(sideinput_svc: T) -> Self {
        Server {
            sock_addr: DEFAULT_SOCK_ADDR.into(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            server_info_file: DEFAULT_SERVER_INFO_FILE.into(),
            svc: Some(sideinput_svc),
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    /// Default value is `/var/run/numaflow/sideinput.sock`
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.sock_addr = file.into();
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections. Default value is `/var/run/numaflow/sideinput.sock`
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

    /// Change the file in which numaflow server information is stored on start up to the new value. Default value is `/var/run/numaflow/sideinput-server-info`
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.server_info_file = file.into();
        self
    }

    /// Get the path to the file where numaflow server info is stored. Default value is `/var/run/numaflow/sideinput-server-info`
    pub fn server_info_file(&self) -> &std::path::Path {
        self.server_info_file.as_path()
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown<F>(
        &mut self,
        shutdown: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
        where
            T: SideInputer + Send + Sync + 'static,
            F: Future<Output=()>,
    {
        let listener = shared::create_listener_stream(&self.sock_addr, &self.server_info_file)?;
        let handler = self.svc.take().unwrap();
        let sideinput_svc = SideInputService { handler };
        let sideinput_svc = proto::side_input_server::SideInputServer::new(sideinput_svc)
            .max_encoding_message_size(self.max_message_size)
            .max_decoding_message_size(self.max_message_size);

        tonic::transport::Server::builder()
            .add_service(sideinput_svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await
            .map_err(Into::into)
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the signal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
        where
            T: SideInputer + Send + Sync + 'static,
    {
        self.start_with_shutdown(shared::shutdown_signal()).await
    }
}
