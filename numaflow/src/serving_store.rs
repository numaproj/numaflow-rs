use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Status};

use crate::error::Error::ServingStoreError;
use crate::error::ErrorKind::{InternalError, UserDefinedError};
use crate::servers::serving::{
    self as serving_pb, GetRequest, GetResponse, PutRequest, PutResponse,
};
use crate::shared::{self, ContainerType};

const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_SOCK_ADDR: &str = "/var/run/numaflow/serving.sock";
const DEFAULT_SERVER_INFO_FILE: &str = "/var/run/numaflow/serving-server-info";

/// ServingStore trait for implementing user defined stores.
///
/// Types implementing this trait can be passed as user-defined store handle.
#[tonic::async_trait]
pub trait ServingStore {
    /// The store handle is given a [`Data`] payload to store. This `Data` may be queried with its
    /// `id` at a later point using the `get` method.
    async fn put(&self, data: Data);
    /// Return the data for the specified `id`
    async fn get(&self, id: String) -> Data;
}

struct ServingService<T: ServingStore> {
    handler: Arc<T>,
    shutdown_tx: mpsc::Sender<()>,
    cancellation_token: CancellationToken,
}

pub struct Data {
    pub id: String,
    pub payloads: Vec<Payload>,
}

pub struct Payload {
    pub origin: String,
    pub value: Vec<u8>,
}

impl From<Data> for GetResponse {
    fn from(value: Data) -> Self {
        let Data { id, payloads } = value;
        Self {
            id,
            payloads: payloads
                .into_iter()
                .map(|p| serving_pb::Payload {
                    origin: p.origin,
                    value: p.value,
                })
                .collect(),
        }
    }
}

impl From<PutRequest> for Data {
    fn from(value: PutRequest) -> Self {
        let PutRequest { id, payloads } = value;
        Self {
            id,
            payloads: payloads
                .into_iter()
                .map(|p| Payload {
                    origin: p.origin,
                    value: p.value,
                })
                .collect(),
        }
    }
}

#[tonic::async_trait]
impl<T> serving_pb::serving_store_server::ServingStore for ServingService<T>
where
    T: ServingStore + Send + Sync + 'static,
{
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<tonic::Response<GetResponse>, tonic::Status> {
        let request = request.into_inner();
        let handler = Arc::clone(&self.handler);
        let handle = tokio::spawn(async move { handler.get(request.id).await });
        let shutdown_tx = self.shutdown_tx.clone();
        let cancellation_token = self.cancellation_token.clone();

        // Wait for the handler to finish processing the request. If the server is shutting down(token will be cancelled),
        // then return an error.
        tokio::select! {
            result = handle => {
                match result {
                    Ok(result) => Ok(tonic::Response::new(result.into())),
                    Err(e) => {
                        tracing::error!("Error in ServingStore handler: {:?}", e);
                        // Send a shutdown signal to the server to do a graceful shutdown because there was
                        // a panic in the handler.
                        shutdown_tx
                            .send(())
                            .await
                            .expect("Sending shutdown signal to gRPC server");
                        Err(Status::internal(ServingStoreError(UserDefinedError(e.to_string())).to_string()))
                    }
                }
            },

            _ = cancellation_token.cancelled() => {
                Err(Status::internal(ServingStoreError(InternalError("Server is shutting down".to_string())).to_string()))
            },
        }
    }

    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        let request = request.into_inner();
        let handler = Arc::clone(&self.handler);
        let handle = tokio::spawn(async move { handler.put(request.into()).await });
        let shutdown_tx = self.shutdown_tx.clone();
        let cancellation_token = self.cancellation_token.clone();
        tokio::select! {
            result = handle => {
                match result {
                    Ok(_) => Ok(tonic::Response::new(PutResponse { success: true })),
                    Err(e) => {
                        tracing::error!("Error in ServingStore put handler: {:?}", e);
                        // Send a shutdown signal to the server to do a graceful shutdown because there was
                        // a panic in the handler.
                        shutdown_tx
                            .send(())
                            .await
                            .expect("Sending shutdown signal to gRPC server");
                        Err(Status::internal(ServingStoreError(UserDefinedError(e.to_string())).to_string()))
                    }
                }
            },

            _ = cancellation_token.cancelled() => {
                Err(Status::internal(ServingStoreError(InternalError("Server is shutting down".to_string())).to_string()))
            },
        }
    }

    async fn is_ready(
        &self,
        _: Request<()>,
    ) -> Result<tonic::Response<serving_pb::ReadyResponse>, Status> {
        Ok(tonic::Response::new(serving_pb::ReadyResponse {
            ready: true,
        }))
    }
}

/// gRPC server to start a `ServingStore` service
#[derive(Debug)]
pub struct Server<T> {
    sock_addr: PathBuf,
    max_message_size: usize,
    server_info_file: PathBuf,
    svc: Option<T>,
}

impl<T> Server<T> {
    pub fn new(svc: T) -> Self {
        let (sock_addr, server_info_file) =
            (DEFAULT_SOCK_ADDR.into(), DEFAULT_SERVER_INFO_FILE.into());

        Self {
            sock_addr,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            server_info_file,
            svc: Some(svc),
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    /// Default value is `/var/run/numaflow/serving.sock`
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.sock_addr = file.into();
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections. Default value is `/var/run/numaflow/serving.sock`
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

    /// Change the file in which numaflow server information is stored on start up to the new value. Default value is `/var/run/numaflow/serving-server-info`
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.server_info_file = file.into();
        self
    }

    /// Get the path to the file where numaflow server info is stored. Default value is `/var/run/numaflow/serving-server-info`
    pub fn server_info_file(&self) -> &std::path::Path {
        self.server_info_file.as_path()
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown(
        &mut self,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: ServingStore + Send + Sync + 'static,
    {
        let info = shared::ServerInfo::new(ContainerType::Serving);
        let listener =
            shared::create_listener_stream(&self.sock_addr, &self.server_info_file, info)?;
        let handler = self.svc.take().unwrap();
        let cln_token = CancellationToken::new();
        let (internal_shutdown_tx, internal_shutdown_rx) = mpsc::channel(1);

        let svc = ServingService {
            handler: Arc::new(handler),
            shutdown_tx: internal_shutdown_tx,
            cancellation_token: cln_token.clone(),
        };

        let svc = serving_pb::serving_store_server::ServingStoreServer::new(svc)
            .max_encoding_message_size(self.max_message_size)
            .max_decoding_message_size(self.max_message_size);

        let shutdown = shared::shutdown_signal(internal_shutdown_rx, Some(shutdown_rx));

        // will call cancel_token.cancel() on drop of _drop_guard
        let _drop_guard = cln_token.drop_guard();

        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await?;

        Ok(())
    }

    /// Starts the gRPC server. Automatically registers signal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the singal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: ServingStore + Send + Sync + 'static,
    {
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        self.start_with_shutdown(shutdown_rx).await
    }
}

impl<C> Drop for Server<C> {
    // Cleanup the socket file when the server is dropped so that when the server is restarted, it can bind to the
    // same address. UnixListener doesn't implement Drop trait, so we have to manually remove the socket file.
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.sock_addr);
    }
}
