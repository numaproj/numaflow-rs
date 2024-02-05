use chrono::{DateTime, Utc};
use std::path::PathBuf;
use tokio::sync::oneshot;
use tonic::{async_trait, Request, Response, Status};

use crate::map::mapper::{
    map_response, map_server, MapRequest as RPCMapRequest, MapResponse, ReadyResponse,
};
use crate::shared;

mod mapper {
    tonic::include_proto!("map.v1");
}

struct MapService<T> {
    handler: T,
}

/// Mapper trait for implementing Map handler.
#[async_trait]
pub trait Mapper {
    /// The `map` takes in an input element and can produce 0, 1, or more results.
    /// In a `map` function, each element is processed independently and there is no state associated with the elements.
    /// More about map can be read [here](https://numaflow.numaproj.io/user-guide/user-defined-functions/map/map/#map-udf).
    ///
    /// # Example
    ///
    /// Following is an example of a `cat` container that just copies the input to output.
    ///
    /// ```no_run
    /// use numaflow::map;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     map::Server::new(Cat).start().await?;
    ///     Ok(())
    /// }
    ///
    /// struct Cat;
    ///
    /// #[tonic::async_trait]
    /// impl map::Mapper for Cat {
    ///     async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
    ///         vec![map::Message {
    ///             keys: input.keys,
    ///             value: input.value,
    ///             tags: vec![],
    ///         }]
    ///     }
    /// }
    /// ```
    async fn map(&self, input: MapRequest) -> Vec<Message>;
}

#[async_trait]
impl<T> map_server::Map for MapService<T>
where
    T: Mapper + Send + Sync + 'static,
{
    async fn map_fn(
        &self,
        request: Request<RPCMapRequest>,
    ) -> Result<Response<MapResponse>, Status> {
        let request = request.into_inner();
        let result = self.handler.map(request.into()).await;

        Ok(Response::new(MapResponse {
            results: result.into_iter().map(|msg| msg.into()).collect(),
        }))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<ReadyResponse>, Status> {
        Ok(Response::new(ReadyResponse { ready: true }))
    }
}

/// Message is the response struct from the [`Mapper::map`] .
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection.
    pub keys: Vec<String>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags: Vec<String>,
}

impl From<Message> for map_response::Result {
    fn from(value: Message) -> Self {
        map_response::Result {
            keys: value.keys,
            value: value.value,
            tags: value.tags,
        }
    }
}

/// Incoming request into the map handles of [`Mapper`].
pub struct MapRequest {
    /// Set of keys in the (key, value) terminology of map/reduce paradigm.
    pub keys: Vec<String>,
    /// The value in the (key, value) terminology of map/reduce paradigm.
    pub value: Vec<u8>,
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this time.
    pub watermark: DateTime<Utc>,
    /// Time of the element as seen at source or aligned after a reduce operation.
    pub eventtime: DateTime<Utc>,
}

impl From<RPCMapRequest> for MapRequest {
    fn from(value: RPCMapRequest) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            watermark: shared::utc_from_timestamp(value.watermark),
            eventtime: shared::utc_from_timestamp(value.event_time),
        }
    }
}

/// gRPC server to start a map service
#[derive(Debug)]
pub struct Server<T> {
    sock_addr: PathBuf,
    max_message_size: usize,
    server_info_file: PathBuf,
    map_svc: Option<T>,
}

impl<T> Server<T> {
    pub fn new(map_svc: T) -> Self {
        let server_info_file = if std::env::var_os("NUMAFLOW_POD").is_some() {
            "/var/run/numaflow/server-info"
        } else {
            "/tmp/numaflow.server-info"
        };
        Server {
            sock_addr: "/var/run/numaflow/map.sock".into(),
            max_message_size: 64 * 1024 * 1024,
            server_info_file: server_info_file.into(),
            map_svc: Some(map_svc),
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections. Defaults value is `/var/run/numaflow/map.sock`
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.sock_addr = file.into();
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections. Default value is `/var/run/numaflow/map.sock`
    pub fn socket_file(&self) -> &std::path::Path {
        self.sock_addr.as_path()
    }

    /// Set the maximum size of an encoded and decoded gRPC message. The value of `message_size` is in bytes. Default value is 4MB.
    pub fn with_max_message_size(mut self, message_size: usize) -> Self {
        self.max_message_size = message_size;
        self
    }

    /// Get the maximum size of an encoded and decoded gRPC message in bytes. Default value is 4MB.
    pub fn max_message_size(&self) -> usize {
        self.max_message_size
    }

    /// Change the file in which numflow server information is stored on start up to the new value. Default value is `/tmp/numaflow.server-info`
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.server_info_file = file.into();
        self
    }

    /// Get the path to the file where numaflow server info is stored. Default value is `/tmp/numaflow.server-info`
    pub fn server_info_file(&self) -> &std::path::Path {
        self.server_info_file.as_path()
    }

    /// Starts the gRPC server. When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub async fn start_with_shutdown(
        &mut self,
        shutdown: oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Mapper + Send + Sync + 'static,
    {
        let listener = shared::create_listener_stream(&self.sock_addr, &self.server_info_file)?;
        let handler = self.map_svc.take().unwrap();
        let map_svc = MapService { handler };
        let map_svc = map_server::MapServer::new(map_svc)
            .max_encoding_message_size(self.max_message_size)
            .max_decoding_message_size(self.max_message_size);

        let shutdown = async {
            shutdown
                .await
                .expect("Receiving message from shutdown channel");
        };
        tonic::transport::Server::builder()
            .add_service(map_svc)
            .serve_with_incoming_shutdown(listener, shutdown)
            .await
            .map_err(Into::into)
    }

    /// Starts the gRPC server. Automatically registers singal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the singal arrives.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Mapper + Send + Sync + 'static,
    {
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(wait_for_signal(tx));
        self.start_with_shutdown(rx).await
    }
}

async fn wait_for_signal(tx: oneshot::Sender<()>) {
    use tokio::signal::unix::{signal, SignalKind};
    let mut interrupt =
        signal(SignalKind::interrupt()).expect("Failed to register SIGINT interrupt handler");
    let mut termination =
        signal(SignalKind::terminate()).expect("Failed to register SIGTERM interrupt handler");
    tokio::select! {
        _ = interrupt.recv() =>  {
            tracing::info!("Received SIGINT. Stopping gRPC server")
        }
        _ = termination.recv() => {
            tracing::info!("Received SIGTERM. Stopping gRPC server")
        }
    }
    tx.send(()).expect("Sending shutdown signal to gRPC server");
}

#[cfg(test)]
mod tests {
    use std::{error::Error, time::Duration};
    use tower::service_fn;

    use crate::map;
    use crate::map::mapper::map_client::MapClient;
    use tempfile::TempDir;
    use tokio::sync::oneshot;
    use tonic::transport::Uri;

    #[tokio::test]
    async fn map_server() -> Result<(), Box<dyn Error>> {
        struct Cat;
        #[tonic::async_trait]
        impl map::Mapper for Cat {
            async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
                vec![map::Message {
                    keys: input.keys,
                    value: input.value,
                    tags: vec![],
                }]
            }
        }

        let tmp_dir = TempDir::new()?;
        let sock_file = tmp_dir.path().join("map.sock");
        let server_info_file = tmp_dir.path().join("server_info");

        let mut server = map::Server::new(Cat)
            .with_server_info_file(&server_info_file)
            .with_socket_file(&sock_file)
            .with_max_message_size(10240);

        assert_eq!(server.max_message_size(), 10240);
        assert_eq!(server.server_info_file(), server_info_file);
        assert_eq!(server.socket_file(), sock_file);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move { server.start_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // https://github.com/hyperium/tonic/blob/master/examples/src/uds/client.rs
        let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(service_fn(move |_: Uri| {
                // Connect to a Uds socket
                let sock_file = sock_file.clone();
                tokio::net::UnixStream::connect(sock_file)
            }))
            .await?;

        let mut client = MapClient::new(channel);
        let request = tonic::Request::new(map::mapper::MapRequest {
            keys: vec!["first".into(), "second".into()],
            value: "hello".into(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
        });

        let resp = client.map_fn(request).await?;
        let resp = resp.into_inner();
        assert_eq!(resp.results.len(), 1, "Expected single message from server");
        let msg = &resp.results[0];
        assert_eq!(msg.keys.first(), Some(&"first".to_owned()));
        assert_eq!(msg.value, "hello".as_bytes());

        shutdown_tx
            .send(())
            .expect("Sending shutdown signal to gRPC server");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(task.is_finished(), "gRPC server is still running");
        Ok(())
    }
}
