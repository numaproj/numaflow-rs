//! Generic gRPC server implementation
//!
//! This module provides a generic Server struct that can be used by all Numaflow services,
//! eliminating code duplication across different service implementations.

use std::path::PathBuf;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::transport::server::Router;

use super::{
    ContainerType, ServerConfig, ServerInfo, SocketCleanup, create_listener_stream,
    init_panic_hook, shutdown_signal,
};

/// Trait providing default implementations for common server configuration methods.
/// This trait works with the builder pattern and eliminates boilerplate delegation code.
#[allow(dead_code)]
pub(crate) trait ServerExtras<T> {
    /// Extract the inner server, transform it, and wrap it back
    fn transform_inner<F>(self, f: F) -> Self
    where
        F: FnOnce(Server<T>) -> Server<T>,
        Self: Sized;

    /// Get a reference to the inner server
    fn inner_ref(&self) -> &Server<T>;

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    fn with_socket_file(self, file: impl Into<PathBuf>) -> Self
    where
        Self: Sized,
    {
        self.transform_inner(|inner| inner.with_socket_file(file))
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections.
    fn socket_file<'a>(&'a self) -> &'a std::path::Path
    where
        T: 'a,
    {
        self.inner_ref().socket_file()
    }

    /// Set the maximum size of an encoded and decoded gRPC message. The value of `message_size` is in bytes.
    fn with_max_message_size(self, message_size: usize) -> Self
    where
        Self: Sized,
    {
        self.transform_inner(|inner| inner.with_max_message_size(message_size))
    }

    /// Get the maximum size of an encoded and decoded gRPC message in bytes.
    fn max_message_size(&self) -> usize {
        self.inner_ref().max_message_size()
    }

    /// Change the file in which numaflow server information is stored on start up to the new value.
    fn with_server_info_file(self, file: impl Into<PathBuf>) -> Self
    where
        Self: Sized,
    {
        self.transform_inner(|inner| inner.with_server_info_file(file))
    }

    /// Get the path to the file where numaflow server info is stored.
    fn server_info_file<'a>(&'a self) -> &'a std::path::Path
    where
        T: 'a,
    {
        self.inner_ref().server_info_file()
    }
}

/// Common server startup configuration and utilities
#[derive(Debug)]
pub(crate) struct ServerStarter {
    config: ServerConfig,
    container_type: ContainerType,
    _cleanup: SocketCleanup,
    init_panic_hook: bool,
}

#[allow(dead_code)]
impl ServerStarter {
    /// Create a new server starter with the given container type and defaults
    pub(crate) fn new(
        container_type: ContainerType,
        default_sock_addr: &str,
        default_server_info_file: &str,
    ) -> Self {
        let config = ServerConfig::new(default_sock_addr, default_server_info_file);
        let cleanup = SocketCleanup::new(default_sock_addr.into(), default_server_info_file.into());

        Self {
            config,
            container_type,
            _cleanup: cleanup,
            init_panic_hook: true,
        }
    }

    /// Set whether to initialize panic hook (default: true)
    pub(crate) fn with_panic_hook(mut self, init_panic_hook: bool) -> Self {
        self.init_panic_hook = init_panic_hook;
        self
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    pub(crate) fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        let file_path = file.into();
        self.config = self.config.with_socket_file(&file_path);
        self._cleanup = SocketCleanup::new(file_path, self.config.server_info_file().to_path_buf());
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections.
    pub(crate) fn socket_file(&self) -> &std::path::Path {
        self.config.socket_file()
    }

    /// Set the maximum size of an encoded and decoded gRPC message. The value of `message_size` is in bytes. Default value is 64MB.
    pub(crate) fn with_max_message_size(mut self, message_size: usize) -> Self {
        self.config = self.config.with_max_message_size(message_size);
        self
    }

    /// Get the maximum size of an encoded and decoded gRPC message in bytes. Default value is 64MB.
    pub(crate) fn max_message_size(&self) -> usize {
        self.config.max_message_size()
    }

    /// Change the file in which numaflow server information is stored on start up to the new value.
    pub(crate) fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        let file_path = file.into();
        self.config = self.config.with_server_info_file(&file_path);
        self._cleanup = SocketCleanup::new(self.config.socket_file().to_path_buf(), file_path);
        self
    }

    /// Get the path to the file where numaflow server info is stored.
    pub(crate) fn server_info_file(&self) -> &std::path::Path {
        self.config.server_info_file()
    }

    /// Common server startup logic that can be used by all services
    pub(crate) async fn start_server<F>(
        self,
        shutdown_rx: Option<oneshot::Receiver<()>>,
        service_builder: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce(mpsc::Sender<()>, CancellationToken) -> Router,
    {
        // Initialize panic hook if requested
        if self.init_panic_hook {
            init_panic_hook();
        }

        let info = ServerInfo::new(self.container_type);
        let listener = create_listener_stream(
            self.config.socket_file(),
            self.config.server_info_file(),
            info,
        )?;

        let (internal_shutdown_tx, internal_shutdown_rx) = mpsc::channel(1);
        let cln_token = CancellationToken::new();

        // Build the service using the provided builder function
        let router = service_builder(internal_shutdown_tx, cln_token.clone());

        let shutdown = shutdown_signal(internal_shutdown_rx, shutdown_rx, cln_token);

        router
            .serve_with_incoming_shutdown(listener, shutdown)
            .await?;

        Ok(())
    }
}

/// Generic gRPC server that can handle any service type
/// This eliminates the need for duplicate Server implementations across all service files
#[derive(Debug)]
pub(crate) struct Server<T> {
    starter: ServerStarter,
    svc: T,
}

#[allow(dead_code)]
impl<T> Server<T> {
    /// Create a new server with the given service and container configuration
    pub(crate) fn new(
        svc: T,
        container_type: ContainerType,
        default_sock_addr: &str,
        default_server_info_file: &str,
    ) -> Self {
        let starter =
            ServerStarter::new(container_type, default_sock_addr, default_server_info_file);

        Self { starter, svc }
    }

    /// Create a new server with custom socket paths (for sink fallback support)
    pub(crate) fn new_with_custom_paths(
        svc: T,
        container_type: ContainerType,
        sock_addr: &str,
        server_info_file: &str,
    ) -> Self {
        let starter = ServerStarter::new(container_type, sock_addr, server_info_file);

        Self { starter, svc }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections
    pub(crate) fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.starter = self.starter.with_socket_file(file);
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections
    pub(crate) fn socket_file(&self) -> &std::path::Path {
        self.starter.socket_file()
    }

    /// Set the maximum size of an encoded and decoded gRPC message. The value of `message_size` is in bytes. Default value is 64MB.
    pub(crate) fn with_max_message_size(mut self, message_size: usize) -> Self {
        self.starter = self.starter.with_max_message_size(message_size);
        self
    }

    /// Get the maximum size of an encoded and decoded gRPC message in bytes. Default value is 64MB.
    pub(crate) fn max_message_size(&self) -> usize {
        self.starter.max_message_size()
    }

    /// Change the file in which numaflow server information is stored on start up to the new value
    pub(crate) fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.starter = self.starter.with_server_info_file(file);
        self
    }

    /// Get the path to the file where numaflow server info is stored
    pub(crate) fn server_info_file(&self) -> &std::path::Path {
        self.starter.server_info_file()
    }

    /// Starts the gRPC server with a custom service builder function.
    /// When message is received on the `shutdown` channel, graceful shutdown of the gRPC server will be initiated.
    pub(crate) async fn start_with_shutdown<F>(
        self,
        shutdown_rx: oneshot::Receiver<()>,
        service_builder: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce(T, usize, mpsc::Sender<()>, CancellationToken) -> Router + Send + 'static,
        T: Send + Sync + 'static,
    {
        let handler = self.svc;
        let max_message_size = self.starter.max_message_size();

        self.starter
            .start_server(Some(shutdown_rx), |shutdown_tx, cln_token| {
                service_builder(handler, max_message_size, shutdown_tx, cln_token)
            })
            .await
    }

    /// Starts the gRPC server with a custom service builder function.
    /// Automatically registers signal handlers for SIGINT and SIGTERM and initiates graceful shutdown of gRPC server when either one of the signal arrives.
    pub(crate) async fn start<F>(
        self,
        service_builder: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce(T, usize, mpsc::Sender<()>, CancellationToken) -> Router + Send + 'static,
        T: Send + Sync + 'static,
    {
        let handler = self.svc;
        let max_message_size = self.starter.max_message_size();

        self.starter
            .start_server(None, |shutdown_tx, cln_token| {
                service_builder(handler, max_message_size, shutdown_tx, cln_token)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_server_starter_creation() {
        let starter = ServerStarter::new(ContainerType::Map, "/tmp/test.sock", "/tmp/test-info");

        assert_eq!(
            starter.socket_file(),
            std::path::Path::new("/tmp/test.sock")
        );
        assert_eq!(
            starter.server_info_file(),
            std::path::Path::new("/tmp/test-info")
        );
        assert_eq!(starter.max_message_size(), 64 * 1024 * 1024); // 64MB default
    }

    #[test]
    fn test_server_starter_configuration() {
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("custom.sock");
        let info_file = tmp_dir.path().join("custom-info");

        let starter = ServerStarter::new(ContainerType::Map, "/tmp/test.sock", "/tmp/test-info")
            .with_socket_file(&sock_file)
            .with_server_info_file(&info_file)
            .with_max_message_size(1024)
            .with_panic_hook(false);

        assert_eq!(starter.socket_file(), sock_file);
        assert_eq!(starter.server_info_file(), info_file);
        assert_eq!(starter.max_message_size(), 1024);
        assert!(!starter.init_panic_hook);
    }

    #[test]
    fn test_create_server_config() {
        let starter = ServerStarter::new(
            ContainerType::Reduce,
            "/var/run/numaflow/reduce.sock",
            "/var/run/numaflow/reducer-server-info",
        );

        assert_eq!(
            starter.socket_file(),
            std::path::Path::new("/var/run/numaflow/reduce.sock")
        );
        assert_eq!(
            starter.server_info_file(),
            std::path::Path::new("/var/run/numaflow/reducer-server-info")
        );
    }
}
