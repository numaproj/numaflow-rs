//! Server configuration and management utilities
//!
//! This module provides server configuration, socket management, and shutdown
//! handling functionality for the Numaflow SDK.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use tokio::net::UnixListener;
use tokio::signal;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::shared::types::ServerInfo;

/// Default maximum message size (64MB)
const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Common server configuration that all services share
#[derive(Debug, Clone)]
pub struct ServerConfig {
    sock_addr: PathBuf,
    max_message_size: usize,
    server_info_file: PathBuf,
}

impl ServerConfig {
    /// Create new server configuration with defaults for the given service
    pub fn new(default_sock_addr: &str, default_server_info_file: &str) -> Self {
        Self {
            sock_addr: default_sock_addr.into(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            server_info_file: default_server_info_file.into(),
        }
    }

    /// Set the unix domain socket file path used by the gRPC server to listen for incoming connections.
    pub fn with_socket_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.sock_addr = file.into();
        self
    }

    /// Get the unix domain socket file path where gRPC server listens for incoming connections.
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

    /// Set the file in which numaflow server information is stored
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.server_info_file = file.into();
        self
    }

    /// Get the path to the file where numaflow server info is stored. Default value is `/var/run/numaflow/serving-server-info`
    pub fn server_info_file(&self) -> &std::path::Path {
        self.server_info_file.as_path()
    }
}

/// It is used to clean up the socket file when the server is dropped.
#[derive(Debug)]
pub struct SocketCleanup {
    sock_addr: PathBuf,
    server_info_file: PathBuf,
}

impl SocketCleanup {
    pub fn new(sock_addr: PathBuf, server_info_file: PathBuf) -> Self {
        Self {
            sock_addr,
            server_info_file,
        }
    }
}

impl Drop for SocketCleanup {
    /// Cleanup the socket file when the server is dropped so that when the server is restarted, it can bind to the same address.
    /// UnixListener doesn't implement Drop trait, so we have to manually remove the socket file.
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.sock_addr);
        let _ = fs::remove_file(&self.server_info_file);
    }
}

/// Write server information to a file
#[tracing::instrument(fields(path = ? path.as_ref()))]
fn write_info_file(path: impl AsRef<Path>, server_info: ServerInfo) -> io::Result<()> {
    let parent = path.as_ref().parent().unwrap();
    fs::create_dir_all(parent)?;
    // Convert to a string of JSON and print it out
    let serialized = serde_json::to_string(&server_info)?;
    let content = format!("{}U+005C__END__", serialized);
    info!(content, "Writing to file");
    fs::write(path, content)
}

/// Create a Unix listener stream for the gRPC server
pub fn create_listener_stream(
    socket_file: impl AsRef<Path>,
    server_info_file: impl AsRef<Path>,
    server_info: ServerInfo,
) -> Result<UnixListenerStream, Box<dyn std::error::Error + Send + Sync>> {
    write_info_file(server_info_file, server_info)
        .map_err(|e| format!("writing info file: {e:?}"))?;

    let uds_stream = UnixListener::bind(socket_file)?;
    Ok(UnixListenerStream::new(uds_stream))
}

/// Shuts down the gRPC server. This happens in 2 cases:
///     1. there has been an internal error (one of the tasks failed) and we need to shut down
///     2. user is explicitly asking us to shut down
/// Once the request for shutdown has been invoked, server will broadcast shutdown to all tasks
/// through the cancellation-token.
pub async fn shutdown_signal(
    mut shutdown_on_err: mpsc::Receiver<()>,
    shutdown_from_user: Option<oneshot::Receiver<()>>,
    cln_token: CancellationToken,
) {
    // will call cancel_token.cancel() on drop of guard
    let _drop_guard = cln_token.drop_guard();

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install SIGINT handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    let shutdown_on_err_future = async {
        shutdown_on_err.recv().await;
    };

    let shutdown_from_user_future = async {
        if let Some(rx) = shutdown_from_user {
            rx.await.ok();
        }
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
        _ = shutdown_on_err_future => {},
        _ = shutdown_from_user_future => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::types::ContainerType;
    use std::fs::File;
    use std::io::Read;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_write_info_file() -> io::Result<()> {
        // Create a temporary file
        let temp_file = NamedTempFile::new()?;

        // Create a new ServerInfo object with ContainerType::BatchMap
        let info = ServerInfo::new(ContainerType::BatchMap);

        // Call write_info_file with the path of the temporary file
        write_info_file(temp_file.path(), info)?;

        // Open the file and read its contents
        let mut file = File::open(temp_file.path())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        // Check if the contents of the file are as expected
        assert!(contents.contains(r#""protocol":"uds""#));
        assert!(contents.contains(r#""language":"rust""#));
        assert!(contents.contains(r#""minimum_numaflow_version":"1.4.0-z""#));
        assert!(contents.contains(r#""metadata":{"MAP_MODE":"batch-map"}"#));

        Ok(())
    }

    #[tokio::test]
    async fn test_shutdown_signal() {
        // Create a channel to send shutdown signal
        let (internal_shutdown_tx, internal_shutdown_rx) = mpsc::channel(1);
        let (_user_shutdown_tx, user_shutdown_rx) = oneshot::channel();

        // Spawn a new task to call shutdown_signal
        let shutdown_signal_task = tokio::spawn(async move {
            shutdown_signal(
                internal_shutdown_rx,
                Some(user_shutdown_rx),
                CancellationToken::new(),
            )
            .await;
        });

        // Send a shutdown signal
        internal_shutdown_tx.send(()).await.unwrap();

        // Wait for the shutdown_signal function to finish
        let result = shutdown_signal_task.await;

        // If we reach this point, it means that the shutdown_signal function has correctly handled the shutdown signal
        assert!(result.is_ok());
    }

    #[test]
    fn test_server_config() {
        let config = ServerConfig::new("/tmp/test.sock", "/tmp/server-info");

        assert_eq!(config.socket_file(), Path::new("/tmp/test.sock"));
        assert_eq!(config.server_info_file(), Path::new("/tmp/server-info"));
        assert_eq!(config.max_message_size(), DEFAULT_MAX_MESSAGE_SIZE);

        let config = config
            .with_socket_file("/tmp/other.sock")
            .with_max_message_size(1024)
            .with_server_info_file("/tmp/other-info");

        assert_eq!(config.socket_file(), Path::new("/tmp/other.sock"));
        assert_eq!(config.server_info_file(), Path::new("/tmp/other-info"));
        assert_eq!(config.max_message_size(), 1024);
    }
}
