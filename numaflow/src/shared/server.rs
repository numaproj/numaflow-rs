//! Common server configuration and builder patterns
//!
//! This module provides shared server configuration structures and builder patterns
//! that eliminate code duplication across all Numaflow service implementations.

use std::fs;
use std::path::PathBuf;

use crate::shared::DEFAULT_MAX_MESSAGE_SIZE;

/// Common server configuration that all services share
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub(crate) sock_addr: PathBuf,
    pub(crate) max_message_size: usize,
    pub(crate) server_info_file: PathBuf,
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

    /// Change the file in which numaflow server information is stored on start up to the new value.
    pub fn with_server_info_file(mut self, file: impl Into<PathBuf>) -> Self {
        self.server_info_file = file.into();
        self
    }

    /// Get the path to the file where numaflow server info is stored.
    pub fn server_info_file(&self) -> &std::path::Path {
        self.server_info_file.as_path()
    }
}

/// Common Drop implementation for cleaning up socket files
#[derive(Debug)]
pub struct SocketCleanup {
    sock_addr: PathBuf,
}

impl SocketCleanup {
    pub fn new(sock_addr: PathBuf) -> Self {
        Self { sock_addr }
    }
}

impl Drop for SocketCleanup {
    /// Cleanup the socket file when the server is dropped so that when the server is restarted, it can bind to the
    /// same address. UnixListener doesn't implement Drop trait, so we have to manually remove the socket file.
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.sock_addr);
    }
}
