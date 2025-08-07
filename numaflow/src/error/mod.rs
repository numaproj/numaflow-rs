//! Error types and handling for the Numaflow SDK
//!
//! This module provides structured error handling across the SDK with proper
//! error chaining, context, and categorization.

use thiserror::Error;

/// The main Result type used throughout the Numaflow SDK
pub type Result<T> = std::result::Result<T, Error>;

/// Enhanced ErrorKind with more specific error categories
#[derive(Error, Debug, Clone)]
pub enum ErrorKind {
    /// User-defined function errors
    #[error("User Defined error: {0}")]
    UserDefinedError(String),

    /// Internal SDK errors
    #[error("Internal error: {0}")]
    InternalError(String),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Network and connection errors
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Protocol and serialization errors
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    /// Validation errors
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// I/O errors
    #[error("I/O error: {0}")]
    IoError(String),

    /// Timeout errors
    #[error("Timeout error: {0}")]
    TimeoutError(String),
}

/// Enhanced Error enum with backward compatibility and new features
#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Map Error - {0}")]
    MapError(ErrorKind),

    #[error("Reduce Error - {0}")]
    ReduceError(ErrorKind),

    #[error("Sink Error - {0}")]
    SinkError(ErrorKind),

    #[error("Source Error - {0}")]
    SourceError(ErrorKind),

    #[error("BatchMap Error - {0}")]
    BatchMapError(ErrorKind),

    #[error("Source Transformer Error: {0}")]
    SourceTransformerError(ErrorKind),

    #[error("SideInput Error: {0}")]
    SideInputError(ErrorKind),

    #[error("ServingStore Error: {0}")]
    ServingStoreError(ErrorKind),

    #[error("MapStream Error: {0}")]
    MapStreamError(ErrorKind),

    #[error("Numaflow Error: {0}")]
    DefaultError(ErrorKind),
}
