use thiserror::Error;
use tonic::Status;

/// The main Result type used throughout the Numaflow SDK
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum ErrorKind {
    /// User-defined function errors
    #[error("User Defined error: {0}")]
    UserDefinedError(String),

    /// Internal SDK errors
    #[error("Internal error: {0}")]
    InternalError(String),
}

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Map - {0}")]
    MapError(ErrorKind),

    #[error("Reduce - {0}")]
    ReduceError(ErrorKind),

    #[error("Sink - {0}")]
    SinkError(ErrorKind),

    #[error("Source - {0}")]
    SourceError(ErrorKind),

    #[error("BatchMap - {0}")]
    BatchMapError(ErrorKind),

    #[error("Source Transformer - {0}")]
    SourceTransformerError(ErrorKind),

    #[error("SideInput - {0}")]
    SideInputError(ErrorKind),

    #[error("ServingStore - {0}")]
    ServingStoreError(ErrorKind),

    #[error("Session Reduce - {0}")]
    SessionReduceError(ErrorKind),

    #[error("Accumulator - {0}")]
    AccumulatorError(ErrorKind),

    #[error("MapStream - {0}")]
    MapStreamError(ErrorKind),

    #[error("Numaflow - {0}")]
    DefaultError(ErrorKind),

    #[error("gRPC Status - {0}")]
    GrpcStatus(Status),
}

impl Error {
    /// Extract the gRPC Status if this error is a GrpcStatus variant,
    /// otherwise create a Status::internal with the error message
    pub fn into_status(self) -> Status {
        match self {
            Error::GrpcStatus(status) => status,
            other => Status::internal(other.to_string()),
        }
    }
}
