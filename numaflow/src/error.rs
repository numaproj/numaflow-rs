use thiserror::Error;

use crate::shared::ServiceKind;

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

    #[error("MapStream - {0}")]
    MapStreamError(ErrorKind),

    #[error("Numaflow - {0}")]
    DefaultError(ErrorKind),
}

/// Wraps the error kind from service in the appropriate error enum
pub fn service_error(kind: ServiceKind, ek: ErrorKind) -> Error {
    match kind {
        ServiceKind::Map => Error::MapError(ek),
        ServiceKind::Reduce => Error::ReduceError(ek),
        ServiceKind::Sink => Error::SinkError(ek),
        ServiceKind::Source => Error::SourceError(ek),
        ServiceKind::BatchMap => Error::BatchMapError(ek),
        ServiceKind::SourceTransformer => Error::SourceTransformerError(ek),
        ServiceKind::SideInput => Error::SideInputError(ek),
        ServiceKind::ServingStore => Error::ServingStoreError(ek),
        ServiceKind::MapStream => Error::MapStreamError(ek),
        ServiceKind::Numaflow => Error::DefaultError(ek),
    }
}
