use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum ErrorKind {
    #[error("User Defined Error: {0}")]
    UserDefinedError(String),

    #[error("Internal Error: {0}")]
    InternalError(String),
}

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

    #[error("Session Reduce Error - {0}")]
    SessionReduceError(ErrorKind),

    #[error("Accumulator Error - {0}")]
    AccumulatorError(ErrorKind),
}
