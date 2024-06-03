use std::fmt;

use tonic::Status;

#[derive(Clone)]
// ErrorKind is an enum that represents the kind of error that can occur.
pub enum ErrorKind {
    // UserDefinedError represents an error that is caused by the user function.
    UserDefinedError(String),
    // InternalError represents an error that is caused by the internal logic of the SDK.
    InternalError(String),
}

#[derive(Clone)]
// Error is an enum that represents the error that can occur in the SDK.
pub enum Error {
    // MapError represents an error that can occur in the Map function.
    MapError(ErrorKind),
    // ReduceError represents an error that can occur in the Reduce function.
    ReduceError(ErrorKind),
    // SinkError represents an error that can occur in the Sink function.
    SinkError(ErrorKind),
    // SourceError represents an error that can occur in the Source function.
    SourceError(ErrorKind),
}

impl Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::MapError(ErrorKind::UserDefinedError(msg)) => {
                write!(f, "Map Error - User Defined Error: {}", msg)
            }
            Error::MapError(ErrorKind::InternalError(msg)) => {
                write!(f, "Map Error - Internal Error: {}", msg)
            }
            Error::ReduceError(ErrorKind::UserDefinedError(msg)) => {
                write!(f, "Reduce Error - User Defined Error: {}", msg)
            }
            Error::ReduceError(ErrorKind::InternalError(msg)) => {
                write!(f, "Reduce Error - Internal Error: {}", msg)
            }
            Error::SinkError(ErrorKind::UserDefinedError(msg)) => {
                write!(f, "Sink Error - User Defined Error: {}", msg)
            }
            Error::SinkError(ErrorKind::InternalError(msg)) => {
                write!(f, "Sink Error - Internal Error: {}", msg)
            }
            Error::SourceError(ErrorKind::UserDefinedError(msg)) => {
                write!(f, "Source Error - User Defined Error: {}", msg)
            }
            Error::SourceError(ErrorKind::InternalError(msg)) => {
                write!(f, "Source Error - Internal Error: {}", msg)
            }
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt(f)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt(f)
    }
}

// Implementing From for tonic::Status to convert Error to tonic::Status
impl From<Error> for Status {
    fn from(error: Error) -> Self {
        match error {
            Error::MapError(ErrorKind::UserDefinedError(msg)) => Status::new(
                tonic::Code::Unknown,
                format!("MapError - UserDefinedError: {}", msg),
            ),
            Error::MapError(ErrorKind::InternalError(msg)) => Status::new(
                tonic::Code::Internal,
                format!("MapError - InternalError: {}", msg),
            ),
            Error::ReduceError(ErrorKind::UserDefinedError(msg)) => Status::new(
                tonic::Code::Unknown,
                format!("ReduceError - UserDefinedError: {}", msg),
            ),
            Error::ReduceError(ErrorKind::InternalError(msg)) => Status::new(
                tonic::Code::Internal,
                format!("ReduceError - InternalError: {}", msg),
            ),
            Error::SinkError(ErrorKind::UserDefinedError(msg)) => Status::new(
                tonic::Code::Unknown,
                format!("SinkError - UserDefinedError: {}", msg),
            ),
            Error::SinkError(ErrorKind::InternalError(msg)) => Status::new(
                tonic::Code::Internal,
                format!("SinkError - InternalError: {}", msg),
            ),
            Error::SourceError(ErrorKind::UserDefinedError(msg)) => Status::new(
                tonic::Code::Unknown,
                format!("SourceError - UserDefinedError: {}", msg),
            ),
            Error::SourceError(ErrorKind::InternalError(msg)) => Status::new(
                tonic::Code::Internal,
                format!("SourceError - InternalError: {}", msg),
            ),
        }
    }
}

impl std::error::Error for Error {}
