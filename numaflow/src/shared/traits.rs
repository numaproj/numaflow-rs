//!Shared service traits for consistent patterns across all Numaflow services
//! Currently only ServiceError trait is implemented
//!

use crate::error::{Error, ErrorKind};
use tonic::Status;

/// Trait for consistent error construction across all Numaflow services
///
/// This trait provides a unified interface for creating service-specific errors.
/// This is an internal trait used only by SDK service implementations.
/// Add more methods here if needed. eg: config_error, network_error, etc.
pub(crate) trait ServiceError {
    /// Get the service name for error context
    fn service_name() -> &'static str;

    /// Create a user-defined function error
    fn user_error(message: impl Into<String>) -> Error {
        let kind = ErrorKind::UserDefinedError(message.into());
        match Self::service_name() {
            "map" => Error::MapError(kind),
            "reduce" => Error::ReduceError(kind),
            "sink" => Error::SinkError(kind),
            "source" => Error::SourceError(kind),
            "batchmap" => Error::BatchMapError(kind),
            "sourcetransformer" => Error::SourceTransformerError(kind),
            "sideinput" => Error::SideInputError(kind),
            "servingstore" => Error::ServingStoreError(kind),
            "mapstream" => Error::MapStreamError(kind),
            _ => Error::DefaultError(kind),
        }
    }

    /// Create an internal service error
    fn internal_error(message: impl Into<String>) -> Error {
        let kind = ErrorKind::InternalError(message.into());
        match Self::service_name() {
            "map" => Error::MapError(kind),
            "reduce" => Error::ReduceError(kind),
            "sink" => Error::SinkError(kind),
            "source" => Error::SourceError(kind),
            "batchmap" => Error::BatchMapError(kind),
            "sourcetransform" => Error::SourceTransformerError(kind),
            "sideinput" => Error::SideInputError(kind),
            "servingstore" => Error::ServingStoreError(kind),
            "mapstream" => Error::MapStreamError(kind),
            _ => Error::DefaultError(kind),
        }
    }

    /// Create a  gRPC internal error
    fn grpc_internal_error(message: impl Into<String>) -> Status {
        Status::internal(message.into())
    }
    /// Create a  gRPC invalid_argument error
    fn grpc_invalid_argument_error(message: impl Into<String>) -> Status {
        Status::invalid_argument(message.into())
    }
    /// Create a  gRPC cancelled error
    fn grpc_cancelled_error(message: impl Into<String>) -> Status {
        Status::cancelled(message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock service for testing
    struct MockService;

    impl ServiceError for MockService {
        fn service_name() -> &'static str {
            "mock"
        }
    }

    #[test]
    fn test_service_error_construction() {
        let user_err = MockService::user_error("function failed");
        assert!(user_err.to_string().contains("User Defined error"));
        assert!(user_err.to_string().contains("function failed"));

        let internal_err = MockService::internal_error("connection lost");
        assert!(internal_err.to_string().contains("Internal error"));
        assert!(internal_err.to_string().contains("connection lost"));
    }
}
