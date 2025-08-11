//! Reduce functionality for the Numaflow SDK
//!
//! This module provides the implementation for reduce operations in Numaflow pipelines.
//! Reduce functions aggregate multiple messages into a single result.
//! You can read more about reduce here https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/reduce/.

mod reduce;

pub use reduce::*;
