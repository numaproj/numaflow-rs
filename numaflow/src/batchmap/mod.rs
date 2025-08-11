//! Batch map functionality for the Numaflow SDK
//!
//! This module provides the implementation for batch map operations in Numaflow pipelines.
//! Batch map functions allows developers to process multiple data items in a UDF single call,
//! rather than each item in separate calls.

mod batchmap;

pub use batchmap::*;
