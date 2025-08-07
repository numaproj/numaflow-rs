//! Sink functionality for the Numaflow SDK
//!
//! This module provides the implementation for custom user-defined sinks in Numaflow pipelines.
//! Sinks receive data from the pipeline and output it to external systems.

mod sink;

pub use sink::*;
