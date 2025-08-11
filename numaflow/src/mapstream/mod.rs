//! Map stream functionality for the Numaflow SDK
//!
//! This module provides the implementation for streaming map operations in Numaflow pipelines.
//! In streaming mode, the messages will be pushed to the downstream vertices once generated
//! instead of in a batch at the end.

mod mapstream;

pub use mapstream::*;
