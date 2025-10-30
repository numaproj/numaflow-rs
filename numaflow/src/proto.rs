//! Generated protobuf modules
//!
//! This module contains all the generated protobuf code for the Numaflow SDK.
//! The protobuf files are generated in src/generated/ for easy access and version control.

/// Map service protobuf definitions
#[path = "generated/map.v1.rs"]
#[rustfmt::skip]
pub mod map;

/// Reduce service protobuf definitions
#[path = "generated/reduce.v1.rs"]
#[rustfmt::skip]
pub mod reduce;

/// Source service protobuf definitions
#[path = "generated/source.v1.rs"]
#[rustfmt::skip]
pub mod source;

/// Sink service protobuf definitions
#[path = "generated/sink.v1.rs"]
#[rustfmt::skip]
pub mod sink;

/// Side input service protobuf definitions
#[path = "generated/sideinput.v1.rs"]
#[rustfmt::skip]
pub mod side_input;

/// Source transformer service protobuf definitions
#[path = "generated/sourcetransformer.v1.rs"]
#[rustfmt::skip]
pub mod source_transformer;

/// Serving store service protobuf definitions
#[path = "generated/serving.v1.rs"]
#[rustfmt::skip]
pub mod serving_store;

/// Session reduce service protobuf definitions
#[path = "generated/sessionreduce.v1.rs"]
#[rustfmt::skip]
pub mod session_reduce;

/// Accumulator service protobuf definitions
#[path = "generated/accumulator.v1.rs"]
#[rustfmt::skip]
pub mod accumulator;

#[path = "common/metadata.rs"]
pub mod metadata;
