//! Map functionality for the Numaflow SDK
//!
//! This module provides the implementation for map operations in Numaflow pipelines.
//! Map in a Map vertex takes an input and returns 0, 1, or more outputs (also known as flat-map operation).
//! Map is an element wise operator.

mod map;

pub use map::*;
