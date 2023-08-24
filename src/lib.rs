//! A Rust SDK for [Numaflow]. The Rust SDK is experimental has only implemented the most important
//! features. It will support all the core features eventually. It supports [Map], [Reduce], and
//! [User Defined Sinks].
//!
//! Please note that the Rust SDK is experimental and will be refactor in the future to make it more
//! idiomatic.
//!
//! [Numaflow]: https://numaflow.numaproj.io/
//! [Map]: https://numaflow.numaproj.io/user-guide/user-defined-functions/map/map/
//! [Reduce]: https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/reduce/
//! [User Defined Sinks]: https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/

/// start up code
mod startup;

pub mod map;

/// map and reduce for writing [map and reduce](https://numaflow.numaproj.io/user-guide/user-defined-functions/user-defined-functions/) handlers.
pub mod function;

/// sink for writing [user defined sinks](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/).
pub mod sink;
