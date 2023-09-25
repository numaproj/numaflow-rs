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
mod shared;

/// source is for building custom [user defined sources](https://numaflow.numaproj.io/user-guide/sources/overview/).
pub mod source;

/// sourcetransform for writing [source data transformers](https://numaflow.numaproj.io/user-guide/sources/transformer/overview/).
pub mod sourcetransform;

/// map is for writing the [map](https://numaflow.numaproj.io/user-guide/user-defined-functions/map/map/) handlers.
pub mod map;

/// reduce is for writing the [reduce](https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/reduce/) handlers.
pub mod reduce;

/// sink for writing [user defined sinks](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/).
pub mod sink;
