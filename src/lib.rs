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
//! [User Defined Sources]: https://numaflow.numaproj.io/user-guide/sources/user-defined-sources/
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

/// building [side input](https://numaflow.numaproj.io/user-guide/reference/side-inputs/)
pub mod sideinput;

// Error handling on Numaflow SDKs!
//
// Any non-recoverable error will cause the process to shutdown with a non-zero exit status. All errors are non-recoverable.
// If there are errors that are retriable, we (gRPC or Numaflow SDK) would have already retried it (hence not an error), that means,
// all errors raised by the SDK are non-recoverable.
//
// Task Ordering and error propagation.
//
//      level-1               level-2               level-3
//
//                   +---> (service_fn) ->
//                   |
//                   |
//                   |                     +---> (task)
//                   |                     |
//                   |                     |
// (gRPC Service) ---+---> (service_fn) ---+---> (task)
//      ^            |                     |
//      |            |                     |
//      |            |                     +---> (task)
//      |            |
//  (shutdown)       |
//      |            +---> (service_fn) ->
//      |
//      |
//   (user)
//
// If a task at level-3 has an error, then that error will be propagated to level-2 (service_fn) via an mpsc::channel using the response channel.
// The Response channel passes a Result type and by returning Err() in response channel, it notifies top service_fn that the task wants to abort itself.
// service_fn (level-2) will now use another mpsc::channel to tell the gRPC service to cancel all the service_fns. gRPC service will
// will ask all the level-2 service_fns to abort using the CancellationToken. service_fn will call abort on all the tasks it created using internal
// mpsc::channel when CancellationToken has been dropped/cancelled.
//
// User can directly send shutdown request to the gRPC server which inturn cancels the CancellationToken.
//
// The above 3 level task ordering is only for complex cases like reduce, but for simpler endpoints like `map`, it only has 2 levels but
// the error propagation is handled the same way.

/// error module
pub mod error;
