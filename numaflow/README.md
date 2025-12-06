# Rust SDK for Numaflow

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Crates.io](https://img.shields.io/crates/v/numaflow.svg)](https://crates.io/crates/numaflow)
[![Documentation](https://img.shields.io/docsrs/numaflow/latest)](https://docs.rs/numaflow/)

Rust SDK for Numaflow which implements all the core Numaflow features:

- [Map UDF](https://numaflow.numaproj.io/user-guide/user-defined-functions/map/map/)
    - [Streaming mode](https://numaflow.numaproj.io/user-guide/user-defined-functions/map/map/#streaming-mode)
    - [BatchMap mode](https://numaflow.numaproj.io/user-guide/user-defined-functions/map/map/#batch-map-mode)
- [Reduce UDF](https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/reduce/)
    - [Accumulator](https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/accumulator/)
    - [Session](https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/session/)
    - [Fixed](https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/fixed/)
    - [Sliding](https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/sliding/)
- [User Defined (UD) Sink](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/)
- [User Defined (UD) Source](https://numaflow.numaproj.io/user-guide/sources/user-defined-sources/)
- [Source Transform](https://numaflow.numaproj.io/user-guide/sources/transformer/overview/)
- [Side Input](https://numaflow.numaproj.io/user-guide/reference/side-inputs/)

Rust SDK is a foundational SDK, and it powers a few other SDKs through FFI.

- [Lightweight Python SDK](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow-lite)
- [JavaScript/TypeScript SDK](https://github.com/numaproj/numaflow-js)

## Examples

You may find examples in the [examples folder](./examples).