# Rust SDK for Numaflow

This SDK provides the interface for writing [User Defined Sources](https://numaflow.numaproj.io/user-guide/sources/overview/), [UDFs](https://numaflow.numaproj.io/user-guide/user-defined-functions/user-defined-functions/) 
and [User Defined Sinks](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/) in [Rust](https://www.rust-lang.org/).

> This rust crate is being actively developed and it supports
> most of the features. You may use this crate but there might be
> few minor changes in the upcoming releases.


## Cargo Dependency
Until we publish the crate, you will have to provide the
git location.

```toml
numaflow = { git = "https://github.com/numaproj/numaflow-rs.git", branch="main" }
```

## Examples

You may find examples in the [examples folder](./examples).

## Documentation

Please run the following to get the `numaflow` crate documentation.
This work around will be in effect until we finalize the SDK contract
and officially publish the crate.

```bash
$ cargo doc -p numaflow --open
```
