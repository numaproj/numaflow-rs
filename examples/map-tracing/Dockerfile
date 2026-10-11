# Newer Rust than the SDK's `rust-version = "1.85.0"` baseline because the
# OTel crate chain (idna_adapter, icu_*, latest tonic) pulls in dependencies
# whose MSRV is 1.86-1.88. The SDK itself still builds on 1.85; only this
# example needs the newer toolchain.
FROM rust:1.88-bullseye AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/map-tracing

# build for release
RUN cargo build --release

# our final base
FROM debian:bullseye AS map-tracing

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/target/release/map-tracing .

# set the startup command to run your binary
CMD ["./map-tracing"]
