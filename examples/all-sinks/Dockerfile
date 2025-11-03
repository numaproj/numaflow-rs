FROM rust:1.85-bullseye AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/all-sinks

# build for release
RUN cargo build --release

# our final base
FROM debian:bullseye AS all-sinks

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/target/release/all-sinks .

# set the startup command to run your binary
CMD ["./all-sinks"]
