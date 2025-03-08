FROM rust:1.84-bullseye AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/sink-log

# build for release
RUN cargo build --release

# our final base
FROM debian:bullseye AS sink-log

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/target/release/sink-log .

# set the startup command to run your binary
CMD ["./sink-log"]
