FROM rust:1.85-bullseye AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/reduce-stream-counter

# build for release
RUN cargo build --release

# our final base
FROM debian:bullseye AS reduce-stream-counter

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/target/release/reduce-stream-counter .

# set the startup command to run your binary
CMD ["./reduce-stream-counter"]

