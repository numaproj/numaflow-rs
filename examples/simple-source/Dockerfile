FROM rust:1.80-bookworm AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/simple-source

# build for release
RUN cargo build --release

# our final base
FROM debian:bookworm AS simple-source

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/examples/simple-source/target/release/server .

# set the startup command to run your binary
CMD ["./server"]
