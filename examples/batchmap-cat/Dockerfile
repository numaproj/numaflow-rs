FROM rust:1.75-bookworm AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/batchmap-cat

# build for release
RUN cargo build --release

# our final base
FROM debian:bookworm AS map-cat

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/examples/batchmap-cat/target/release/server .

# set the startup command to run your binary
CMD ["./server"]
