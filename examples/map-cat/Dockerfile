# use a minimal rust base image
FROM rust:1.75-bookworm as build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

# create a new empty shell project
WORKDIR /numaflow-rs

# Copy the whole application code into the Docker image
COPY ./ ./

WORKDIR /numaflow-rs/examples/map-cat

# build for release
RUN cargo build --release

# our final base
FROM debian:bookworm

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/examples/map-cat/target/release/server .

# set the startup command to run your binary
CMD ["./server"]
