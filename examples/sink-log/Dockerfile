FROM rust:1.70 as build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

# create a new empty shell project
WORKDIR /numaflow-rs

# Copy the whole application code into the Docker image
COPY ./ ./

WORKDIR /numaflow-rs/examples/sink-log

# build for release
RUN cargo build --release

# our final base
FROM rust

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/examples/sink-log/target/release/server .

# set the startup command to run your binary
CMD ["./server"]