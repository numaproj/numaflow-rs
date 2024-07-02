FROM rust:1.70 as build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

# create a new empty shell project
WORKDIR /numaflow-rs

# Copy the whole application code into the Docker image
COPY ./ ./

WORKDIR /numaflow-rs/examples/sideinput

# build for release
RUN cargo build --release

# our final base
FROM rust AS sideinput

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/examples/sideinput/target/release/server .

# set the startup command to run your binary
CMD ["./server"]