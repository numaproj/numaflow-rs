FROM rust:1.70 AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
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