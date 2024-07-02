FROM rust:1.75-bookworm AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

# create a new empty shell project
WORKDIR /numaflow-rs

# copy your source tree
COPY ./ ./

WORKDIR /numaflow-rs/examples/map-tickgen-serde

# build for release
RUN cargo build --release

# our final base
FROM debian:bookworm AS map-tickgen-serde

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/examples/map-tickgen-serde/target/release/server .

# set the startup command to run your binary
CMD ["./server"]