FROM rust:1.99-bookworm AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/flatmap-stream

# build for release
RUN cargo build --release

# our final base
FROM debian:bookworm AS flatmap-stream

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/target/release/flatmap-stream .

# set the startup command to run your binary
CMD ["./flatmap-stream"]
