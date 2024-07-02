FROM rust:1.75-bookworm AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/source-transformer-event-time-filter

# build for release
RUN cargo build --release

# our final base
FROM debian:bookworm AS source-transformer-event-time-filter

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/examples/source-transformer-event-time-filter/target/release/server .

# set the startup command to run your binary
CMD ["./server"]
