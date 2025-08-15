FROM rust:1.85-bullseye AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/mapt-event-time-filter

# build for release
RUN cargo build --release

# our final base
FROM debian:bullseye AS mapt-event-time-filter

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/target/release/source-transformer-event-time-filter .

# set the startup command to run your binary
CMD ["./source-transformer-event-time-filter"]
