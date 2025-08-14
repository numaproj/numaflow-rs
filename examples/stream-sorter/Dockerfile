FROM rust:1.89-bullseye AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/stream-sorter

# build for release
RUN cargo build --release

# our final base
FROM debian:bullseye AS stream-sorter

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/target/release/stream-sorter .

# set the startup command to run your binary
CMD ["./stream-sorter"]
