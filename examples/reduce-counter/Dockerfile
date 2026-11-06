FROM rust:1.99-bookworm AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/reduce-counter

# build for release
RUN cargo build --release

# our final base
FROM debian:bookworm AS reduce-counter

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/target/release/reduce-counter .

# set the startup command to run your binary
CMD ["./reduce-counter"]
