FROM rust:1.88-bullseye AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/source-transformer-conditional-fail

# build for release
RUN cargo build --release

# our final base
FROM debian:bullseye AS source-transformer-conditional-fail

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/target/release/source-transformer-conditional-fail .

# set the startup command to run your binary
CMD ["./source-transformer-conditional-fail"]
