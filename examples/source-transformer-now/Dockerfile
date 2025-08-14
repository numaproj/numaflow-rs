FROM rust:1.89-bullseye AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/source-transformer-now

# build for release
RUN cargo build --release

# our final base
FROM debian:bullseye AS source-transformer-now

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/target/release/source-transformer-now .

# set the startup command to run your binary
CMD ["./source-transformer-now"]
