FROM rust:1.82-bullseye AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/map-cat

# build for release
RUN cargo build --release

# our final base
FROM debian:bullseye AS map-cat

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/target/release/map-cat .

# set the startup command to run your binary
CMD ["./map-cat"]
