FROM rust:1.85-bullseye AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /numaflow-rs
COPY ./ ./
WORKDIR /numaflow-rs/examples/sideinput/udf

# build for release
RUN cargo build --release

# our final base
FROM debian:bookworm AS sideinput-udf

# copy the build artifact from the build stage
COPY --from=build /numaflow-rs/target/release/sideinput-udf .

RUN mkdir -p /var/numaflow/sideinputs

# set the startup command to run your binary
CMD ["./sideinput-udf"]
