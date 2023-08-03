FROM rust:1.70 as build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

# create a new empty shell project
WORKDIR /examples

# copy your source tree
COPY src ./src

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# build for release
RUN cargo build --release

# our final base
FROM rust

# copy the build artifact from the build stage
COPY --from=build /examples/target/release/server .

# set the startup command to run your binary
CMD ["./server"]