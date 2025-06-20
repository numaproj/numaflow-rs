####################################################################################################
# Builder image
####################################################################################################
FROM rust:1.79-bookworm as builder

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /numaflow-rs

COPY Cargo.toml Cargo.lock ./
COPY numaflow ./numaflow
COPY examples/stream-sorter ./examples/stream-sorter

RUN cargo build --release --bin stream-sorter

####################################################################################################
# Runtime image
####################################################################################################
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /numaflow-rs/target/release/stream-sorter /usr/local/bin/stream-sorter

CMD ["stream-sorter"]
