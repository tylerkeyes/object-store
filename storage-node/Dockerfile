FROM rust:1.90-slim-bookworm AS builder

WORKDIR /app

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler

# Set target dir to ensure binary is found at /app/target/release
ENV CARGO_TARGET_DIR=/app/target

# Copy the entire project to handle path dependencies
COPY . .

# Build the specific service
RUN cargo build --release --manifest-path storage-node/Cargo.toml

FROM debian:bookworm-slim

RUN groupadd -g 1000 storage-node \
  && useradd -u 1000 -g storage-node storage-node

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/storage-node /app/storage-node

# Expose the port
EXPOSE 3000

RUN chown -R storage-node:storage-node /app
USER 1000

# Set the entrypoint
CMD ["/app/storage-node"]
