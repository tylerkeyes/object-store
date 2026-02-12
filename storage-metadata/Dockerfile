FROM rust:1.90-slim-bookworm AS builder

WORKDIR /app

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler

# Set target dir to ensure binary is found at /app/target/release
ENV CARGO_TARGET_DIR=/app/target

# Copy the entire project to handle path dependencies
COPY . .

# Build the specific service
RUN cargo build --release --manifest-path storage-metadata/Cargo.toml

FROM debian:bookworm-slim

RUN groupadd -g 1000 storage-metadata \
  && useradd -u 1000 -g storage-metadata storage-metadata

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/storage-metadata /app/storage-metadata

# Expose the port
EXPOSE 3001

RUN chown -R storage-metadata:storage-metadata /app
USER 1000

# Set the entrypoint
CMD ["/app/storage-metadata"]
