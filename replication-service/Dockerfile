FROM rust:1.90-slim-bookworm AS builder

WORKDIR /app

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler

# Set target dir to ensure binary is found at /app/target/release
ENV CARGO_TARGET_DIR=/app/target

# Copy the entire project to handle path dependencies
COPY . .

# Build the specific service
RUN cargo build --release --manifest-path replication-service/Cargo.toml

FROM debian:bookworm-slim

RUN groupadd -g 1000 replication-service \
  && useradd -u 1000 -g replication-service replication-service

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/replication-service /app/replication-service

# Expose the metrics port
EXPOSE 9093

RUN chown -R replication-service:replication-service /app
USER 1000

# Set the entrypoint
CMD ["/app/replication-service"]
