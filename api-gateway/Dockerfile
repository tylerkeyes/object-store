FROM rust:1.90-slim-bookworm AS builder

WORKDIR /app

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler

# Set target dir to ensure binary is found at /app/target/release
ENV CARGO_TARGET_DIR=/app/target

# Copy the entire project to handle path dependencies
COPY . .

# Build the specific service
RUN cargo build --release --manifest-path api-gateway/Cargo.toml

FROM debian:bookworm-slim

RUN groupadd -g 1000 api-gateway \
  && useradd -u 1000 -g api-gateway api-gateway

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/api-gateway /app/api-gateway

# Expose the port
EXPOSE 8080

RUN chown -R api-gateway:api-gateway /app
USER 1000

# Set the entrypoint
CMD ["/app/api-gateway"]
