# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build all workspace members
cargo build --release

# Build individual services
cargo build --release -p api-gateway
cargo build --release -p storage-metadata
cargo build --release -p storage-node
cargo build --release -p replication-service

# Run tests
cargo test --workspace

# Run E2E tests (requires services running or uses Cargo fallback)
cd tests && python e2e_test.py [--clean]

# Run with Docker Compose (recommended for full system)
docker-compose build
docker-compose up
```

### Bazel OCI Images (Optional)

Bazel can be used for hermetic cross-compilation of OCI container images from macOS to Linux:

```bash
# Build OCI container images
bazel build //api-gateway:api_gateway_image //storage-metadata:storage_metadata_image //storage-node:storage_node_image \
  --platforms=//platforms:linux_amd64 --action_env=HOME

# Load OCI images into Docker
bazel run //api-gateway:api_gateway_load --platforms=//platforms:linux_amd64 --action_env=HOME
bazel run //storage-metadata:storage_metadata_load --platforms=//platforms:linux_amd64 --action_env=HOME
bazel run //storage-node:storage_node_load --platforms=//platforms:linux_amd64 --action_env=HOME
```

## Running Services Locally

Services must be started in order (metadata first, then storage node, then gateway):

```bash
# Terminal 1: Metadata service (port 3001, metrics 9091)
cargo run -p storage-metadata --release

# Terminal 2: Storage node (port 3000, metrics 9092) - requires metadata service running
cargo run -p storage-node --release

# Terminal 3: API gateway (port 8080)
METADATA_URL=http://localhost:3001 cargo run -p api-gateway --release

# Terminal 4 (optional): Replication service (metrics 9093)
METADATA_URL=http://localhost:3001 cargo run -p replication-service --release
```

## Testing the API

```bash
# Upload (base64-encoded bytes in JSON body)
curl -X PUT http://localhost:8080/myobject \
  -H "Content-Type: application/json" \
  -d '{"bytes":"aGVsbG8gd29ybGQ="}'

# Download
curl http://localhost:8080/myobject

# List objects
curl http://localhost:8080/

# Delete
curl -X DELETE http://localhost:8080/myobject
```

## Architecture

This is a distributed object storage system with four core services communicating via gRPC:

```
┌─────────────────────────────────────────┐
│         API Gateway (port 8080)         │
│       REST/HTTP → Axum framework        │
└───────────────┬─────────────────────────┘
                │ gRPC (Tonic)
        ┌───────┴───────┐
        │               │
┌───────▼───────┐ ┌─────▼─────────────────┐
│   Metadata    │ │   Storage Node(s)     │
│  (port 3001)  │ │     (port 3000)       │
│  In-memory +  │ │  Fixed-size file      │
│  persistence  │ │  store (8MB chunks)   │
└───────┬───────┘ └───────────────────────┘
        │                  ▲
        │    ┌─────────────┘
        │    │ gRPC (replication)
┌───────▼────▼──────────────────┐
│   Replication Service         │
│   Background chunk replicator │
│   (metrics port 9093)         │
└───────────────────────────────┘
```

**API Gateway** (`api-gateway/`): HTTP entry point using Axum. Splits objects into chunks, coordinates with metadata service for chunk allocation, writes/reads chunks to/from storage nodes. Uses trait-based client abstraction (`src/clients.rs`) with mock implementations for testing. Handlers in `src/handlers.rs`, Prometheus metrics in `src/metrics.rs`.

**Storage Metadata** (`storage-metadata/`): gRPC service tracking objects, chunks, and storage node registry. Uses in-memory HashMaps persisted to `metadata.dat`. Supports chunk allocation (Query → Action → Persist pattern) and under-replication detection. Core logic in `src/metadata_store.rs`.

**Storage Node** (`storage-node/`): gRPC service storing binary chunks in a fixed-size file store (`chunkstore.dat`). 128 slots × 8MB = ~1GB capacity. Chunk store implementation in `src/chunk_store.rs`. Reads metadata service address from `config` file.

**Replication Service** (`replication-service/`): Background service that detects under-replicated chunks and copies them to additional storage nodes to maintain a target of 3 replicas. Configurable via `REPLICATION_INTERVAL_SECS` (default 60) and `REPLICATION_MAX_CHUNKS_PER_CYCLE` (default 100) env vars.

**Proto Definitions** (`protos/`): Shared gRPC definitions compiled via `tonic-build` in `build.rs`. Proto files in `protos/storage_metadata.proto` and `protos/storage_node.proto`.

## Data Flow

**PUT object**: Gateway → Metadata.AllocateChunk → StorageNode.GetChunkSize → split data → for each chunk: Metadata.ConfirmChunk → StorageNode.WriteChunk → finally Metadata.PutObject

**GET object**: Gateway → Metadata.GetObject → for each chunk: Metadata.GetChunk → StorageNode.ReadChunk → reassemble

**Replication**: ReplicationService → Metadata.ListUnderReplicatedChunks → for each chunk: StorageNode.ReadChunk (source) → StorageNode.WriteChunk (target) → Metadata.AddChunkReplica

## Observability

All services expose Prometheus metrics and use OpenTelemetry for distributed tracing. Docker Compose includes:

- **Prometheus** (port 9090): Scrapes metrics from all services
- **Grafana** (port 3030): Dashboards with Prometheus, Loki, and Tempo datasources
- **Tempo** (ports 4317/4318): Distributed trace collection via OTLP
- **Loki + Promtail** (port 3100): Log aggregation from Docker containers

Metrics endpoints:
- API Gateway: `http://localhost:8080/metrics`
- Storage Metadata: `http://localhost:9091/metrics`
- Storage Node: `http://localhost:9092/metrics`
- Replication Service: `http://localhost:9093/metrics`

## Key Implementation Details

- Workspace uses Cargo resolver v3 with 5 members: api-gateway, storage-metadata, storage-node, replication-service, protos
- Checksums use CRC32Fast for integrity verification
- Storage node self-registers to metadata service on startup (reads address from `config` file)
- Metadata service runs health checks every 20 seconds, removing unhealthy nodes
- Chunk allocation uses a two-phase pattern: AllocateChunk (reserve) → ConfirmChunk (persist)
- All services use OpenTelemetry + Tracing for structured JSON logging
- Internal gRPC traffic is plaintext (use service mesh or reverse proxy for TLS in production)
- Container images can be built with Docker Compose or optionally with Bazel for hermetic cross-compilation
- E2E tests in `tests/e2e_test.py` orchestrate service startup and run 8 test scenarios against the HTTP API
