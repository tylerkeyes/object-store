# Object Store

A distributed object storage system built in Rust, inspired by systems like Amazon S3. Objects are split into chunks, distributed across storage nodes, and replicated for durability.

## Architecture

```
┌─────────────────────────────────────────┐
│         API Gateway (port 8080)         │
│         REST/HTTP via Axum              │
└───────────────┬─────────────────────────┘
                │ gRPC (Tonic)
        ┌───────┴──────────────────┐
        │                          │
┌───────▼───────────────┐  ┌───────▼───────────────┐
│  Metadata Cluster      │  │   Storage Node(s)     │
│  3 nodes with gossip   │  │     (port 3000)       │
│  (ports 3001/3002/3003)│  │  Fixed-size file store│
│  In-memory + persisted │  │  128 slots × 8MB      │
└───────┬───────────────┘  └───────────────────────┘
        │                              ▲
        │    ┌─────────────────────────┘
        │    │ gRPC (replication)
┌───────▼────▼──────────────────────────┐
│          Replication Service           │
│   Background chunk replicator          │
│   Target: 3 replicas per chunk         │
└───────────────────────────────────────┘
```

**API Gateway** — HTTP entry point. Splits objects into chunks, coordinates chunk placement with the metadata cluster, and reads/writes chunks to storage nodes.

**Storage Metadata** — Clustered gRPC service (3 nodes with gossip-based membership) tracking objects, chunks, and storage node registry. Persists to disk; uses consistent hashing for chunk placement.

**Storage Node** — gRPC service storing binary chunks in a fixed-size file (`chunkstore.dat`). Self-registers to the metadata cluster on startup.

**Replication Service** — Background worker that detects under-replicated chunks and copies them to additional nodes to maintain a target of 3 replicas.

## Getting Started

### Docker Compose (recommended)

Starts all services plus the full observability stack (Prometheus, Grafana, Loki, Tempo):

```bash
docker-compose build
docker-compose up
```

### Local Development

Services must start in order:

```bash
# Terminal 1: Metadata service (port 3001, metrics 9091)
cargo run -p storage-metadata --release

# Terminal 2: Storage node (port 3000, metrics 9092)
cargo run -p storage-node --release

# Terminal 3: API gateway (port 8080)
METADATA_URL=http://localhost:3001 cargo run -p api-gateway --release

# Terminal 4 (optional): Replication service
METADATA_URL=http://localhost:3001 cargo run -p replication-service --release
```

## API

| Method | Path | Description |
|--------|------|-------------|
| `PUT` | `/<key>` | Upload an object (base64-encoded JSON body) |
| `GET` | `/<key>` | Download an object |
| `DELETE` | `/<key>` | Delete an object |
| `GET` | `/` | List all objects |

```bash
# Upload
curl -X PUT http://localhost:8080/myobject \
  -H "Content-Type: application/json" \
  -d '{"bytes":"aGVsbG8gd29ybGQ="}'

# Download
curl http://localhost:8080/myobject

# List
curl http://localhost:8080/

# Delete
curl -X DELETE http://localhost:8080/myobject
```

## Building

```bash
# Build all workspace members
cargo build --release

# Run all tests
cargo test --workspace

# Run E2E tests (requires services running)
cd tests && python e2e_test.py
```

### Container Images with Bazel (optional)

For hermetic cross-compilation of Linux OCI images from macOS:

```bash
# Build images
bazel build //api-gateway:api_gateway_image //storage-metadata:storage_metadata_image \
  //storage-node:storage_node_image --platforms=//platforms:linux_amd64 --action_env=HOME

# Load into Docker
bazel run //api-gateway:api_gateway_load --platforms=//platforms:linux_amd64 --action_env=HOME
```

## Observability

| Service | URL | Description |
|---------|-----|-------------|
| Grafana | http://localhost:3030 | Dashboards (Prometheus + Loki + Tempo) |
| Prometheus | http://localhost:9090 | Metrics |
| Tempo | http://localhost:3200 | Distributed traces |
| API Gateway metrics | http://localhost:8080/metrics | |
| Metadata metrics | http://localhost:9091/metrics | |
| Storage Node metrics | http://localhost:9092/metrics | |
| Replication metrics | http://localhost:9093/metrics | |

All services emit structured JSON logs, Prometheus metrics, and OpenTelemetry traces.

## Configuration

| Service | Environment Variable | Default | Description |
|---------|---------------------|---------|-------------|
| API Gateway | `METADATA_URLS` | — | Comma-separated metadata node URLs |
| API Gateway | `STORAGE_NODE_URL` | — | Storage node URL |
| Replication | `REPLICATION_INTERVAL_SECS` | `60` | Seconds between replication cycles |
| Replication | `REPLICATION_MAX_CHUNKS_PER_CYCLE` | `100` | Max chunks replicated per cycle |
| All services | `OTLP_ENDPOINT` | — | OpenTelemetry collector endpoint |
| All services | `RUST_LOG` | — | Log level (e.g. `info`, `debug`) |

## Project Structure

```
object-store/
├── api-gateway/         # HTTP entry point (Axum)
├── storage-metadata/    # Metadata cluster (gRPC, gossip)
├── storage-node/        # Chunk storage (gRPC)
├── replication-service/ # Background replication worker
├── protos/              # Shared gRPC proto definitions + client traits
├── tests/               # E2E test suite (Python)
├── grafana/             # Grafana dashboard provisioning
├── docker-compose.yml
├── prometheus.yml
└── Cargo.toml           # Workspace root
```

## Data Flow

**PUT object**: Gateway → `Metadata.AllocateChunk` → `StorageNode.GetChunkSize` → split data → for each chunk: `Metadata.ConfirmChunk` → `StorageNode.WriteChunk` → `Metadata.PutObject`

**GET object**: Gateway → `Metadata.GetObject` → for each chunk: `Metadata.GetChunk` → `StorageNode.ReadChunk` → reassemble

**Replication**: ReplicationService → `Metadata.ListUnderReplicatedChunks` → for each chunk: `StorageNode.ReadChunk` (source) → `StorageNode.WriteChunk` (target) → `Metadata.AddChunkReplica`
