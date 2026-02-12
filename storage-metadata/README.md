# Storage Metadata Service

## Overview
The metadata service tracks objects, chunks, and registered storage nodes. It owns chunk allocation and confirmation, detects under-replicated chunks, and exposes gRPC APIs used by the gateway and replication service. It can run standalone or as a clustered service with gossip-based membership and request forwarding.

## Run
From repo root:
```bash
cargo run -p storage-metadata --release
```

The gRPC server binds to `0.0.0.0:3001` by default. The metrics server binds to `0.0.0.0:9091` by default.

## Configuration
Environment variables:
- `NODE_ID` unique node identifier for clustering. Default: hostname.
- `GRPC_PORT` gRPC listen port. Default: `3001`.
- `GOSSIP_PORT` gossip/membership port. Default: `3002`.
- `SEED_NODES` comma-separated list of peer addresses for cluster bootstrap. Default: empty.
- `DATA_DIR` directory for persisted metadata files. Default: current directory (`.`).
- `METRICS_PORT` Prometheus metrics port. Default: `9091`.
- `OTLP_ENDPOINT` OpenTelemetry collector endpoint. Default: `http://localhost:4317`.

Persistence:
- Metadata is stored as JSON in `DATA_DIR/metadata-<node_id>.dat`.
- Storage node registrations are not persisted; nodes re-register on startup.

## gRPC Responsibilities
Key behaviors:
- Allocates chunk IDs and target storage nodes for new chunks.
- Confirms chunk writes after storage nodes persist data.
- Tracks object -> chunks and chunk -> storage node mappings.
- Lists objects and storage nodes for the gateway and replication service.
- Detects under-replicated chunks (< 3 replicas) for the replication service.

## Health Checks
A background task health-checks storage nodes approximately every 20 seconds (with jitter). Unhealthy nodes are removed from the in-memory registry.

## Observability
Metrics:
- `GET /metrics` exposes Prometheus metrics on `METRICS_PORT`.

Tracing and logs:
- JSON logs via `tracing`.
- OTLP traces exported to `OTLP_ENDPOINT`.

## Cluster Notes
- Gossip membership and consistent hashing are used for routing requests in a cluster.
- Requests may be forwarded to the node responsible for an object or chunk.
