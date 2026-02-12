# Storage Node

## Overview
The storage node stores fixed-size chunk slots on disk and serves gRPC requests for read/write/delete. Chunks are stored in a single preallocated file with a small header per slot. Each chunk slot is 8 MiB.

## Run
From repo root:
```bash
cargo run -p storage-node --release
```

The service loads its configuration from `storage-node/config.yaml` by default. Override with `CONFIG_PATH=/path/to/config.yaml`.

## Configuration
Configuration lives in `storage-node/config.yaml` (or `storage-node/config-docker.yaml` for containers).

Example:
```yaml
metadata-addresses:
  - http://localhost:3001
port: 3000
metrics-port: 9092
chunk-store-path: "chunkstore.dat"
allocated-slots: 2048
fsync-every-n-writes: 1
fsync-on-delete: true
fast-recover: false
```

Field notes:
- `allocated-slots` is the number of 8 MiB slots to preallocate. Default is 2048 when not provided.
- `chunk-store-path` points to the data file used to store chunks.
- `fsync-every-n-writes` set to `0` disables fsync on writes.
- `fast-recover` skips checksum verification on startup.

Environment overrides:
- `CONFIG_PATH` path to a config file.
- `METADATA_URLS` comma-separated list of metadata service URLs.
- `GRPC_PORT` gRPC service port.
- `METRICS_PORT` metrics HTTP port.
- `ALLOCATED_SLOTS` number of chunk slots.
- `FSYNC_EVERY_N_WRITES` fsync cadence for writes.
- `FSYNC_ON_DELETE` enable fsync after deletes.
- `FAST_RECOVER` skip checksum verification during recovery.
- `OTLP_ENDPOINT` OpenTelemetry collector endpoint.

## Behavior Notes
- On startup the node registers itself to the metadata service using its local IP and configured gRPC port.
- gRPC max message size is 16 MiB to accommodate 8 MiB chunks plus overhead.

## Metrics
Metrics are exposed on `http://localhost:<metrics-port>/metrics`.

## Tracing and Logs
- JSON logs via `tracing`.
- OTLP traces exported to `OTLP_ENDPOINT`.
