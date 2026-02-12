# Replication Service

## Overview
The replication service monitors metadata for under-replicated chunks and copies them to additional storage nodes until each chunk has up to 3 replicas. It runs as a background loop and uses metadata and storage-node gRPC APIs.

## Run
From repo root:
```bash
METADATA_URL=http://localhost:3001 cargo run -p replication-service --release
```

## Configuration
Environment variables:
- `REPLICATION_INTERVAL_SECS` replication loop interval in seconds. Default: `60`.
- `REPLICATION_MAX_CHUNKS_PER_CYCLE` maximum number of chunks to replicate per cycle. Default: `100`.
- `METADATA_URLS` comma-separated metadata service URLs.
- `METADATA_URL` single metadata URL fallback when `METADATA_URLS` is not set. Default: `http://localhost:3001`.
- `OTLP_ENDPOINT` OpenTelemetry collector endpoint. Default: `http://localhost:4317`.

## Behavior Notes
- Targets up to 3 replicas per chunk.
- Skips nodes that already host the chunk.
- Checks target nodes for free slots before writing.
- Retries metadata connections and reconnects after consecutive failures.

## Metrics
Metrics are exposed on `http://localhost:9093/metrics`.

## Tracing and Logs
- JSON logs via `tracing`.
- OTLP traces exported to `OTLP_ENDPOINT`.
