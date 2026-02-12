# API Gateway

## Overview
The API Gateway is the HTTP entry point for the object store. It accepts REST requests, validates object names, splits objects into chunks, coordinates chunk placement with the metadata service, and reads/writes chunk data to storage nodes over gRPC.

## Run
From repo root:
```bash
cargo run -p api-gateway --release
```

The server listens on `0.0.0.0:8080`.

## Configuration
Environment variables:
- `METADATA_URLS` comma-separated metadata gRPC URLs. Example: `http://localhost:3001,http://localhost:3003`.
- `METADATA_URL` single metadata URL fallback when `METADATA_URLS` is not set. Default: `http://127.0.0.1:3001`.
- `OTLP_ENDPOINT` OpenTelemetry collector endpoint. Default: `http://localhost:4317`.

## HTTP API
Object name rules:
- 1 to 1024 characters.
- Allowed characters: alphanumeric, `-`, `_`, `.`, `/`.
- Must not start or end with `/`.
- Must not contain `//`.

Object size limit:
- Maximum object size is 10 GiB.
Note on JSON uploads:
- The JSON upload path buffers the full request body and enforces a 1.5 GB read limit. Use multipart streaming for larger objects.

Endpoints:
- `PUT /{objectName}`
- `GET /{objectName}`
- `DELETE /{objectName}`
- `GET /` (list objects)
- `GET /health`
- `GET /metrics`

### PUT /{objectName}
Two upload modes are supported based on `Content-Type`.

JSON mode:
- `Content-Type: application/json`
- Body: `{"bytes":"<base64>"}`
- Response: `{"msg":"upload complete","decoded_bytes_len":<bytes>}`

Multipart streaming mode:
- `Content-Type: multipart/form-data`
- Form field: `file` (binary data)
- Response: `{"msg":"upload complete","decoded_bytes_len":<bytes>}`

Chunk sizes are discovered dynamically from a storage node. The gateway allocates and confirms chunks via the metadata service for each chunk written.

### GET /{objectName}
Response depends on the `Accept` header.

JSON mode:
- `Accept: application/json`
- Response: `{"bytes":"<base64>"}`

Streaming mode:
- Any other `Accept` value or no `Accept`
- Response: `application/octet-stream` with chunked transfer encoding.

### GET /
Returns a JSON array of objects and their chunks:
```json
[
  {
    "name": "example",
    "chunks": [
      { "id": 1, "bytes": "<base64>", "node_address": "http://node:3000" }
    ]
  }
]
```

### DELETE /{objectName}
Deletes all chunk replicas from storage nodes, removes chunk metadata, then deletes the object. Response:
```json
{ "msg": "object deleted" }
```

## Observability
Metrics:
- `GET /metrics` exposes Prometheus metrics.

Tracing and logs:
- JSON logs via `tracing`.
- OTLP traces exported to `OTLP_ENDPOINT`.

## Behavior Notes
- The gateway retries reads and deletes against multiple storage nodes for a chunk.
- PUT requests use a two-phase allocate → write → confirm protocol with metadata.
