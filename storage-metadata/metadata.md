# Metadata Data Model (Quick Reference)

The metadata service persists objects and chunks to disk as JSON. Storage node registrations are in-memory only.

## MetaObject
Fields:
- `name`: object name.
- `checksum`: CRC32 checksum of the whole object.
- `chunks`: ordered list of chunk IDs.
- `total_size`: total object size in bytes.

## MetaChunk
Fields:
- `id`: chunk ID.
- `object_name`: owning object name.
- `checksum`: CRC32 checksum of the chunk.
- `storage_nodes`: list of storage node IDs that host this chunk.
- `status`: `Pending` or `Confirmed`.

## MetaStorageNode
Fields:
- `id`: numeric node ID assigned by metadata service.
- `address`: gRPC address used to reach the node (for example `http://host:3000`).

## Persistence
The metadata file is stored at `DATA_DIR/metadata-<node_id>.dat`.
