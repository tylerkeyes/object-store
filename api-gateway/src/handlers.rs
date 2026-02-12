use axum::body::Body;
use axum::extract::{FromRequest, Multipart};
use axum::response::IntoResponse;
use axum::{
    Json,
    extract::{Path, State},
    http::{StatusCode, header, HeaderMap},
};
use base64::{Engine, engine::general_purpose};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage_proto_lib::{
    storage_metadata::{
        AllocateChunkRequest,
        ConfirmChunkRequest,
        DeleteChunkRequest as MetadataDeleteChunkRequest,
        DeleteObjectRequest,
        GetObjectRequest,
        ListObjectsRequest,
        ListStorageNodesRequest,
        PutObjectRequest,
        UpdateObjectChecksumRequest,
    },
    storage_node::{
        DeleteChunkRequest as StorageDeleteChunkRequest,
        GetChunkSizeRequest, ReadChunkRequest, WriteChunkRequest,
    },
    MetadataClient, StorageNodeClient, StorageNodeClientFactory,
};
use tonic::Code;

use crate::AppState;
use crate::metrics::{self, OperationLabels, RequestLabels};

/// Retry an async operation with exponential backoff. Only use for idempotent operations.
async fn retry_with_backoff<F, Fut, T, E>(max_retries: u32, mut f: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let delays = [100, 500, 2000]; // milliseconds
    let mut last_err = None;
    for attempt in 0..=max_retries {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                last_err = Some(e);
                if attempt < max_retries {
                    let delay = delays.get(attempt as usize).copied().unwrap_or(2000);
                    tokio::time::sleep(Duration::from_millis(delay as u64)).await;
                }
            }
        }
    }
    Err(last_err.unwrap())
}

/// Get or create a cached storage node connection.
async fn get_or_connect<F>(
    state: &Arc<AppState<impl MetadataClient, F>>,
    address: &str,
) -> Result<F::Client, (StatusCode, String)>
where
    F: StorageNodeClientFactory,
{
    // Check read lock first
    {
        let cache = state.storage_connections.read().await;
        if let Some(client) = cache.get(address) {
            return Ok(client.clone());
        }
    }
    // Not found, acquire write lock and connect
    let mut cache = state.storage_connections.write().await;
    // Double-check after acquiring write lock
    if let Some(client) = cache.get(address) {
        return Ok(client.clone());
    }
    let client = state
        .storage_factory
        .connect(address)
        .await
        .map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("could not connect to storage node {}: {}", address, err),
            )
        })?;
    cache.insert(address.to_string(), client.clone());
    Ok(client)
}

fn compute_checksum(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

const MAX_OBJECT_NAME_LENGTH: usize = 1024;
const MAX_OBJECT_SIZE: usize = 10 * 1024 * 1024 * 1024; // 10 GiB cluster-wide limit (objects span multiple nodes)

fn validate_object_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("object name must not be empty".to_string());
    }
    if name.len() > MAX_OBJECT_NAME_LENGTH {
        return Err(format!(
            "object name must not exceed {} characters",
            MAX_OBJECT_NAME_LENGTH
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/')
    {
        return Err(
            "object name may only contain alphanumeric characters, hyphens, underscores, dots, and slashes"
                .to_string(),
        );
    }
    if name.starts_with('/') || name.ends_with('/') {
        return Err("object name must not start or end with a slash".to_string());
    }
    if name.contains("//") {
        return Err("object name must not contain consecutive slashes".to_string());
    }
    Ok(())
}

#[derive(Deserialize)]
pub struct PutObjectRequestPayload {
    pub bytes: String,
}

#[derive(Debug, Serialize)]
pub struct PutObjectResponsePayload {
    pub msg: String,
    pub decoded_bytes_len: usize,
}

#[tracing::instrument(skip(state, payload), fields(object_name = %object_name, error, decoded_bytes, decoded_bytes_len, storage_nodes_count, chunk_size, object_checksum, chunks_count, storage_node_address, chunk_id, cleanup_error, response.decoded_bytes_len))]
pub async fn handle_put_object<M, F>(
    Path(object_name): Path<String>,
    State(state): State<Arc<AppState<M, F>>>,
    Json(payload): Json<PutObjectRequestPayload>,
) -> Result<Json<PutObjectResponsePayload>, (axum::http::StatusCode, String)>
where
    M: MetadataClient,
    F: StorageNodeClientFactory,
{
    let start = Instant::now();

    if let Err(msg) = validate_object_name(&object_name) {
        tracing::Span::current().record("error", &msg);
        state
            .metrics
            .http_requests_total
            .get_or_create(&RequestLabels {
                method: "PUT".to_string(),
                endpoint: "object".to_string(),
                status: "400".to_string(),
            })
            .inc();
        return Err((StatusCode::BAD_REQUEST, msg));
    }

    let decoded_bytes = match general_purpose::STANDARD.decode(&payload.bytes) {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::Span::current().record("error", format!("invalid base64 encoding: {}", e));
            state
                .metrics
                .http_requests_total
                .get_or_create(&RequestLabels {
                    method: "PUT".to_string(),
                    endpoint: "object".to_string(),
                    status: "400".to_string(),
                })
                .inc();
            return Err((
                StatusCode::BAD_REQUEST,
                format!("invalid base64 encoding: {}", e),
            ));
        }
    };
    if decoded_bytes.len() > MAX_OBJECT_SIZE {
        tracing::Span::current().record("error", "object too large");
        state
            .metrics
            .http_requests_total
            .get_or_create(&RequestLabels {
                method: "PUT".to_string(),
                endpoint: "object".to_string(),
                status: "413".to_string(),
            })
            .inc();
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "object size {} exceeds maximum allowed size {}",
                decoded_bytes.len(),
                MAX_OBJECT_SIZE
            ),
        ));
    }

    let span = tracing::Span::current();
    let decoded_len = decoded_bytes.len();
    span.record("decoded_bytes_len", decoded_len);
    // Avoid logging large payloads to prevent blocking stdout pipes in test harnesses.
    if decoded_len <= 512 {
        match std::str::from_utf8(&decoded_bytes) {
            Ok(byte_str) => span.record("decoded_bytes", byte_str),
            Err(_) => span.record("decoded_bytes", "<non-utf8>"),
        };
    } else {
        span.record("decoded_bytes", "<omitted>");
    }

    let mut metadata_client = state.metadata_client.clone();

    let storage_nodes = match metadata_client
        .list_storage_nodes(ListStorageNodesRequest {})
        .await
    {
        Ok(storage_nodes) => storage_nodes.get_ref().storage_nodes.clone(),
        Err(err) => {
            tracing::Span::current().record("error", format!("no storage nodes found: {}", err));
            state
                .metrics
                .http_requests_total
                .get_or_create(&RequestLabels {
                    method: "PUT".to_string(),
                    endpoint: "object".to_string(),
                    status: "500".to_string(),
                })
                .inc();
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("no storage nodes found: {}", err),
            ));
        }
    };

    if storage_nodes.is_empty() {
        tracing::Span::current().record("error", "no storage nodes found");
        state
            .metrics
            .http_requests_total
            .get_or_create(&RequestLabels {
                method: "PUT".to_string(),
                endpoint: "object".to_string(),
                status: "500".to_string(),
            })
            .inc();
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "no storage nodes found".to_string(),
        ));
    }

    // Safe: we already checked storage_nodes is non-empty above
    let test_address = storage_nodes[0].address.clone();
    tracing::Span::current().record("storage_nodes_count", storage_nodes.len());
    let mut storage_client = match state.storage_factory.connect(&test_address).await {
        Ok(storage_client) => storage_client,
        Err(err) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "could not connect to storage node {}: {}",
                    test_address, err
                ),
            ));
        }
    };

    // This should be the same between all storage nodes
    let chunk_size = match storage_client
        .get_chunk_size(GetChunkSizeRequest {})
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            tracing::Span::current().record("error", format!("could not get chunk size: {}", err));
            state
                .metrics
                .http_requests_total
                .get_or_create(&RequestLabels {
                    method: "PUT".to_string(),
                    endpoint: "object".to_string(),
                    status: "500".to_string(),
                })
                .inc();
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("could not get chunk size: {}", err),
            ));
        }
    };

    let chunk_size_value = chunk_size.get_ref().chunk_size;
    tracing::Span::current().record("chunk_size", chunk_size_value);

    let object_checksum = compute_checksum(&decoded_bytes);
    tracing::Span::current().record("object_checksum", object_checksum);
    let chunked: Vec<Vec<u8>> = decoded_bytes
        .chunks(chunk_size.get_ref().chunk_size as usize)
        .map(|chunk| chunk.to_vec())
        .collect();

    let put_obj_req = PutObjectRequest {
        checksum: object_checksum,
        name: object_name.clone(),
    };
    if metadata_client.put_object(put_obj_req).await.is_err() {
        tracing::Span::current().record("error", "object name already used");
        state
            .metrics
            .http_requests_total
            .get_or_create(&RequestLabels {
                method: "PUT".to_string(),
                endpoint: "object".to_string(),
                status: "409".to_string(),
            })
            .inc();
        return Err((StatusCode::CONFLICT, "object name already used".to_string()));
    }

    tracing::Span::current().record("chunks_count", chunked.len());
    for chunk in chunked {
        let chunk_checksum = compute_checksum(&chunk);

        // Step 1: Query - Ask metadata which node to use (no persistence yet)
        let allocate_resp = match metadata_client
            .allocate_chunk(AllocateChunkRequest {
                object_name: object_name.clone(),
                checksum: chunk_checksum,
            })
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                tracing::Span::current().record("error", format!("could not allocate chunk: {}", err));
                if metadata_client
                    .delete_object(DeleteObjectRequest {
                        object_name: object_name.clone(),
                    })
                    .await
                    .is_err()
                {
                    tracing::Span::current().record(
                        "cleanup_error",
                        format!("could not delete object {}", object_name.clone()),
                    );
                }
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "could not allocate chunk".to_string(),
                ));
            }
        };

        let allocate_resp = allocate_resp.get_ref();
        tracing::Span::current()
            .record("storage_node_address", &allocate_resp.storage_node_address);
        tracing::Span::current().record("chunk_id", allocate_resp.chunk_id);

        // Step 2: Action - Write to storage node
        let mut storage_node_client = get_or_connect(&state, &allocate_resp.storage_node_address).await?;

        let write_chunk_req = WriteChunkRequest {
            chunk_id: allocate_resp.chunk_id,
            chunk_bytes: chunk,
        };

        if let Err(err) = storage_node_client.write_chunk(write_chunk_req).await {
            let error_msg = format!(
                "could not write chunk {} in object {}: {}",
                allocate_resp.chunk_id, object_name, err
            );
            tracing::Span::current().record("error", &error_msg);
            // Rollback: delete the partially created object from metadata
            if let Err(cleanup_err) = metadata_client
                .delete_object(DeleteObjectRequest {
                    object_name: object_name.clone(),
                })
                .await
            {
                tracing::Span::current().record(
                    "cleanup_error",
                    format!("failed to rollback object {}: {}", object_name, cleanup_err),
                );
            }
            return Err((StatusCode::INTERNAL_SERVER_ERROR, error_msg));
        }

        // Step 3: Persist - Confirm the chunk was written successfully
        if let Err(err) = metadata_client
            .confirm_chunk(ConfirmChunkRequest {
                chunk_id: allocate_resp.chunk_id,
                object_name: object_name.clone(),
                checksum: chunk_checksum,
                storage_node_id: allocate_resp.storage_node_id,
            })
            .await
        {
            let error_msg = format!(
                "could not confirm chunk {} in object {}: {}",
                allocate_resp.chunk_id, object_name, err
            );
            tracing::Span::current().record("error", &error_msg);
            // Rollback: delete the partially created object from metadata
            if let Err(cleanup_err) = metadata_client
                .delete_object(DeleteObjectRequest {
                    object_name: object_name.clone(),
                })
                .await
            {
                tracing::Span::current().record(
                    "cleanup_error",
                    format!("failed to rollback object {}: {}", object_name, cleanup_err),
                );
            }
            return Err((StatusCode::INTERNAL_SERVER_ERROR, error_msg));
        }
    }

    // Record successful metrics
    let bytes_len = decoded_bytes.len();
    state.metrics.objects_uploaded_total.inc();
    state.metrics.bytes_uploaded_total.inc_by(bytes_len as u64);
    state
        .metrics
        .http_requests_total
        .get_or_create(&RequestLabels {
            method: "PUT".to_string(),
            endpoint: "object".to_string(),
            status: "200".to_string(),
        })
        .inc();
    state
        .metrics
        .http_request_duration_seconds
        .get_or_create(&OperationLabels {
            operation: "put_object".to_string(),
        })
        .observe(start.elapsed().as_secs_f64());

    tracing::Span::current().record("response.decoded_bytes_len", bytes_len);
    Ok(Json(PutObjectResponsePayload {
        msg: "upload complete".to_string(),
        decoded_bytes_len: bytes_len,
    }))
}

#[derive(Debug, Serialize)]
pub struct GetObjectResponsePayload {
    pub bytes: String,
}

#[tracing::instrument(skip(state), fields(object_name = %object_name, error, chunks_count, response.data_len))]
pub async fn handle_get_object<M, F>(
    Path(object_name): Path<String>,
    State(state): State<Arc<AppState<M, F>>>,
) -> Result<Json<GetObjectResponsePayload>, (axum::http::StatusCode, String)>
where
    M: MetadataClient,
    F: StorageNodeClientFactory,
{
    let start = Instant::now();

    if let Err(msg) = validate_object_name(&object_name) {
        tracing::Span::current().record("error", &msg);
        state
            .metrics
            .http_requests_total
            .get_or_create(&RequestLabels {
                method: "GET".to_string(),
                endpoint: "object".to_string(),
                status: "400".to_string(),
            })
            .inc();
        return Err((StatusCode::BAD_REQUEST, msg));
    }

    let mut metadata_client = state.metadata_client.clone();

    let get_object_resp = match metadata_client
        .get_object(GetObjectRequest {
            object_name: object_name.clone(),
        })
        .await
    {
        Ok(get_object_resp) => get_object_resp,
        Err(err) => {
            tracing::Span::current().record(
                "error",
                format!("could not get object {}: {}", object_name.clone(), err),
            );
            state
                .metrics
                .http_requests_total
                .get_or_create(&RequestLabels {
                    method: "GET".to_string(),
                    endpoint: "object".to_string(),
                    status: "404".to_string(),
                })
                .inc();
            return Err((
                StatusCode::NOT_FOUND,
                format!("could not get object {}: {}", object_name.clone(), err),
            ));
        }
    };

    let chunks = get_object_resp.into_inner().chunks;
    tracing::Span::current().record("chunks_count", chunks.len());

    // Read all chunks in parallel using FuturesOrdered to preserve ordering
    let read_futures: Vec<_> = chunks
        .iter()
        .map(|chunk| {
            let state = state.clone();
            let storage_nodes = chunk.storage_nodes.clone();
            let chunk_id = chunk.id;
            async move {
                // Try each storage node until one succeeds (failover) with retry
                for storage_node in &storage_nodes {
                    let addr = storage_node.address.clone();
                    let client = match get_or_connect(&state, &addr).await {
                        Ok(c) => c,
                        Err(_) => {
                            tracing::warn!("Failed to connect to storage node {}", addr);
                            continue;
                        }
                    };
                    match retry_with_backoff(2, || {
                        let mut c = client.clone();
                        async move { c.read_chunk(ReadChunkRequest { chunk_id }).await }
                    })
                    .await
                    {
                        Ok(resp) => return Ok(resp.into_inner().chunk_bytes),
                        Err(e) => {
                            tracing::warn!("Failed to read chunk {} from node {}: {}", chunk_id, addr, e);
                            continue;
                        }
                    }
                }
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("could not read chunk {} from any storage node", chunk_id),
                ))
            }
        })
        .collect();

    let results = join_all(read_futures).await;

    let mut chunk_data: Vec<u8> = Vec::new();
    for result in results {
        match result {
            Ok(bytes) => chunk_data.extend_from_slice(&bytes),
            Err((status, msg)) => {
                tracing::Span::current().record("error", &msg);
                state
                    .metrics
                    .http_requests_total
                    .get_or_create(&RequestLabels {
                        method: "GET".to_string(),
                        endpoint: "object".to_string(),
                        status: "500".to_string(),
                    })
                    .inc();
                return Err((status, msg));
            }
        }
    }

    let data_len = chunk_data.len();
    tracing::Span::current().record("response.data_len", data_len);
    let encoded = general_purpose::STANDARD.encode(&chunk_data);
    state.metrics.objects_downloaded_total.inc();
    state.metrics.bytes_downloaded_total.inc_by(data_len as u64);
    state
        .metrics
        .http_requests_total
        .get_or_create(&RequestLabels {
            method: "GET".to_string(),
            endpoint: "object".to_string(),
            status: "200".to_string(),
        })
        .inc();
    state
        .metrics
        .http_request_duration_seconds
        .get_or_create(&OperationLabels {
            operation: "get_object".to_string(),
        })
        .observe(start.elapsed().as_secs_f64());
    Ok(Json(GetObjectResponsePayload { bytes: encoded }))
}

#[derive(Serialize)]
pub struct ListObjectsResponseChunk {
    pub id: u64,
    pub bytes: String,
    pub node_address: String,
}

#[derive(Serialize)]
pub struct ListObjectsResponsePayload {
    pub name: String,
    pub chunks: Vec<ListObjectsResponseChunk>,
}

#[tracing::instrument(skip(state), fields(error, objects_count, response.objects_count))]
pub async fn handle_list_objects<M, F>(
    State(state): State<Arc<AppState<M, F>>>,
) -> Result<Json<Vec<ListObjectsResponsePayload>>, (axum::http::StatusCode, String)>
where
    M: MetadataClient,
    F: StorageNodeClientFactory,
{
    let start = Instant::now();
    let mut metadata_client = state.metadata_client.clone();

    let objects = match metadata_client.list_objects(ListObjectsRequest {}).await {
        Ok(objects) => objects,
        Err(err) => {
            tracing::Span::current().record("error", format!("could not list objects: {}", err));
            state
                .metrics
                .http_requests_total
                .get_or_create(&RequestLabels {
                    method: "GET".to_string(),
                    endpoint: "list".to_string(),
                    status: "500".to_string(),
                })
                .inc();
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("could not list objects: {}", err),
            ));
        }
    };

    let objects_list = objects.get_ref().objects.clone();
    tracing::Span::current().record("objects_count", objects_list.len());
    let mut object_list: Vec<ListObjectsResponsePayload> = Vec::new();

    for obj in objects_list {
        let mut chunks: Vec<ListObjectsResponseChunk> = Vec::new();
        for chunk in obj.chunks {
            if chunk.storage_nodes.is_empty() {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("chunk {} has no storage nodes", chunk.id),
                ));
            }

            let storage_node_client: Result<(F::Client, String), (axum::http::StatusCode, String)> = {
                let mut last_err = None;
                let mut result = None;
                for storage_node in &chunk.storage_nodes {
                    match state.storage_factory.connect(&storage_node.address).await {
                        Ok(client) => {
                            result = Some((client, storage_node.address.clone()));
                            break;
                        }
                        Err(e) => {
                            last_err = Some(e);
                        }
                    }
                }
                match result {
                    Some(r) => Ok(r),
                    None => Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "no storage node for chunk {}: {}",
                            chunk.id,
                            last_err.map(|e| e.to_string()).unwrap_or_default()
                        ),
                    )),
                }
            };

            let (read_chunk_resp, node_address) = match storage_node_client {
                Ok((mut client, address)) => {
                    let chunk_resp =
                        match client.read_chunk(ReadChunkRequest { chunk_id: chunk.id }).await {
                            Ok(chunk_resp) => chunk_resp,
                            Err(err) => {
                                return Err((
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    format!(
                                        "could not read chunk {} from storage nodes: {}",
                                        chunk.id, err
                                    ),
                                ));
                            }
                        };
                    (chunk_resp, address)
                }
                Err(err) => {
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "could not read chunk {} from storage nodes: {:?}",
                            chunk.id, err
                        ),
                    ));
                }
            };

            let chunk_bytes = general_purpose::STANDARD.encode(&read_chunk_resp.get_ref().chunk_bytes);

            let chunk_payload = ListObjectsResponseChunk {
                id: chunk.id,
                bytes: chunk_bytes,
                node_address,
            };
            chunks.push(chunk_payload);
        }

        let obj_inst = ListObjectsResponsePayload {
            name: obj.name.clone(),
            chunks,
        };
        object_list.push(obj_inst);
    }

    tracing::Span::current().record("response.objects_count", object_list.len());
    state
        .metrics
        .http_requests_total
        .get_or_create(&RequestLabels {
            method: "GET".to_string(),
            endpoint: "list".to_string(),
            status: "200".to_string(),
        })
        .inc();
    state
        .metrics
        .http_request_duration_seconds
        .get_or_create(&OperationLabels {
            operation: "list_objects".to_string(),
        })
        .observe(start.elapsed().as_secs_f64());
    Ok(Json(object_list))
}

#[derive(Debug, Serialize)]
pub struct DeleteObjectResponsePayload {
    pub msg: String,
}

#[tracing::instrument(skip(state), fields(object_name = %object_name, error, chunks_deleted, storage_nodes_deleted))]
pub async fn handle_delete_object<M, F>(
    Path(object_name): Path<String>,
    State(state): State<Arc<AppState<M, F>>>,
) -> Result<Json<DeleteObjectResponsePayload>, (axum::http::StatusCode, String)>
where
    M: MetadataClient,
    F: StorageNodeClientFactory,
{
    let start = Instant::now();

    if let Err(msg) = validate_object_name(&object_name) {
        tracing::Span::current().record("error", &msg);
        state
            .metrics
            .http_requests_total
            .get_or_create(&RequestLabels {
                method: "DELETE".to_string(),
                endpoint: "object".to_string(),
                status: "400".to_string(),
            })
            .inc();
        return Err((StatusCode::BAD_REQUEST, msg));
    }

    let mut metadata_client = state.metadata_client.clone();
    let span = tracing::Span::current();

    // First, get the object to find all chunks
    let get_object_resp = match metadata_client
        .get_object(GetObjectRequest {
            object_name: object_name.clone(),
        })
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            let error_msg = match err.code() {
                Code::NotFound => "object not found".to_string(),
                _ => format!("could not get object: {}", err.message()),
            };
            span.record("error", &error_msg);
            state
                .metrics
                .http_requests_total
                .get_or_create(&RequestLabels {
                    method: "DELETE".to_string(),
                    endpoint: "object".to_string(),
                    status: "404".to_string(),
                })
                .inc();
            return Err((StatusCode::NOT_FOUND, error_msg));
        }
    };

    // Delete each chunk from all storage nodes in parallel, then update metadata
    let chunks = get_object_resp.get_ref().chunks.clone();

    // Collect all (chunk_id, address) pairs to delete
    let mut delete_targets = Vec::new();
    for chunk in &chunks {
        for storage_node in &chunk.storage_nodes {
            delete_targets.push((chunk.id, storage_node.address.clone()));
        }
    }

    // Build delete futures for all chunk-node pairs
    let delete_futures: Vec<_> = delete_targets
        .into_iter()
        .map(|(chunk_id, address)| {
            let state_clone = state.clone();
            async move {
                match get_or_connect(&state_clone, &address).await {
                    Ok(client) => {
                        match retry_with_backoff(2, || {
                            let mut c = client.clone();
                            async move {
                                c.delete_chunk(StorageDeleteChunkRequest { chunk_id }).await
                            }
                        })
                        .await
                        {
                            Ok(_) => true,
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to delete chunk {} from node {}: {}",
                                    chunk_id, address, e
                                );
                                false
                            }
                        }
                    }
                    Err(_) => {
                        tracing::warn!(
                            "Failed to connect to storage node {} for delete",
                            address
                        );
                        false
                    }
                }
            }
        })
        .collect();

    let delete_results = join_all(delete_futures).await;
    let total_deleted_nodes = delete_results.iter().filter(|&&ok| ok).count();

    // Delete chunks from metadata (sequentially since metadata is stateful)
    for chunk in &chunks {
        if let Err(e) = metadata_client
            .delete_chunk(MetadataDeleteChunkRequest { chunk_id: chunk.id })
            .await
        {
            tracing::warn!("Failed to delete chunk {} from metadata: {}", chunk.id, e);
        }
    }

    span.record("chunks_deleted", chunks.len());
    span.record("storage_nodes_deleted", total_deleted_nodes);

    // Step 3: Persist state change - delete object from metadata
    match metadata_client
        .delete_object(DeleteObjectRequest {
            object_name: object_name.clone(),
        })
        .await
    {
        Ok(_) => {
            state.metrics.objects_deleted_total.inc();
            state
                .metrics
                .http_requests_total
                .get_or_create(&RequestLabels {
                    method: "DELETE".to_string(),
                    endpoint: "object".to_string(),
                    status: "200".to_string(),
                })
                .inc();
            state
                .metrics
                .http_request_duration_seconds
                .get_or_create(&OperationLabels {
                    operation: "delete_object".to_string(),
                })
                .observe(start.elapsed().as_secs_f64());
            Ok(Json(DeleteObjectResponsePayload {
                msg: "object deleted".to_string(),
            }))
        }
        Err(err) => {
            let error_msg = match err.code() {
                Code::NotFound => "object not found".to_string(),
                _ => format!("could not delete object: {}", err.message()),
            };
            span.record("error", &error_msg);
            state
                .metrics
                .http_requests_total
                .get_or_create(&RequestLabels {
                    method: "DELETE".to_string(),
                    endpoint: "object".to_string(),
                    status: "500".to_string(),
                })
                .inc();
            Err((StatusCode::INTERNAL_SERVER_ERROR, error_msg))
        }
    }
}

/// Write a buffer as a single chunk: allocate → write → confirm.
async fn flush_chunk_to_storage<M, F>(
    buf: &[u8],
    metadata_client: &mut M,
    state: &Arc<AppState<M, F>>,
    object_name: &str,
) -> Result<(), (StatusCode, String)>
where
    M: MetadataClient,
    F: StorageNodeClientFactory,
{
    let chunk_checksum = compute_checksum(buf);
    let allocate_resp = metadata_client
        .allocate_chunk(AllocateChunkRequest {
            object_name: object_name.to_string(),
            checksum: chunk_checksum,
        })
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, format!("could not allocate chunk: {}", err)))?;

    let alloc = allocate_resp.get_ref();
    let mut storage_node_client = get_or_connect(state, &alloc.storage_node_address).await?;

    storage_node_client
        .write_chunk(WriteChunkRequest {
            chunk_id: alloc.chunk_id,
            chunk_bytes: buf.to_vec(),
        })
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, format!("could not write chunk: {}", err)))?;

    metadata_client
        .confirm_chunk(ConfirmChunkRequest {
            chunk_id: alloc.chunk_id,
            object_name: object_name.to_string(),
            checksum: chunk_checksum,
            storage_node_id: alloc.storage_node_id,
        })
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, format!("could not confirm chunk: {}", err)))?;

    Ok(())
}

/// Streaming multipart upload handler.
/// Accepts `PUT /{objectName}` with `Content-Type: multipart/form-data` and a "file" field.
/// Streams data in chunk-sized buffers, computing a rolling CRC32 checksum.
#[tracing::instrument(skip(state, multipart), fields(object_name = %object_name, error, total_size, chunks_count, object_checksum))]
pub async fn handle_put_object_multipart<M, F>(
    Path(object_name): Path<String>,
    State(state): State<Arc<AppState<M, F>>>,
    mut multipart: Multipart,
) -> Result<Json<PutObjectResponsePayload>, (StatusCode, String)>
where
    M: MetadataClient,
    F: StorageNodeClientFactory,
{
    let start = Instant::now();
    let span = tracing::Span::current();

    if let Err(msg) = validate_object_name(&object_name) {
        span.record("error", &msg);
        state.metrics.http_requests_total.get_or_create(&RequestLabels {
            method: "PUT".to_string(), endpoint: "object_multipart".to_string(), status: "400".to_string(),
        }).inc();
        return Err((StatusCode::BAD_REQUEST, msg));
    }

    // Get chunk size from a storage node
    let mut metadata_client = state.metadata_client.clone();

    let storage_nodes = match metadata_client.list_storage_nodes(ListStorageNodesRequest {}).await {
        Ok(resp) => resp.get_ref().storage_nodes.clone(),
        Err(err) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("no storage nodes found: {}", err)));
        }
    };
    if storage_nodes.is_empty() {
        return Err((StatusCode::INTERNAL_SERVER_ERROR, "no storage nodes found".to_string()));
    }

    let mut storage_client = match state.storage_factory.connect(&storage_nodes[0].address).await {
        Ok(c) => c,
        Err(err) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("could not connect to storage node: {}", err)));
        }
    };
    let chunk_size = match storage_client.get_chunk_size(GetChunkSizeRequest {}).await {
        Ok(resp) => resp.get_ref().chunk_size as usize,
        Err(err) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("could not get chunk size: {}", err)));
        }
    };

    // Create object with placeholder checksum (will update after streaming)
    let put_obj_req = PutObjectRequest {
        checksum: 0,
        name: object_name.clone(),
    };
    if metadata_client.put_object(put_obj_req).await.is_err() {
        state.metrics.http_requests_total.get_or_create(&RequestLabels {
            method: "PUT".to_string(), endpoint: "object_multipart".to_string(), status: "409".to_string(),
        }).inc();
        return Err((StatusCode::CONFLICT, "object name already used".to_string()));
    }

    // Find the "file" field in the multipart request
    let field = loop {
        match multipart.next_field().await {
            Ok(Some(f)) => {
                if f.name() == Some("file") {
                    break f;
                }
                // Skip non-file fields
            }
            Ok(None) => {
                // No "file" field found, rollback
                let _ = metadata_client.delete_object(DeleteObjectRequest { object_name: object_name.clone() }).await;
                return Err((StatusCode::BAD_REQUEST, "missing 'file' field in multipart request".to_string()));
            }
            Err(err) => {
                let _ = metadata_client.delete_object(DeleteObjectRequest { object_name: object_name.clone() }).await;
                return Err((StatusCode::BAD_REQUEST, format!("multipart error: {}", err)));
            }
        }
    };

    // Stream the field in chunk_size buffers
    let mut hasher = crc32fast::Hasher::new();
    let mut buffer = Vec::with_capacity(chunk_size);
    let mut total_size: u64 = 0;
    let mut chunks_written: u64 = 0;

    // Read from the multipart field in streaming fashion
    let mut stream_error: Option<(StatusCode, String)> = None;
    let mut field = field;
    loop {
        match field.chunk().await {
            Ok(Some(chunk_data)) => {
                hasher.update(&chunk_data);
                total_size += chunk_data.len() as u64;

                if total_size > MAX_OBJECT_SIZE as u64 {
                    stream_error = Some((StatusCode::PAYLOAD_TOO_LARGE, format!("object exceeds maximum size {}", MAX_OBJECT_SIZE)));
                    break;
                }

                buffer.extend_from_slice(&chunk_data);

                // Flush full chunks
                while buffer.len() >= chunk_size {
                    let chunk_buf: Vec<u8> = buffer.drain(..chunk_size).collect();
                    if let Err(e) = flush_chunk_to_storage(&chunk_buf, &mut metadata_client, &state, &object_name).await {
                        stream_error = Some(e);
                        break;
                    }
                    chunks_written += 1;
                }
                if stream_error.is_some() {
                    break;
                }
            }
            Ok(None) => break, // End of stream
            Err(err) => {
                stream_error = Some((StatusCode::BAD_REQUEST, format!("stream read error: {}", err)));
                break;
            }
        }
    }

    // Check for errors
    if let Some(err) = stream_error {
        let _ = metadata_client.delete_object(DeleteObjectRequest { object_name: object_name.clone() }).await;
        span.record("error", &err.1);
        return Err(err);
    }

    // Flush remaining data in buffer
    if !buffer.is_empty() {
        if let Err(e) = flush_chunk_to_storage(&buffer, &mut metadata_client, &state, &object_name).await {
            let _ = metadata_client.delete_object(DeleteObjectRequest { object_name: object_name.clone() }).await;
            span.record("error", &e.1);
            return Err(e);
        }
        chunks_written += 1;
    }

    // Update object checksum now that we've streamed all data
    let object_checksum = hasher.finalize();
    if let Err(err) = metadata_client
        .update_object_checksum(UpdateObjectChecksumRequest {
            object_name: object_name.clone(),
            checksum: object_checksum,
            total_size,
        })
        .await
    {
        let _ = metadata_client.delete_object(DeleteObjectRequest { object_name: object_name.clone() }).await;
        let error_msg = format!("could not update object checksum: {}", err);
        span.record("error", &error_msg);
        return Err((StatusCode::INTERNAL_SERVER_ERROR, error_msg));
    }

    span.record("total_size", total_size);
    span.record("chunks_count", chunks_written);
    span.record("object_checksum", object_checksum);

    state.metrics.objects_uploaded_total.inc();
    state.metrics.bytes_uploaded_total.inc_by(total_size);
    state.metrics.http_requests_total.get_or_create(&RequestLabels {
        method: "PUT".to_string(), endpoint: "object_multipart".to_string(), status: "200".to_string(),
    }).inc();
    state.metrics.http_request_duration_seconds.get_or_create(&OperationLabels {
        operation: "put_object_multipart".to_string(),
    }).observe(start.elapsed().as_secs_f64());

    Ok(Json(PutObjectResponsePayload {
        msg: "upload complete".to_string(),
        decoded_bytes_len: total_size as usize,
    }))
}

/// Streaming binary download handler.
/// Returns `application/octet-stream` with chunked transfer encoding.
#[tracing::instrument(skip(state), fields(object_name = %object_name, error, chunks_count, total_size))]
pub async fn handle_get_object_stream<M, F>(
    Path(object_name): Path<String>,
    State(state): State<Arc<AppState<M, F>>>,
) -> Result<axum::response::Response, (StatusCode, String)>
where
    M: MetadataClient + 'static,
    F: StorageNodeClientFactory + 'static,
{
    let start = Instant::now();

    if let Err(msg) = validate_object_name(&object_name) {
        tracing::Span::current().record("error", &msg);
        state.metrics.http_requests_total.get_or_create(&RequestLabels {
            method: "GET".to_string(), endpoint: "object_stream".to_string(), status: "400".to_string(),
        }).inc();
        return Err((StatusCode::BAD_REQUEST, msg));
    }

    let mut metadata_client = state.metadata_client.clone();

    let get_object_resp = match metadata_client
        .get_object(GetObjectRequest { object_name: object_name.clone() })
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            state.metrics.http_requests_total.get_or_create(&RequestLabels {
                method: "GET".to_string(), endpoint: "object_stream".to_string(), status: "404".to_string(),
            }).inc();
            return Err((StatusCode::NOT_FOUND, format!("could not get object {}: {}", object_name, err)));
        }
    };

    let chunks = get_object_resp.into_inner().chunks;
    let span = tracing::Span::current();
    span.record("chunks_count", chunks.len());

    // Build a stream that yields chunk data sequentially
    let state_clone = state.clone();
    let stream = async_stream::stream! {
        for chunk in chunks {
            let mut read_ok = false;
            for storage_node in &chunk.storage_nodes {
                let addr = storage_node.address.clone();
                let client = match get_or_connect(&state_clone, &addr).await {
                    Ok(c) => c,
                    Err(_) => continue,
                };
                match retry_with_backoff(2, || {
                    let mut c = client.clone();
                    async move { c.read_chunk(ReadChunkRequest { chunk_id: chunk.id }).await }
                }).await {
                    Ok(resp) => {
                        yield Ok::<Vec<u8>, std::io::Error>(resp.into_inner().chunk_bytes);
                        read_ok = true;
                        break;
                    }
                    Err(_) => continue,
                }
            }
            if !read_ok {
                yield Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("could not read chunk {} from any storage node", chunk.id),
                ));
                return;
            }
        }
    };

    let body = Body::from_stream(stream);

    state.metrics.objects_downloaded_total.inc();
    state.metrics.http_requests_total.get_or_create(&RequestLabels {
        method: "GET".to_string(), endpoint: "object_stream".to_string(), status: "200".to_string(),
    }).inc();
    state.metrics.http_request_duration_seconds.get_or_create(&OperationLabels {
        operation: "get_object_stream".to_string(),
    }).observe(start.elapsed().as_secs_f64());

    let response = axum::response::Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(body)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("failed to build response: {}", e)))?;

    Ok(response)
}

/// Dispatches PUT requests: multipart → streaming handler, JSON → legacy handler.
pub async fn handle_put_dispatch<M, F>(
    path: Path<String>,
    State(state): State<Arc<AppState<M, F>>>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Result<axum::response::Response, (StatusCode, String)>
where
    M: MetadataClient + 'static,
    F: StorageNodeClientFactory + 'static,
{
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_type.starts_with("multipart/form-data") {
        // Reconstruct a request for axum's Multipart extractor
        let mut request = axum::http::Request::new(body);
        *request.headers_mut() = headers;
        let multipart = match Multipart::from_request(request, &()).await {
            Ok(m) => m,
            Err(e) => return Err((StatusCode::BAD_REQUEST, format!("invalid multipart request: {}", e))),
        };
        let result = handle_put_object_multipart(path, State(state), multipart).await;
        match result {
            Ok(json) => Ok(json.into_response()),
            Err(e) => Err(e),
        }
    } else {
        // JSON path - parse body manually
        let bytes = match axum::body::to_bytes(body, 1_500_000_000).await {
            Ok(b) => b,
            Err(e) => return Err((StatusCode::BAD_REQUEST, format!("failed to read body: {}", e))),
        };
        let payload: PutObjectRequestPayload = match serde_json::from_slice(&bytes) {
            Ok(p) => p,
            Err(e) => return Err((StatusCode::BAD_REQUEST, format!("invalid JSON: {}", e))),
        };
        let result = handle_put_object(path, State(state), Json(payload)).await;
        match result {
            Ok(json) => Ok(json.into_response()),
            Err(e) => Err(e),
        }
    }
}

/// Dispatches GET requests: Accept: application/json → legacy JSON handler, otherwise → streaming binary.
pub async fn handle_get_dispatch<M, F>(
    path: Path<String>,
    State(state): State<Arc<AppState<M, F>>>,
    headers: HeaderMap,
) -> Result<axum::response::Response, (StatusCode, String)>
where
    M: MetadataClient + 'static,
    F: StorageNodeClientFactory + 'static,
{
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if accept.contains("application/json") {
        let result = handle_get_object(path, State(state)).await;
        match result {
            Ok(json) => Ok(json.into_response()),
            Err(e) => Err(e),
        }
    } else {
        handle_get_object_stream(path, State(state)).await
    }
}

pub async fn handle_metrics<M, F>(
    axum::extract::State(state): axum::extract::State<Arc<AppState<M, F>>>,
) -> impl IntoResponse
where
    M: MetadataClient,
    F: StorageNodeClientFactory,
{
    let registry = match state.registry.lock() {
        Ok(r) => r,
        Err(poisoned) => poisoned.into_inner(),
    };
    let body = metrics::encode_metrics(&registry);
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===================
    // Checksum Tests
    // ===================

    #[test]
    fn test_compute_checksum_deterministic() {
        let data = b"hello world";
        let checksum1 = compute_checksum(data);
        let checksum2 = compute_checksum(data);
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_compute_checksum_different_data_produces_different_checksum() {
        let checksum1 = compute_checksum(b"hello");
        let checksum2 = compute_checksum(b"world");
        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_compute_checksum_empty_data() {
        let checksum = compute_checksum(b"");
        assert_eq!(checksum, 0); // CRC32 of empty is 0
    }

    #[test]
    fn test_compute_checksum_large_data() {
        let data = vec![0xAB; 1024 * 1024]; // 1MB
        let checksum = compute_checksum(&data);
        // Just verify it completes and returns consistent value
        assert_eq!(checksum, compute_checksum(&data));
    }

    // ===================
    // Request Payload Tests
    // ===================

    #[test]
    fn test_put_request_payload_deserialize() {
        let json = r#"{"bytes": "aGVsbG8gd29ybGQ="}"#;
        let payload: PutObjectRequestPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.bytes, "aGVsbG8gd29ybGQ=");
    }

    #[test]
    fn test_put_request_payload_missing_bytes_fails() {
        let json = r#"{}"#;
        let result: Result<PutObjectRequestPayload, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_put_request_payload_extra_fields_ignored() {
        let json = r#"{"bytes": "dGVzdA==", "extra": "ignored"}"#;
        let payload: PutObjectRequestPayload = serde_json::from_str(json).unwrap();
        assert_eq!(payload.bytes, "dGVzdA==");
    }

    // ===================
    // Response Payload Tests
    // ===================

    #[test]
    fn test_put_response_payload_serialize() {
        let payload = PutObjectResponsePayload {
            msg: "upload complete".to_string(),
            decoded_bytes_len: 1024,
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"msg\":\"upload complete\""));
        assert!(json.contains("\"decoded_bytes_len\":1024"));
    }

    #[test]
    fn test_get_response_payload_serialize() {
        let payload = GetObjectResponsePayload {
            bytes: "aGVsbG8=".to_string(),
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"bytes\":\"aGVsbG8=\""));
    }

    #[test]
    fn test_delete_response_payload_serialize() {
        let payload = DeleteObjectResponsePayload {
            msg: "object deleted".to_string(),
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"msg\":\"object deleted\""));
    }

    #[test]
    fn test_list_objects_response_chunk_serialize() {
        let chunk = ListObjectsResponseChunk {
            id: 12345,
            bytes: "Y2h1bmsgZGF0YQ==".to_string(),
            node_address: "127.0.0.1:3000".to_string(),
        };
        let json = serde_json::to_string(&chunk).unwrap();
        assert!(json.contains("\"id\":12345"));
        assert!(json.contains("\"bytes\":\"Y2h1bmsgZGF0YQ==\""));
        assert!(json.contains("\"node_address\":\"127.0.0.1:3000\""));
    }

    #[test]
    fn test_list_objects_response_payload_serialize() {
        let payload = ListObjectsResponsePayload {
            name: "myobject".to_string(),
            chunks: vec![
                ListObjectsResponseChunk {
                    id: 1,
                    bytes: "YWJj".to_string(),
                    node_address: "127.0.0.1:3000".to_string(),
                },
                ListObjectsResponseChunk {
                    id: 2,
                    bytes: "ZGVm".to_string(),
                    node_address: "127.0.0.1:3001".to_string(),
                },
            ],
        };
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"name\":\"myobject\""));
        assert!(json.contains("\"chunks\":["));
    }

    // ===================
    // Input Validation Tests
    // ===================

    #[test]
    fn test_validate_object_name_valid() {
        assert!(validate_object_name("myobject").is_ok());
        assert!(validate_object_name("my-object").is_ok());
        assert!(validate_object_name("my_object").is_ok());
        assert!(validate_object_name("my.object").is_ok());
        assert!(validate_object_name("path/to/object").is_ok());
        assert!(validate_object_name("a").is_ok());
    }

    #[test]
    fn test_validate_object_name_empty() {
        assert!(validate_object_name("").is_err());
    }

    #[test]
    fn test_validate_object_name_too_long() {
        let long_name = "a".repeat(MAX_OBJECT_NAME_LENGTH + 1);
        assert!(validate_object_name(&long_name).is_err());

        let max_name = "a".repeat(MAX_OBJECT_NAME_LENGTH);
        assert!(validate_object_name(&max_name).is_ok());
    }

    #[test]
    fn test_validate_object_name_invalid_characters() {
        assert!(validate_object_name("my object").is_err());
        assert!(validate_object_name("my@object").is_err());
        assert!(validate_object_name("my#object").is_err());
        assert!(validate_object_name("my object!").is_err());
    }

    #[test]
    fn test_validate_object_name_slash_boundaries() {
        assert!(validate_object_name("/leading").is_err());
        assert!(validate_object_name("trailing/").is_err());
        assert!(validate_object_name("double//slash").is_err());
    }

    // ===================
    // Base64 Encoding/Decoding Integration Tests
    // ===================

    #[test]
    fn test_base64_roundtrip_with_payload() {
        // Simulate what the handler does
        let original_data = b"hello world";
        let encoded = general_purpose::STANDARD.encode(original_data);

        let payload = PutObjectRequestPayload {
            bytes: encoded.clone(),
        };

        // Deserialize and decode
        let decoded = general_purpose::STANDARD.decode(&payload.bytes).unwrap();
        assert_eq!(decoded, original_data);
    }

    #[test]
    fn test_base64_with_binary_data() {
        // Test with non-UTF8 binary data
        let binary_data: Vec<u8> = (0..=255).collect();
        let encoded = general_purpose::STANDARD.encode(&binary_data);

        let payload = PutObjectRequestPayload { bytes: encoded };
        let decoded = general_purpose::STANDARD.decode(&payload.bytes).unwrap();

        assert_eq!(decoded, binary_data);
    }

    #[test]
    fn test_invalid_base64_fails_decode() {
        let payload = PutObjectRequestPayload {
            bytes: "not valid base64!!!".to_string(),
        };
        let result = general_purpose::STANDARD.decode(&payload.bytes);
        assert!(result.is_err());
    }

    // ===================
    // Data Chunking Logic Tests
    // ===================

    #[test]
    fn test_chunking_small_data_single_chunk() {
        let data = b"small data";
        let chunk_size = 1024;

        let chunks: Vec<Vec<u8>> = data
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], data);
    }

    #[test]
    fn test_chunking_exact_multiple() {
        let data = vec![0u8; 1024]; // Exactly 1024 bytes
        let chunk_size = 512;

        let chunks: Vec<Vec<u8>> = data
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 512);
        assert_eq!(chunks[1].len(), 512);
    }

    #[test]
    fn test_chunking_with_remainder() {
        let data = vec![0u8; 1000];
        let chunk_size = 300;

        let chunks: Vec<Vec<u8>> = data
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].len(), 300);
        assert_eq!(chunks[1].len(), 300);
        assert_eq!(chunks[2].len(), 300);
        assert_eq!(chunks[3].len(), 100); // Remainder
    }

    #[test]
    fn test_chunking_reassembly() {
        let original = b"the quick brown fox jumps over the lazy dog";
        let chunk_size = 10;

        let chunks: Vec<Vec<u8>> = original
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        // Reassemble
        let mut reassembled = Vec::new();
        for chunk in chunks {
            reassembled.extend_from_slice(&chunk);
        }

        assert_eq!(reassembled, original);
    }

    #[test]
    fn test_checksum_consistent_across_chunking() {
        let data = b"test data for checksum verification";
        let original_checksum = compute_checksum(data);

        // Chunk and compute individual checksums
        let chunks: Vec<Vec<u8>> = data.chunks(10).map(|c| c.to_vec()).collect();
        let chunk_checksums: Vec<u32> = chunks.iter().map(|c| compute_checksum(c)).collect();

        // Reassemble and verify
        let mut reassembled = Vec::new();
        for chunk in &chunks {
            reassembled.extend_from_slice(chunk);
        }
        let reassembled_checksum = compute_checksum(&reassembled);

        assert_eq!(original_checksum, reassembled_checksum);

        // Individual chunk checksums should be different from whole
        for chunk_checksum in chunk_checksums {
            assert_ne!(chunk_checksum, original_checksum);
        }
    }
}

#[cfg(test)]
mod handler_tests {
    use super::*;
    use crate::clients::mocks::{MockMetadataClient, MockStorageNodeClientFactory};
    use crate::metrics::Metrics;
    use axum::extract::Path;
    use prometheus_client::registry::Registry;
    use std::sync::Mutex;

    type TestAppState = AppState<MockMetadataClient, MockStorageNodeClientFactory>;

    fn create_test_state() -> Arc<TestAppState> {
        let mut registry = Registry::default();
        let metrics = Arc::new(Metrics::new(&mut registry));

        Arc::new(AppState {
            metadata_client: MockMetadataClient::default(),
            storage_factory: MockStorageNodeClientFactory::default(),
            metrics,
            registry: Arc::new(Mutex::new(registry)),
            storage_connections: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        })
    }

    // ===================
    // PUT Handler Tests
    // ===================

    #[tokio::test]
    async fn test_put_object_success() {
        let state = create_test_state();
        let object_name = "test-object".to_string();
        let data = b"hello world";
        let encoded = general_purpose::STANDARD.encode(data);

        let result = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name.clone()),
            State(state.clone()),
            Json(PutObjectRequestPayload { bytes: encoded }),
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.msg, "upload complete");
        assert_eq!(response.decoded_bytes_len, data.len());

        // Verify object was stored in mock metadata
        let objects = state.metadata_client.objects.lock().unwrap();
        assert!(objects.contains_key("test-object"));
    }

    #[tokio::test]
    async fn test_put_object_invalid_base64() {
        let state = create_test_state();
        let object_name = "test-object".to_string();

        let result = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name),
            State(state),
            Json(PutObjectRequestPayload {
                bytes: "not valid base64!!!".to_string(),
            }),
        )
        .await;

        assert!(result.is_err());
        let (status, msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("invalid base64"));
    }

    #[tokio::test]
    async fn test_put_object_no_storage_nodes() {
        let state = create_test_state();
        // Clear storage nodes
        state.metadata_client.storage_nodes.lock().unwrap().clear();

        let object_name = "test-object".to_string();
        let encoded = general_purpose::STANDARD.encode(b"data");

        let result = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name),
            State(state),
            Json(PutObjectRequestPayload { bytes: encoded }),
        )
        .await;

        assert!(result.is_err());
        let (status, msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(msg.contains("no storage nodes"));
    }

    #[tokio::test]
    async fn test_put_object_duplicate_name() {
        let state = create_test_state();
        let object_name = "duplicate-object".to_string();
        let encoded = general_purpose::STANDARD.encode(b"data");

        // First PUT should succeed
        let result1 = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name.clone()),
            State(state.clone()),
            Json(PutObjectRequestPayload {
                bytes: encoded.clone(),
            }),
        )
        .await;
        assert!(result1.is_ok());

        // Second PUT with same name should fail
        let result2 = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name),
            State(state),
            Json(PutObjectRequestPayload { bytes: encoded }),
        )
        .await;

        assert!(result2.is_err());
        let (status, _) = result2.unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
    }

    // ===================
    // GET Handler Tests
    // ===================

    #[tokio::test]
    async fn test_get_object_success() {
        let state = create_test_state();
        let object_name = "get-test-object".to_string();
        let data = b"hello world";
        let encoded = general_purpose::STANDARD.encode(data);

        // First, PUT the object
        let put_result = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name.clone()),
            State(state.clone()),
            Json(PutObjectRequestPayload {
                bytes: encoded.clone(),
            }),
        )
        .await;
        assert!(put_result.is_ok());

        // Now GET it
        let get_result = handle_get_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name),
            State(state),
        )
        .await;

        assert!(get_result.is_ok());
        let response = get_result.unwrap();
        // Decode and verify the data matches
        let decoded = general_purpose::STANDARD.decode(&response.bytes).unwrap();
        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn test_get_object_not_found() {
        let state = create_test_state();
        let object_name = "nonexistent-object".to_string();

        let result = handle_get_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name),
            State(state),
        )
        .await;

        assert!(result.is_err());
        let (status, _) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    // ===================
    // DELETE Handler Tests
    // ===================

    #[tokio::test]
    async fn test_delete_object_success() {
        let state = create_test_state();
        let object_name = "delete-test-object".to_string();
        let encoded = general_purpose::STANDARD.encode(b"data to delete");

        // First, PUT the object
        let put_result = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name.clone()),
            State(state.clone()),
            Json(PutObjectRequestPayload { bytes: encoded }),
        )
        .await;
        assert!(put_result.is_ok());

        // Delete it
        let delete_result =
            handle_delete_object::<MockMetadataClient, MockStorageNodeClientFactory>(
                Path(object_name.clone()),
                State(state.clone()),
            )
            .await;

        assert!(delete_result.is_ok());
        let response = delete_result.unwrap();
        assert_eq!(response.msg, "object deleted");

        // Verify object is gone
        let objects = state.metadata_client.objects.lock().unwrap();
        assert!(!objects.contains_key(&object_name));
    }

    #[tokio::test]
    async fn test_delete_object_not_found() {
        let state = create_test_state();
        let object_name = "nonexistent-delete-object".to_string();

        let result = handle_delete_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name),
            State(state),
        )
        .await;

        assert!(result.is_err());
        let (status, _) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    // ===================
    // LIST Handler Tests
    // ===================

    #[tokio::test]
    async fn test_list_objects_empty() {
        let state = create_test_state();

        let result = handle_list_objects::<MockMetadataClient, MockStorageNodeClientFactory>(
            State(state),
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.is_empty());
    }

    #[tokio::test]
    async fn test_list_objects_with_objects() {
        let state = create_test_state();

        // Create two objects
        let _ = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path("object1".to_string()),
            State(state.clone()),
            Json(PutObjectRequestPayload {
                bytes: general_purpose::STANDARD.encode(b"data1"),
            }),
        )
        .await;

        let _ = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path("object2".to_string()),
            State(state.clone()),
            Json(PutObjectRequestPayload {
                bytes: general_purpose::STANDARD.encode(b"data2"),
            }),
        )
        .await;

        let result = handle_list_objects::<MockMetadataClient, MockStorageNodeClientFactory>(
            State(state),
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.len(), 2);

        let names: Vec<&str> = response.iter().map(|o| o.name.as_str()).collect();
        assert!(names.contains(&"object1"));
        assert!(names.contains(&"object2"));
    }

    // ===================
    // Roundtrip Tests
    // ===================

    #[tokio::test]
    async fn test_put_get_delete_roundtrip() {
        let state = create_test_state();
        let object_name = "roundtrip-object".to_string();
        let original_data = b"the quick brown fox jumps over the lazy dog";
        let encoded = general_purpose::STANDARD.encode(original_data);

        // PUT
        let put_result = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name.clone()),
            State(state.clone()),
            Json(PutObjectRequestPayload { bytes: encoded }),
        )
        .await;
        assert!(put_result.is_ok());

        // GET
        let get_result = handle_get_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name.clone()),
            State(state.clone()),
        )
        .await;
        assert!(get_result.is_ok());
        let retrieved = general_purpose::STANDARD
            .decode(&get_result.unwrap().bytes)
            .unwrap();
        assert_eq!(retrieved, original_data);

        // DELETE
        let delete_result =
            handle_delete_object::<MockMetadataClient, MockStorageNodeClientFactory>(
                Path(object_name.clone()),
                State(state.clone()),
            )
            .await;
        assert!(delete_result.is_ok());

        // GET should now fail
        let get_after_delete =
            handle_get_object::<MockMetadataClient, MockStorageNodeClientFactory>(
                Path(object_name),
                State(state),
            )
            .await;
        assert!(get_after_delete.is_err());
    }

    // ===================
    // Streaming GET Tests
    // ===================

    #[tokio::test]
    async fn test_get_object_stream_success() {
        let state = create_test_state();
        let object_name = "stream-get-test".to_string();
        let data = b"hello streaming world";
        let encoded = general_purpose::STANDARD.encode(data);

        // PUT via JSON handler
        let put_result = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name.clone()),
            State(state.clone()),
            Json(PutObjectRequestPayload { bytes: encoded }),
        )
        .await;
        assert!(put_result.is_ok());

        // GET via streaming handler
        let get_result = handle_get_object_stream::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name),
            State(state),
        )
        .await;

        assert!(get_result.is_ok());
        let response = get_result.unwrap();
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/octet-stream"
        );

        // Read the body
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(body_bytes.as_ref(), data);
    }

    #[tokio::test]
    async fn test_get_object_stream_not_found() {
        let state = create_test_state();

        let result = handle_get_object_stream::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path("nonexistent".to_string()),
            State(state),
        )
        .await;

        assert!(result.is_err());
        let (status, _) = result.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_object_stream_multi_chunk() {
        let state = create_test_state();

        let object_name = "multi-chunk-stream".to_string();
        let data = vec![0xABu8; 1024]; // 1KB object
        let encoded = general_purpose::STANDARD.encode(&data);

        let put_result = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name.clone()),
            State(state.clone()),
            Json(PutObjectRequestPayload { bytes: encoded }),
        )
        .await;
        assert!(put_result.is_ok());

        let get_result = handle_get_object_stream::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name),
            State(state),
        )
        .await;

        assert!(get_result.is_ok());
        let body_bytes = axum::body::to_bytes(get_result.unwrap().into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(body_bytes.as_ref(), data.as_slice());
    }

    // ===================
    // Dispatch Handler Tests
    // ===================

    #[tokio::test]
    async fn test_get_dispatch_json_accept() {
        let state = create_test_state();
        let object_name = "dispatch-get-json".to_string();
        let data = b"dispatch test data";
        let encoded = general_purpose::STANDARD.encode(data);

        // PUT first
        let _ = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name.clone()),
            State(state.clone()),
            Json(PutObjectRequestPayload { bytes: encoded }),
        )
        .await
        .unwrap();

        // GET with Accept: application/json should return JSON response
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, "application/json".parse().unwrap());

        let result = handle_get_dispatch::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name),
            State(state),
            headers,
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        // JSON response contains base64-encoded bytes field
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body_str = std::str::from_utf8(&body_bytes).unwrap();
        assert!(body_str.contains("bytes"));
    }

    #[tokio::test]
    async fn test_get_dispatch_stream_default() {
        let state = create_test_state();
        let object_name = "dispatch-get-stream".to_string();
        let data = b"stream dispatch test";
        let encoded = general_purpose::STANDARD.encode(data);

        // PUT first
        let _ = handle_put_object::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name.clone()),
            State(state.clone()),
            Json(PutObjectRequestPayload { bytes: encoded }),
        )
        .await
        .unwrap();

        // GET without Accept header should return streaming binary
        let headers = HeaderMap::new();

        let result = handle_get_dispatch::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name),
            State(state),
            headers,
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(body_bytes.as_ref(), data);
    }

    #[tokio::test]
    async fn test_put_dispatch_json_content_type() {
        let state = create_test_state();
        let object_name = "dispatch-put-json".to_string();
        let data = b"json upload";
        let encoded = general_purpose::STANDARD.encode(data);
        let json_body = serde_json::json!({"bytes": encoded}).to_string();

        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

        let body = Body::from(json_body);

        let result = handle_put_dispatch::<MockMetadataClient, MockStorageNodeClientFactory>(
            Path(object_name.clone()),
            State(state.clone()),
            headers,
            body,
        )
        .await;

        assert!(result.is_ok());

        // Verify the object was stored
        let objects = state.metadata_client.objects.lock().unwrap();
        assert!(objects.contains_key(&object_name));
    }
}
