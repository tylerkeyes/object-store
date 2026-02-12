use std::sync::Arc;

use tokio::sync::Mutex as TokioMutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::chunk_index::ChunkIndex;
use crate::metadata_store::MetadataStore;
use storage_proto_lib::storage_metadata::{
    self, AddChunkReplicaRequest, AddChunkReplicaResponse, AllocateChunkRequest,
    AllocateChunkResponse, ChunkIndexUpdate, ChunkIndexUpdateResponse, ConfirmChunkRequest,
    ConfirmChunkResponse, DeleteChunkRequest, DeleteChunkResponse, DeleteObjectRequest,
    DeleteObjectResponse, DeleteStorageNodeRequest, DeleteStorageNodeResponse, GetChunkRequest,
    GetChunkResponse, GetObjectRequest, GetObjectResponse, ListObjectsRequest, ListObjectsResponse,
    ListUnderReplicatedChunksRequest, ListUnderReplicatedChunksResponse, PutChunkRequest,
    PutChunkResponse, PutObjectRequest, PutObjectResponse, PutStorageNodeRequest,
    PutStorageNodeResponse, TransferPartitionEntry, TransferPartitionRequest, UnderReplicatedChunk,
    metadata_cluster_service_server::MetadataClusterService,
};

/// Internal gRPC service for inter-node cluster communication.
/// Forward* RPCs are called when a peer has determined we are the owner.
/// Broadcast* RPCs are called to replicate global state changes.
/// ListLocal* RPCs are called for scatter-gather aggregation.
pub struct MetadataClusterServiceImpl {
    pub metadata_store: Arc<TokioMutex<MetadataStore>>,
    pub chunk_index: Arc<ChunkIndex>,
}

#[tonic::async_trait]
impl MetadataClusterService for MetadataClusterServiceImpl {
    // ===== Forward RPCs: handle locally (we are the owner) =====

    async fn forward_put_object(
        &self,
        request: Request<PutObjectRequest>,
    ) -> Result<Response<PutObjectResponse>, Status> {
        let req = request.get_ref();
        self.metadata_store
            .lock()
            .await
            .put_object(req.checksum, req.name.clone())
            .map_err(|e| Status::internal(format!("put_object failed: {}", e)))?;
        Ok(Response::new(PutObjectResponse {}))
    }

    async fn forward_get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>, Status> {
        let req = request.get_ref();
        let store = self.metadata_store.lock().await;
        let object = store
            .get_object(req.object_name.clone())
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => Status::not_found("object not found"),
                _ => Status::internal("failed to get object"),
            })?
            .clone();

        let mut response_chunks = Vec::with_capacity(object.chunks.len());
        for chunk_id in &object.chunks {
            let chunk = store.chunks.get(chunk_id).ok_or_else(|| {
                Status::internal(format!("chunk {} referenced but not found", chunk_id))
            })?;
            let mut storage_nodes = Vec::new();
            for node_id in &chunk.storage_nodes {
                if let Some(node) = store.storage_nodes.get(node_id) {
                    storage_nodes.push(storage_metadata::MetaStorageNode {
                        id: *node_id,
                        address: node.address.clone(),
                    });
                }
            }
            response_chunks.push(storage_metadata::MetaChunk {
                id: chunk.id,
                checksum: chunk.checksum,
                storage_nodes,
            });
        }

        Ok(Response::new(GetObjectResponse {
            checksum: object.checksum,
            chunks: response_chunks,
        }))
    }

    async fn forward_delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let req = request.get_ref();
        // Get chunk IDs before deletion for chunk index cleanup
        let chunk_ids = {
            let store = self.metadata_store.lock().await;
            match store.get_object(req.object_name.clone()) {
                Ok(obj) => obj.chunks.clone(),
                Err(_) => Vec::new(),
            }
        };

        self.metadata_store
            .lock()
            .await
            .delete_object(req.object_name.clone())
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => Status::not_found("object not found"),
                _ => Status::internal("failed to delete object"),
            })?;

        // Clean up chunk index
        for chunk_id in chunk_ids {
            self.chunk_index.remove(chunk_id).await;
        }

        Ok(Response::new(DeleteObjectResponse {}))
    }

    async fn forward_put_chunk(
        &self,
        request: Request<PutChunkRequest>,
    ) -> Result<Response<PutChunkResponse>, Status> {
        let req = request.get_ref();
        let (chunk_id, storage_node_address) = self
            .metadata_store
            .lock()
            .await
            .put_chunk(req.object_name.clone(), req.checksum)
            .map_err(|e| Status::internal(format!("put_chunk failed: {}", e)))?;

        // Update chunk index
        self.chunk_index
            .insert(chunk_id, req.object_name.clone())
            .await;

        Ok(Response::new(PutChunkResponse {
            chunk_id,
            storage_node_address,
        }))
    }

    async fn forward_allocate_chunk(
        &self,
        _request: Request<AllocateChunkRequest>,
    ) -> Result<Response<AllocateChunkResponse>, Status> {
        let mut store = self.metadata_store.lock().await;
        let object_name = &_request.get_ref().object_name;
        let (chunk_id, storage_node_id, storage_node_address) = store
            .allocate_chunk(object_name)
            .map_err(|e| Status::unavailable(format!("{}", e)))?;

        Ok(Response::new(AllocateChunkResponse {
            chunk_id,
            storage_node_address,
            storage_node_id,
        }))
    }

    async fn forward_confirm_chunk(
        &self,
        request: Request<ConfirmChunkRequest>,
    ) -> Result<Response<ConfirmChunkResponse>, Status> {
        let req = request.get_ref();
        self.metadata_store
            .lock()
            .await
            .confirm_chunk(
                req.chunk_id,
                req.object_name.clone(),
                req.checksum,
                req.storage_node_id,
            )
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => Status::not_found(format!("{}", e)),
                std::io::ErrorKind::AlreadyExists => Status::already_exists(format!("{}", e)),
                _ => Status::internal("failed to confirm chunk"),
            })?;

        // Update chunk index
        self.chunk_index
            .insert(req.chunk_id, req.object_name.clone())
            .await;

        Ok(Response::new(ConfirmChunkResponse {}))
    }

    async fn forward_get_chunk(
        &self,
        request: Request<GetChunkRequest>,
    ) -> Result<Response<GetChunkResponse>, Status> {
        let req = request.get_ref();
        let store = self.metadata_store.lock().await;
        let (object_name, chunk) = store
            .get_chunk(req.chunk_id)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => Status::not_found("chunk not found"),
                _ => Status::internal("failed to get chunk"),
            })?;

        let mut storage_nodes = Vec::new();
        for node_id in &chunk.storage_nodes {
            if let Some(node) = store.storage_nodes.get(node_id) {
                storage_nodes.push(storage_metadata::MetaStorageNode {
                    id: *node_id,
                    address: node.address.clone(),
                });
            }
        }

        Ok(Response::new(GetChunkResponse {
            object_name,
            checksum: chunk.checksum,
            storage_nodes,
        }))
    }

    async fn forward_delete_chunk(
        &self,
        request: Request<DeleteChunkRequest>,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        let req = request.get_ref();
        self.metadata_store
            .lock()
            .await
            .delete_chunk(req.chunk_id)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => Status::not_found("chunk not found"),
                _ => Status::internal("failed to delete chunk"),
            })?;

        self.chunk_index.remove(req.chunk_id).await;

        Ok(Response::new(DeleteChunkResponse {}))
    }

    async fn forward_add_chunk_replica(
        &self,
        request: Request<AddChunkReplicaRequest>,
    ) -> Result<Response<AddChunkReplicaResponse>, Status> {
        let req = request.get_ref();
        self.metadata_store
            .lock()
            .await
            .add_chunk_replica(req.chunk_id, req.storage_node_id)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => Status::not_found(format!("{}", e)),
                std::io::ErrorKind::AlreadyExists => Status::already_exists(format!("{}", e)),
                std::io::ErrorKind::InvalidInput => {
                    Status::failed_precondition(format!("{}", e))
                }
                _ => Status::internal("failed to add chunk replica"),
            })?;

        Ok(Response::new(AddChunkReplicaResponse {}))
    }

    // ===== Broadcast RPCs: apply locally, do NOT re-broadcast =====

    async fn broadcast_put_storage_node(
        &self,
        request: Request<PutStorageNodeRequest>,
    ) -> Result<Response<PutStorageNodeResponse>, Status> {
        let req = request.get_ref();
        let id = self
            .metadata_store
            .lock()
            .await
            .construct_storage_node(req.address.clone())
            .map_err(|e| Status::internal(format!("put_storage_node failed: {}", e)))?;

        Ok(Response::new(PutStorageNodeResponse { id }))
    }

    async fn broadcast_delete_storage_node(
        &self,
        request: Request<DeleteStorageNodeRequest>,
    ) -> Result<Response<DeleteStorageNodeResponse>, Status> {
        let req = request.get_ref();
        self.metadata_store
            .lock()
            .await
            .delete_storage_node(req.id)
            .map_err(|e| Status::internal(format!("delete_storage_node failed: {}", e)))?;

        Ok(Response::new(DeleteStorageNodeResponse {}))
    }

    // ===== ListLocal RPCs: return only locally-owned data =====

    async fn list_local_objects(
        &self,
        _request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let store = self.metadata_store.lock().await;
        let objects = store.list_objects();

        let mut response_objects = Vec::with_capacity(objects.len());
        for obj in &objects {
            let mut response_chunks = Vec::with_capacity(obj.chunks.len());
            for chunk_id in &obj.chunks {
                if let Some(chunk) = store.chunks.get(chunk_id) {
                    let mut storage_nodes = Vec::new();
                    for node_id in &chunk.storage_nodes {
                        if let Some(node) = store.storage_nodes.get(node_id) {
                            storage_nodes.push(storage_metadata::MetaStorageNode {
                                id: *node_id,
                                address: node.address.clone(),
                            });
                        }
                    }
                    response_chunks.push(storage_metadata::MetaChunk {
                        id: chunk.id,
                        checksum: chunk.checksum,
                        storage_nodes,
                    });
                }
            }
            response_objects.push(storage_metadata::MetaObject {
                name: obj.name.clone(),
                checksum: obj.checksum,
                chunks: response_chunks,
                total_size: obj.total_size,
            });
        }

        Ok(Response::new(ListObjectsResponse {
            objects: response_objects,
        }))
    }

    async fn list_local_under_replicated_chunks(
        &self,
        request: Request<ListUnderReplicatedChunksRequest>,
    ) -> Result<Response<ListUnderReplicatedChunksResponse>, Status> {
        let max_replicas = request.get_ref().max_replicas;
        let max_replicas = if max_replicas == 0 { 3 } else { max_replicas };

        let store = self.metadata_store.lock().await;
        let chunks = store.list_under_replicated_chunks(max_replicas);

        Ok(Response::new(ListUnderReplicatedChunksResponse {
            chunks: chunks
                .iter()
                .map(|chunk| UnderReplicatedChunk {
                    chunk_id: chunk.id,
                    checksum: chunk.checksum,
                    current_storage_node_ids: chunk.storage_nodes.clone(),
                })
                .collect(),
        }))
    }

    // ===== Migration =====

    type TransferPartitionStream = ReceiverStream<Result<TransferPartitionEntry, Status>>;

    async fn transfer_partition(
        &self,
        request: Request<TransferPartitionRequest>,
    ) -> Result<Response<Self::TransferPartitionStream>, Status> {
        let ranges: Vec<(u64, u64)> = request
            .get_ref()
            .ranges
            .iter()
            .map(|r| (r.start, r.end))
            .collect();

        let store = self.metadata_store.lock().await;
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        // Collect objects in the requested hash ranges
        let objects_to_transfer = store.objects_in_hash_ranges(&ranges);

        // Stream objects and their chunks
        tokio::spawn(async move {
            for (obj, chunks) in objects_to_transfer {
                // Send the object
                let entry = TransferPartitionEntry {
                    entry: Some(
                        storage_metadata::transfer_partition_entry::Entry::Object(
                            storage_metadata::TransferObject {
                                name: obj.name.clone(),
                                checksum: obj.checksum,
                                chunk_ids: obj.chunks.clone(),
                            },
                        ),
                    ),
                };
                if tx.send(Ok(entry)).await.is_err() {
                    return;
                }

                // Send each chunk
                for chunk in chunks {
                    let entry = TransferPartitionEntry {
                        entry: Some(
                            storage_metadata::transfer_partition_entry::Entry::Chunk(
                                storage_metadata::TransferChunk {
                                    id: chunk.id,
                                    object_name: chunk.object_name.clone(),
                                    checksum: chunk.checksum,
                                    storage_node_ids: chunk.storage_nodes.clone(),
                                },
                            ),
                        ),
                    };
                    if tx.send(Ok(entry)).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // ===== Chunk Index Synchronization =====

    async fn broadcast_chunk_index(
        &self,
        request: Request<ChunkIndexUpdate>,
    ) -> Result<Response<ChunkIndexUpdateResponse>, Status> {
        let entries: Vec<(u64, String)> = request
            .get_ref()
            .entries
            .iter()
            .map(|e| (e.chunk_id, e.object_name.clone()))
            .collect();

        self.chunk_index.bulk_insert(entries).await;

        Ok(Response::new(ChunkIndexUpdateResponse {}))
    }
}
