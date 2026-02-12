//! Client implementations for gRPC services.
//!
//! This module provides:
//! - Real client wrappers that delegate to tonic-generated clients
//! - Mock implementations for testing (behind #[cfg(test)])

use async_trait::async_trait;
use storage_proto_lib::storage_metadata::storage_metadata_service_client::StorageMetadataServiceClient;
use storage_proto_lib::storage_metadata::{
    AddChunkReplicaRequest, AddChunkReplicaResponse,
    AllocateChunkRequest, AllocateChunkResponse, ConfirmChunkRequest, ConfirmChunkResponse,
    DeleteChunkRequest as MetadataDeleteChunkRequest,
    DeleteChunkResponse as MetadataDeleteChunkResponse,
    DeleteObjectRequest, DeleteObjectResponse,
    DeleteStorageNodeRequest, DeleteStorageNodeResponse,
    GetChunkRequest, GetChunkResponse,
    GetObjectRequest, GetObjectResponse,
    GetStorageNodeRequest, GetStorageNodeResponse,
    ListObjectsRequest, ListObjectsResponse,
    ListStorageNodesRequest, ListStorageNodesResponse,
    ListUnderReplicatedChunksRequest, ListUnderReplicatedChunksResponse,
    PutChunkRequest, PutChunkResponse,
    PutObjectRequest, PutObjectResponse,
    PutStorageNodeRequest, PutStorageNodeResponse,
    UpdateObjectChecksumRequest, UpdateObjectChecksumResponse,
};
use storage_proto_lib::storage_node::storage_node_service_client::StorageNodeServiceClient;
use storage_proto_lib::storage_node::{
    CheckHealthRequest, CheckHealthResponse,
    DeleteChunkRequest as StorageDeleteChunkRequest,
    DeleteChunkResponse as StorageDeleteChunkResponse,
    GetChunkSizeRequest, GetChunkSizeResponse,
    GetFreeChunksRequest, GetFreeChunksResponse,
    ReadChunkRequest, ReadChunkResponse, WriteChunkRequest, WriteChunkResponse,
};
use storage_proto_lib::{MetadataClient, StorageNodeClient, StorageNodeClientFactory};
use tonic::transport::Channel;
use tonic::{Response, Status};

/// Real metadata client wrapper that delegates to the tonic-generated client.
#[derive(Clone)]
pub struct RealMetadataClient {
    inner: StorageMetadataServiceClient<Channel>,
}

impl RealMetadataClient {
    pub fn new(client: StorageMetadataServiceClient<Channel>) -> Self {
        Self { inner: client }
    }
}

#[async_trait]
impl MetadataClient for RealMetadataClient {
    async fn list_storage_nodes(
        &mut self,
        request: ListStorageNodesRequest,
    ) -> Result<Response<ListStorageNodesResponse>, Status> {
        self.inner.list_storage_nodes(request).await
    }

    async fn put_object(
        &mut self,
        request: PutObjectRequest,
    ) -> Result<Response<PutObjectResponse>, Status> {
        self.inner.put_object(request).await
    }

    async fn get_object(
        &mut self,
        request: GetObjectRequest,
    ) -> Result<Response<GetObjectResponse>, Status> {
        self.inner.get_object(request).await
    }

    async fn delete_object(
        &mut self,
        request: DeleteObjectRequest,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        self.inner.delete_object(request).await
    }

    async fn allocate_chunk(
        &mut self,
        request: AllocateChunkRequest,
    ) -> Result<Response<AllocateChunkResponse>, Status> {
        self.inner.allocate_chunk(request).await
    }

    async fn confirm_chunk(
        &mut self,
        request: ConfirmChunkRequest,
    ) -> Result<Response<ConfirmChunkResponse>, Status> {
        self.inner.confirm_chunk(request).await
    }

    async fn list_objects(
        &mut self,
        request: ListObjectsRequest,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        self.inner.list_objects(request).await
    }

    async fn delete_chunk(
        &mut self,
        request: MetadataDeleteChunkRequest,
    ) -> Result<Response<MetadataDeleteChunkResponse>, Status> {
        self.inner.delete_chunk(request).await
    }

    async fn put_chunk(
        &mut self,
        request: PutChunkRequest,
    ) -> Result<Response<PutChunkResponse>, Status> {
        self.inner.put_chunk(request).await
    }

    async fn put_storage_node(
        &mut self,
        request: PutStorageNodeRequest,
    ) -> Result<Response<PutStorageNodeResponse>, Status> {
        self.inner.put_storage_node(request).await
    }

    async fn get_chunk(
        &mut self,
        request: GetChunkRequest,
    ) -> Result<Response<GetChunkResponse>, Status> {
        self.inner.get_chunk(request).await
    }

    async fn get_storage_node(
        &mut self,
        request: GetStorageNodeRequest,
    ) -> Result<Response<GetStorageNodeResponse>, Status> {
        self.inner.get_storage_node(request).await
    }

    async fn delete_storage_node(
        &mut self,
        request: DeleteStorageNodeRequest,
    ) -> Result<Response<DeleteStorageNodeResponse>, Status> {
        self.inner.delete_storage_node(request).await
    }

    async fn list_under_replicated_chunks(
        &mut self,
        request: ListUnderReplicatedChunksRequest,
    ) -> Result<Response<ListUnderReplicatedChunksResponse>, Status> {
        self.inner.list_under_replicated_chunks(request).await
    }

    async fn add_chunk_replica(
        &mut self,
        request: AddChunkReplicaRequest,
    ) -> Result<Response<AddChunkReplicaResponse>, Status> {
        self.inner.add_chunk_replica(request).await
    }

    async fn update_object_checksum(
        &mut self,
        request: UpdateObjectChecksumRequest,
    ) -> Result<Response<UpdateObjectChecksumResponse>, Status> {
        self.inner.update_object_checksum(request).await
    }
}

/// Real storage node client wrapper that delegates to the tonic-generated client.
#[derive(Clone)]
pub struct RealStorageNodeClient {
    inner: StorageNodeServiceClient<Channel>,
}

impl RealStorageNodeClient {
    pub fn new(client: StorageNodeServiceClient<Channel>) -> Self {
        Self { inner: client }
    }
}

#[async_trait]
impl StorageNodeClient for RealStorageNodeClient {
    async fn get_chunk_size(
        &mut self,
        request: GetChunkSizeRequest,
    ) -> Result<Response<GetChunkSizeResponse>, Status> {
        self.inner.get_chunk_size(request).await
    }

    async fn write_chunk(
        &mut self,
        request: WriteChunkRequest,
    ) -> Result<Response<WriteChunkResponse>, Status> {
        self.inner.write_chunk(request).await
    }

    async fn read_chunk(
        &mut self,
        request: ReadChunkRequest,
    ) -> Result<Response<ReadChunkResponse>, Status> {
        self.inner.read_chunk(request).await
    }

    async fn delete_chunk(
        &mut self,
        request: StorageDeleteChunkRequest,
    ) -> Result<Response<StorageDeleteChunkResponse>, Status> {
        self.inner.delete_chunk(request).await
    }

    async fn get_free_chunks(
        &mut self,
        request: GetFreeChunksRequest,
    ) -> Result<Response<GetFreeChunksResponse>, Status> {
        self.inner.get_free_chunks(request).await
    }

    async fn check_health(
        &mut self,
        request: CheckHealthRequest,
    ) -> Result<Response<CheckHealthResponse>, Status> {
        self.inner.check_health(request).await
    }
}

/// Factory that creates real storage node connections.
#[derive(Clone, Default)]
pub struct RealStorageNodeClientFactory;

const MAX_GRPC_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16 MiB for 8 MiB chunks + overhead

#[async_trait]
impl StorageNodeClientFactory for RealStorageNodeClientFactory {
    type Client = RealStorageNodeClient;

    async fn connect(&self, address: &str) -> Result<Self::Client, tonic::transport::Error> {
        let client = StorageNodeServiceClient::connect(address.to_string())
            .await?
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);
        Ok(RealStorageNodeClient::new(client))
    }
}

#[cfg(test)]
pub mod mocks {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use storage_proto_lib::storage_metadata::{
        GetChunkResponse, GetStorageNodeResponse,
        MetaChunk, MetaObject, MetaStorageNode,
    };

    /// Mock metadata client with configurable in-memory state.
    #[derive(Clone)]
    pub struct MockMetadataClient {
        /// Storage nodes available in the system
        pub storage_nodes: Arc<Mutex<Vec<MetaStorageNode>>>,
        /// Objects stored (by name)
        pub objects: Arc<Mutex<HashMap<String, StoredObject>>>,
        /// Chunks stored (by id)
        pub chunks: Arc<Mutex<HashMap<u64, StoredChunk>>>,
        /// Counter for generating chunk IDs
        pub next_chunk_id: Arc<Mutex<u64>>,
        /// Error injection flags
        pub fail_list_storage_nodes: bool,
        pub fail_put_object: bool,
        pub fail_allocate_chunk: bool,
    }

    #[derive(Clone)]
    pub struct StoredObject {
        pub checksum: u32,
        pub chunk_ids: Vec<u64>,
    }

    #[derive(Clone)]
    pub struct StoredChunk {
        pub object_name: String,
        pub checksum: u32,
        pub storage_node_ids: Vec<u64>,
    }

    impl Default for MockMetadataClient {
        fn default() -> Self {
            Self {
                storage_nodes: Arc::new(Mutex::new(vec![MetaStorageNode {
                    id: 1,
                    address: "http://mock-storage:3000".to_string(),
                }])),
                objects: Arc::new(Mutex::new(HashMap::new())),
                chunks: Arc::new(Mutex::new(HashMap::new())),
                next_chunk_id: Arc::new(Mutex::new(1)),
                fail_list_storage_nodes: false,
                fail_put_object: false,
                fail_allocate_chunk: false,
            }
        }
    }

    #[async_trait]
    impl MetadataClient for MockMetadataClient {
        async fn list_storage_nodes(
            &mut self,
            _request: ListStorageNodesRequest,
        ) -> Result<Response<ListStorageNodesResponse>, Status> {
            if self.fail_list_storage_nodes {
                return Err(Status::unavailable("Mock: list_storage_nodes failure"));
            }
            let nodes = self.storage_nodes.lock().unwrap().clone();
            Ok(Response::new(ListStorageNodesResponse {
                storage_nodes: nodes,
            }))
        }

        async fn put_object(
            &mut self,
            request: PutObjectRequest,
        ) -> Result<Response<PutObjectResponse>, Status> {
            if self.fail_put_object {
                return Err(Status::internal("Mock: put_object failure"));
            }
            let mut objects = self.objects.lock().unwrap();
            if objects.contains_key(&request.name) {
                return Err(Status::already_exists("Object already exists"));
            }
            objects.insert(
                request.name,
                StoredObject {
                    checksum: request.checksum,
                    chunk_ids: Vec::new(),
                },
            );
            Ok(Response::new(PutObjectResponse {}))
        }

        async fn get_object(
            &mut self,
            request: GetObjectRequest,
        ) -> Result<Response<GetObjectResponse>, Status> {
            let objects = self.objects.lock().unwrap();
            let chunks_store = self.chunks.lock().unwrap();
            let storage_nodes = self.storage_nodes.lock().unwrap();

            match objects.get(&request.object_name) {
                Some(obj) => {
                    let chunks: Vec<MetaChunk> = obj
                        .chunk_ids
                        .iter()
                        .filter_map(|id| {
                            chunks_store.get(id).map(|chunk| MetaChunk {
                                id: *id,
                                checksum: chunk.checksum,
                                storage_nodes: chunk
                                    .storage_node_ids
                                    .iter()
                                    .filter_map(|node_id| {
                                        storage_nodes
                                            .iter()
                                            .find(|n| n.id == *node_id)
                                            .cloned()
                                    })
                                    .collect(),
                            })
                        })
                        .collect();

                    Ok(Response::new(GetObjectResponse {
                        checksum: obj.checksum,
                        chunks,
                    }))
                }
                None => Err(Status::not_found("Object not found")),
            }
        }

        async fn delete_object(
            &mut self,
            request: DeleteObjectRequest,
        ) -> Result<Response<DeleteObjectResponse>, Status> {
            let mut objects = self.objects.lock().unwrap();
            match objects.remove(&request.object_name) {
                Some(_) => Ok(Response::new(DeleteObjectResponse {})),
                None => Err(Status::not_found("Object not found")),
            }
        }

        async fn allocate_chunk(
            &mut self,
            _request: AllocateChunkRequest,
        ) -> Result<Response<AllocateChunkResponse>, Status> {
            if self.fail_allocate_chunk {
                return Err(Status::internal("Mock: allocate_chunk failure"));
            }
            let storage_nodes = self.storage_nodes.lock().unwrap();
            if storage_nodes.is_empty() {
                return Err(Status::unavailable("No storage nodes available"));
            }

            let mut chunk_id = self.next_chunk_id.lock().unwrap();
            let id = *chunk_id;
            *chunk_id += 1;

            let node = &storage_nodes[0];
            Ok(Response::new(AllocateChunkResponse {
                chunk_id: id,
                storage_node_address: node.address.clone(),
                storage_node_id: node.id,
            }))
        }

        async fn confirm_chunk(
            &mut self,
            request: ConfirmChunkRequest,
        ) -> Result<Response<ConfirmChunkResponse>, Status> {
            let mut objects = self.objects.lock().unwrap();
            let mut chunks = self.chunks.lock().unwrap();

            // Store the chunk
            chunks.insert(
                request.chunk_id,
                StoredChunk {
                    object_name: request.object_name.clone(),
                    checksum: request.checksum,
                    storage_node_ids: vec![request.storage_node_id],
                },
            );

            // Add chunk to object
            if let Some(obj) = objects.get_mut(&request.object_name) {
                obj.chunk_ids.push(request.chunk_id);
            }

            Ok(Response::new(ConfirmChunkResponse {}))
        }

        async fn list_objects(
            &mut self,
            _request: ListObjectsRequest,
        ) -> Result<Response<ListObjectsResponse>, Status> {
            let objects = self.objects.lock().unwrap();
            let chunks_store = self.chunks.lock().unwrap();
            let storage_nodes = self.storage_nodes.lock().unwrap();

            let object_list: Vec<MetaObject> = objects
                .iter()
                .map(|(name, obj)| {
                    let chunks: Vec<MetaChunk> = obj
                        .chunk_ids
                        .iter()
                        .filter_map(|id| {
                            chunks_store.get(id).map(|chunk| MetaChunk {
                                id: *id,
                                checksum: chunk.checksum,
                                storage_nodes: chunk
                                    .storage_node_ids
                                    .iter()
                                    .filter_map(|node_id| {
                                        storage_nodes
                                            .iter()
                                            .find(|n| n.id == *node_id)
                                            .cloned()
                                    })
                                    .collect(),
                            })
                        })
                        .collect();

                    MetaObject {
                        name: name.clone(),
                        checksum: obj.checksum,
                        chunks,
                        total_size: 0,
                    }
                })
                .collect();

            Ok(Response::new(ListObjectsResponse {
                objects: object_list,
            }))
        }

        async fn delete_chunk(
            &mut self,
            request: MetadataDeleteChunkRequest,
        ) -> Result<Response<MetadataDeleteChunkResponse>, Status> {
            let mut chunks = self.chunks.lock().unwrap();
            chunks.remove(&request.chunk_id);
            Ok(Response::new(MetadataDeleteChunkResponse {}))
        }

        async fn put_chunk(
            &mut self,
            request: PutChunkRequest,
        ) -> Result<Response<PutChunkResponse>, Status> {
            let storage_nodes = self.storage_nodes.lock().unwrap();
            if storage_nodes.is_empty() {
                return Err(Status::unavailable("No storage nodes available"));
            }
            let node = &storage_nodes[0];
            let mut chunk_id_counter = self.next_chunk_id.lock().unwrap();
            let id = *chunk_id_counter;
            *chunk_id_counter += 1;

            let mut chunks = self.chunks.lock().unwrap();
            chunks.insert(id, StoredChunk {
                object_name: request.object_name.clone(),
                checksum: request.checksum,
                storage_node_ids: vec![node.id],
            });

            let mut objects = self.objects.lock().unwrap();
            if let Some(obj) = objects.get_mut(&request.object_name) {
                obj.chunk_ids.push(id);
            }

            Ok(Response::new(PutChunkResponse {
                chunk_id: id,
                storage_node_address: node.address.clone(),
            }))
        }

        async fn put_storage_node(
            &mut self,
            request: PutStorageNodeRequest,
        ) -> Result<Response<PutStorageNodeResponse>, Status> {
            let mut nodes = self.storage_nodes.lock().unwrap();
            let id = nodes.len() as u64 + 1;
            nodes.push(MetaStorageNode {
                id,
                address: request.address,
            });
            Ok(Response::new(PutStorageNodeResponse { id }))
        }

        async fn get_chunk(
            &mut self,
            request: GetChunkRequest,
        ) -> Result<Response<GetChunkResponse>, Status> {
            let chunks = self.chunks.lock().unwrap();
            let storage_nodes = self.storage_nodes.lock().unwrap();
            match chunks.get(&request.chunk_id) {
                Some(chunk) => {
                    let nodes = chunk.storage_node_ids.iter()
                        .filter_map(|node_id| {
                            storage_nodes.iter().find(|n| n.id == *node_id).cloned()
                        })
                        .collect();
                    Ok(Response::new(GetChunkResponse {
                        object_name: chunk.object_name.clone(),
                        checksum: chunk.checksum,
                        storage_nodes: nodes,
                    }))
                }
                None => Err(Status::not_found("Chunk not found")),
            }
        }

        async fn get_storage_node(
            &mut self,
            request: GetStorageNodeRequest,
        ) -> Result<Response<GetStorageNodeResponse>, Status> {
            let nodes = self.storage_nodes.lock().unwrap();
            match nodes.iter().find(|n| n.id == request.id) {
                Some(node) => Ok(Response::new(GetStorageNodeResponse {
                    address: node.address.clone(),
                })),
                None => Err(Status::not_found("Storage node not found")),
            }
        }

        async fn delete_storage_node(
            &mut self,
            request: DeleteStorageNodeRequest,
        ) -> Result<Response<DeleteStorageNodeResponse>, Status> {
            let mut nodes = self.storage_nodes.lock().unwrap();
            let before = nodes.len();
            nodes.retain(|n| n.id != request.id);
            if nodes.len() == before {
                return Err(Status::not_found("Storage node not found"));
            }
            Ok(Response::new(DeleteStorageNodeResponse {}))
        }

        async fn list_under_replicated_chunks(
            &mut self,
            request: ListUnderReplicatedChunksRequest,
        ) -> Result<Response<ListUnderReplicatedChunksResponse>, Status> {
            let chunks = self.chunks.lock().unwrap();
            let max = request.max_replicas as usize;
            let under_rep: Vec<_> = chunks.iter()
                .filter(|(_, c)| c.storage_node_ids.len() < max)
                .map(|(id, c)| storage_proto_lib::storage_metadata::UnderReplicatedChunk {
                    chunk_id: *id,
                    checksum: c.checksum,
                    current_storage_node_ids: c.storage_node_ids.clone(),
                })
                .collect();
            Ok(Response::new(ListUnderReplicatedChunksResponse { chunks: under_rep }))
        }

        async fn add_chunk_replica(
            &mut self,
            request: AddChunkReplicaRequest,
        ) -> Result<Response<AddChunkReplicaResponse>, Status> {
            let mut chunks = self.chunks.lock().unwrap();
            match chunks.get_mut(&request.chunk_id) {
                Some(chunk) => {
                    chunk.storage_node_ids.push(request.storage_node_id);
                    Ok(Response::new(AddChunkReplicaResponse {}))
                }
                None => Err(Status::not_found("Chunk not found")),
            }
        }

        async fn update_object_checksum(
            &mut self,
            request: UpdateObjectChecksumRequest,
        ) -> Result<Response<UpdateObjectChecksumResponse>, Status> {
            let mut objects = self.objects.lock().unwrap();
            match objects.get_mut(&request.object_name) {
                Some(obj) => {
                    obj.checksum = request.checksum;
                    Ok(Response::new(UpdateObjectChecksumResponse {}))
                }
                None => Err(Status::not_found("Object not found")),
            }
        }
    }

    /// Mock storage node client with in-memory chunk storage.
    #[derive(Clone)]
    pub struct MockStorageNodeClient {
        /// Shared chunk storage (chunk_id -> bytes)
        pub chunks: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
        /// Chunk size for this "node"
        pub chunk_size: u64,
        /// Error injection
        pub fail_write: bool,
        pub fail_read: bool,
    }

    impl Default for MockStorageNodeClient {
        fn default() -> Self {
            Self {
                chunks: Arc::new(Mutex::new(HashMap::new())),
                chunk_size: 8 * 1024 * 1024, // 8MB default
                fail_write: false,
                fail_read: false,
            }
        }
    }

    #[async_trait]
    impl StorageNodeClient for MockStorageNodeClient {
        async fn get_chunk_size(
            &mut self,
            _request: GetChunkSizeRequest,
        ) -> Result<Response<GetChunkSizeResponse>, Status> {
            Ok(Response::new(GetChunkSizeResponse {
                chunk_size: self.chunk_size,
            }))
        }

        async fn write_chunk(
            &mut self,
            request: WriteChunkRequest,
        ) -> Result<Response<WriteChunkResponse>, Status> {
            if self.fail_write {
                return Err(Status::internal("Mock: write_chunk failure"));
            }
            let mut chunks = self.chunks.lock().unwrap();
            chunks.insert(request.chunk_id, request.chunk_bytes);
            Ok(Response::new(WriteChunkResponse {
                chunk_id: request.chunk_id,
            }))
        }

        async fn read_chunk(
            &mut self,
            request: ReadChunkRequest,
        ) -> Result<Response<ReadChunkResponse>, Status> {
            if self.fail_read {
                return Err(Status::internal("Mock: read_chunk failure"));
            }
            let chunks = self.chunks.lock().unwrap();
            match chunks.get(&request.chunk_id) {
                Some(bytes) => {
                    let mut hasher = crc32fast::Hasher::new();
                    hasher.update(bytes);
                    let checksum = hasher.finalize();
                    Ok(Response::new(ReadChunkResponse {
                        chunk_id: request.chunk_id,
                        chunk_bytes: bytes.clone(),
                        checksum,
                    }))
                }
                None => Err(Status::not_found("Chunk not found")),
            }
        }

        async fn delete_chunk(
            &mut self,
            request: StorageDeleteChunkRequest,
        ) -> Result<Response<StorageDeleteChunkResponse>, Status> {
            let mut chunks = self.chunks.lock().unwrap();
            chunks.remove(&request.chunk_id);
            Ok(Response::new(StorageDeleteChunkResponse {
                chunk_id: request.chunk_id,
            }))
        }

        async fn get_free_chunks(
            &mut self,
            _request: GetFreeChunksRequest,
        ) -> Result<Response<GetFreeChunksResponse>, Status> {
            Ok(Response::new(GetFreeChunksResponse { num_free: 100 }))
        }

        async fn check_health(
            &mut self,
            _request: CheckHealthRequest,
        ) -> Result<Response<CheckHealthResponse>, Status> {
            Ok(Response::new(CheckHealthResponse { healthy: true }))
        }
    }

    /// Mock factory that returns pre-configured mock clients sharing state.
    #[derive(Clone)]
    pub struct MockStorageNodeClientFactory {
        /// Shared chunk storage across all clients
        pub shared_chunks: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
        /// Chunk size for created clients
        pub chunk_size: u64,
    }

    impl Default for MockStorageNodeClientFactory {
        fn default() -> Self {
            Self {
                shared_chunks: Arc::new(Mutex::new(HashMap::new())),
                chunk_size: 8 * 1024 * 1024,
            }
        }
    }

    #[async_trait]
    impl StorageNodeClientFactory for MockStorageNodeClientFactory {
        type Client = MockStorageNodeClient;

        async fn connect(&self, _address: &str) -> Result<Self::Client, tonic::transport::Error> {
            Ok(MockStorageNodeClient {
                chunks: self.shared_chunks.clone(),
                chunk_size: self.chunk_size,
                fail_write: false,
                fail_read: false,
            })
        }
    }
}
