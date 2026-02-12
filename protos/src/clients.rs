//! Client trait abstractions for gRPC services.
//!
//! This module provides trait definitions for dependency injection and testing.
//! Concrete implementations are provided by downstream crates.

use async_trait::async_trait;
use crate::storage_metadata::{
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
use crate::storage_node::{
    CheckHealthRequest, CheckHealthResponse,
    DeleteChunkRequest as StorageDeleteChunkRequest,
    DeleteChunkResponse as StorageDeleteChunkResponse,
    GetChunkSizeRequest, GetChunkSizeResponse,
    GetFreeChunksRequest, GetFreeChunksResponse,
    ReadChunkRequest, ReadChunkResponse, WriteChunkRequest, WriteChunkResponse,
};
use tonic::{Response, Status};

/// Trait for interacting with the metadata service.
#[async_trait]
pub trait MetadataClient: Clone + Send + Sync {
    async fn list_storage_nodes(
        &mut self,
        request: ListStorageNodesRequest,
    ) -> Result<Response<ListStorageNodesResponse>, Status>;

    async fn put_object(
        &mut self,
        request: PutObjectRequest,
    ) -> Result<Response<PutObjectResponse>, Status>;

    async fn get_object(
        &mut self,
        request: GetObjectRequest,
    ) -> Result<Response<GetObjectResponse>, Status>;

    async fn delete_object(
        &mut self,
        request: DeleteObjectRequest,
    ) -> Result<Response<DeleteObjectResponse>, Status>;

    async fn allocate_chunk(
        &mut self,
        request: AllocateChunkRequest,
    ) -> Result<Response<AllocateChunkResponse>, Status>;

    async fn confirm_chunk(
        &mut self,
        request: ConfirmChunkRequest,
    ) -> Result<Response<ConfirmChunkResponse>, Status>;

    async fn list_objects(
        &mut self,
        request: ListObjectsRequest,
    ) -> Result<Response<ListObjectsResponse>, Status>;

    async fn delete_chunk(
        &mut self,
        request: MetadataDeleteChunkRequest,
    ) -> Result<Response<MetadataDeleteChunkResponse>, Status>;

    async fn put_chunk(
        &mut self,
        request: PutChunkRequest,
    ) -> Result<Response<PutChunkResponse>, Status>;

    async fn put_storage_node(
        &mut self,
        request: PutStorageNodeRequest,
    ) -> Result<Response<PutStorageNodeResponse>, Status>;

    async fn get_chunk(
        &mut self,
        request: GetChunkRequest,
    ) -> Result<Response<GetChunkResponse>, Status>;

    async fn get_storage_node(
        &mut self,
        request: GetStorageNodeRequest,
    ) -> Result<Response<GetStorageNodeResponse>, Status>;

    async fn delete_storage_node(
        &mut self,
        request: DeleteStorageNodeRequest,
    ) -> Result<Response<DeleteStorageNodeResponse>, Status>;

    async fn list_under_replicated_chunks(
        &mut self,
        request: ListUnderReplicatedChunksRequest,
    ) -> Result<Response<ListUnderReplicatedChunksResponse>, Status>;

    async fn add_chunk_replica(
        &mut self,
        request: AddChunkReplicaRequest,
    ) -> Result<Response<AddChunkReplicaResponse>, Status>;

    async fn update_object_checksum(
        &mut self,
        request: UpdateObjectChecksumRequest,
    ) -> Result<Response<UpdateObjectChecksumResponse>, Status>;
}

/// Trait for interacting with storage nodes.
#[async_trait]
pub trait StorageNodeClient: Clone + Send + Sync {
    async fn get_chunk_size(
        &mut self,
        request: GetChunkSizeRequest,
    ) -> Result<Response<GetChunkSizeResponse>, Status>;

    async fn write_chunk(
        &mut self,
        request: WriteChunkRequest,
    ) -> Result<Response<WriteChunkResponse>, Status>;

    async fn read_chunk(
        &mut self,
        request: ReadChunkRequest,
    ) -> Result<Response<ReadChunkResponse>, Status>;

    async fn delete_chunk(
        &mut self,
        request: StorageDeleteChunkRequest,
    ) -> Result<Response<StorageDeleteChunkResponse>, Status>;

    async fn get_free_chunks(
        &mut self,
        request: GetFreeChunksRequest,
    ) -> Result<Response<GetFreeChunksResponse>, Status>;

    async fn check_health(
        &mut self,
        request: CheckHealthRequest,
    ) -> Result<Response<CheckHealthResponse>, Status>;
}

/// Factory trait for creating storage node client connections.
#[async_trait]
pub trait StorageNodeClientFactory: Clone + Send + Sync {
    type Client: StorageNodeClient;

    async fn connect(&self, address: &str) -> Result<Self::Client, tonic::transport::Error>;
}
