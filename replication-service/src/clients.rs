use async_trait::async_trait;
use storage_proto_lib::storage_metadata::storage_metadata_service_client::StorageMetadataServiceClient;
use storage_proto_lib::storage_metadata::{
    AddChunkReplicaRequest, AddChunkReplicaResponse,
    AllocateChunkRequest, AllocateChunkResponse,
    ConfirmChunkRequest, ConfirmChunkResponse,
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
    ReadChunkRequest, ReadChunkResponse,
    WriteChunkRequest, WriteChunkResponse,
};
use storage_proto_lib::{MetadataClient, StorageNodeClient, StorageNodeClientFactory};
use tonic::transport::Channel;
use tonic::{Response, Status};

/// Real metadata client wrapper.
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
    async fn list_storage_nodes(&mut self, request: ListStorageNodesRequest) -> Result<Response<ListStorageNodesResponse>, Status> {
        self.inner.list_storage_nodes(request).await
    }
    async fn put_object(&mut self, request: PutObjectRequest) -> Result<Response<PutObjectResponse>, Status> {
        self.inner.put_object(request).await
    }
    async fn get_object(&mut self, request: GetObjectRequest) -> Result<Response<GetObjectResponse>, Status> {
        self.inner.get_object(request).await
    }
    async fn delete_object(&mut self, request: DeleteObjectRequest) -> Result<Response<DeleteObjectResponse>, Status> {
        self.inner.delete_object(request).await
    }
    async fn allocate_chunk(&mut self, request: AllocateChunkRequest) -> Result<Response<AllocateChunkResponse>, Status> {
        self.inner.allocate_chunk(request).await
    }
    async fn confirm_chunk(&mut self, request: ConfirmChunkRequest) -> Result<Response<ConfirmChunkResponse>, Status> {
        self.inner.confirm_chunk(request).await
    }
    async fn list_objects(&mut self, request: ListObjectsRequest) -> Result<Response<ListObjectsResponse>, Status> {
        self.inner.list_objects(request).await
    }
    async fn delete_chunk(&mut self, request: MetadataDeleteChunkRequest) -> Result<Response<MetadataDeleteChunkResponse>, Status> {
        self.inner.delete_chunk(request).await
    }
    async fn put_chunk(&mut self, request: PutChunkRequest) -> Result<Response<PutChunkResponse>, Status> {
        self.inner.put_chunk(request).await
    }
    async fn put_storage_node(&mut self, request: PutStorageNodeRequest) -> Result<Response<PutStorageNodeResponse>, Status> {
        self.inner.put_storage_node(request).await
    }
    async fn get_chunk(&mut self, request: GetChunkRequest) -> Result<Response<GetChunkResponse>, Status> {
        self.inner.get_chunk(request).await
    }
    async fn get_storage_node(&mut self, request: GetStorageNodeRequest) -> Result<Response<GetStorageNodeResponse>, Status> {
        self.inner.get_storage_node(request).await
    }
    async fn delete_storage_node(&mut self, request: DeleteStorageNodeRequest) -> Result<Response<DeleteStorageNodeResponse>, Status> {
        self.inner.delete_storage_node(request).await
    }
    async fn list_under_replicated_chunks(&mut self, request: ListUnderReplicatedChunksRequest) -> Result<Response<ListUnderReplicatedChunksResponse>, Status> {
        self.inner.list_under_replicated_chunks(request).await
    }
    async fn add_chunk_replica(&mut self, request: AddChunkReplicaRequest) -> Result<Response<AddChunkReplicaResponse>, Status> {
        self.inner.add_chunk_replica(request).await
    }
    async fn update_object_checksum(&mut self, request: UpdateObjectChecksumRequest) -> Result<Response<UpdateObjectChecksumResponse>, Status> {
        self.inner.update_object_checksum(request).await
    }
}

/// Real storage node client wrapper.
#[derive(Clone)]
pub struct RealStorageNodeClient {
    inner: StorageNodeServiceClient<Channel>,
}

#[async_trait]
impl StorageNodeClient for RealStorageNodeClient {
    async fn get_chunk_size(&mut self, request: GetChunkSizeRequest) -> Result<Response<GetChunkSizeResponse>, Status> {
        self.inner.get_chunk_size(request).await
    }
    async fn write_chunk(&mut self, request: WriteChunkRequest) -> Result<Response<WriteChunkResponse>, Status> {
        self.inner.write_chunk(request).await
    }
    async fn read_chunk(&mut self, request: ReadChunkRequest) -> Result<Response<ReadChunkResponse>, Status> {
        self.inner.read_chunk(request).await
    }
    async fn delete_chunk(&mut self, request: StorageDeleteChunkRequest) -> Result<Response<StorageDeleteChunkResponse>, Status> {
        self.inner.delete_chunk(request).await
    }
    async fn get_free_chunks(&mut self, request: GetFreeChunksRequest) -> Result<Response<GetFreeChunksResponse>, Status> {
        self.inner.get_free_chunks(request).await
    }
    async fn check_health(&mut self, request: CheckHealthRequest) -> Result<Response<CheckHealthResponse>, Status> {
        self.inner.check_health(request).await
    }
}

/// Factory that creates real storage node connections.
#[derive(Clone)]
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
        Ok(RealStorageNodeClient { inner: client })
    }
}
