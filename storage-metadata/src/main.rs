mod chunk_index;
mod cluster;
mod cluster_service;
mod forwarding;
mod hash_ring;
mod metadata_store;
mod metrics;
mod migration;

use axum::{Router, http::header, routing::get};
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace::SdkTracerProvider};
use prometheus_client::registry::Registry;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::time::{self, Duration};
use tonic::{Code, Request, Response, Status, transport::Server};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use storage_proto_lib::storage_metadata::{
    self, AddChunkReplicaRequest, AddChunkReplicaResponse, AllocateChunkRequest,
    AllocateChunkResponse, ConfirmChunkRequest, ConfirmChunkResponse, DeleteChunkRequest,
    DeleteChunkResponse, DeleteObjectRequest, DeleteObjectResponse, DeleteStorageNodeRequest,
    DeleteStorageNodeResponse, GetChunkRequest, GetChunkResponse, GetObjectRequest,
    GetObjectResponse, GetStorageNodeRequest, GetStorageNodeResponse, ListObjectsRequest,
    ListObjectsResponse, ListStorageNodesRequest, ListStorageNodesResponse,
    ListUnderReplicatedChunksRequest, ListUnderReplicatedChunksResponse, PutChunkRequest,
    PutChunkResponse, PutObjectRequest, PutObjectResponse, PutStorageNodeRequest,
    PutStorageNodeResponse, UnderReplicatedChunk,
    metadata_cluster_service_server::MetadataClusterServiceServer,
    storage_metadata_service_server::{StorageMetadataService, StorageMetadataServiceServer},
};

use crate::chunk_index::ChunkIndex;
use crate::cluster::ClusterManager;
use crate::cluster_service::MetadataClusterServiceImpl;
use crate::forwarding::{ForwardingLayer, RouteAction};
use crate::metadata_store::MetadataStore;
use crate::migration::MigrationManager;

#[derive(Clone)]
pub struct StorageMetadataImpl {
    metadata_store: Arc<tokio::sync::Mutex<MetadataStore>>,
    metrics: Arc<metrics::Metrics>,
    forwarding_layer: Option<Arc<ForwardingLayer>>,
    chunk_index: Arc<ChunkIndex>,
}

impl StorageMetadataImpl {
    pub fn construct(
        metrics: Arc<metrics::Metrics>,
        data_dir: &str,
        node_id: &str,
    ) -> std::io::Result<Self> {
        let metadata_store = MetadataStore::new_with_node(data_dir, node_id)?;
        let metadata_store = Arc::new(tokio::sync::Mutex::new(metadata_store));
        let chunk_index = Arc::new(ChunkIndex::new());
        Ok(StorageMetadataImpl {
            metadata_store,
            metrics,
            forwarding_layer: None,
            chunk_index,
        })
    }

    /// Construct for standalone (non-clustered) mode — backwards compatible.
    pub fn construct_standalone(metrics: Arc<metrics::Metrics>) -> std::io::Result<Self> {
        let path = "metadata.dat";
        let metadata_store = MetadataStore::new(path)?;
        let metadata_store = Arc::new(tokio::sync::Mutex::new(metadata_store));
        let chunk_index = Arc::new(ChunkIndex::new());
        Ok(StorageMetadataImpl {
            metadata_store,
            metrics,
            forwarding_layer: None,
            chunk_index,
        })
    }

    pub fn set_forwarding_layer(&mut self, layer: Arc<ForwardingLayer>) {
        self.forwarding_layer = Some(layer);
    }

    /// Populate the chunk index from the current metadata store.
    pub async fn populate_chunk_index(&self) {
        let store = self.metadata_store.lock().await;
        self.chunk_index.populate_from_chunks(&store.chunks).await;
    }

    /// Check if a request should be forwarded, based on object name routing.
    async fn route_by_object(&self, object_name: &str) -> RouteAction {
        match &self.forwarding_layer {
            Some(fl) => fl.route_by_object(object_name).await,
            None => RouteAction::Local,
        }
    }

    /// Check if a request should be forwarded, based on chunk ID routing.
    async fn route_by_chunk(&self, chunk_id: u64) -> Result<RouteAction, Status> {
        match &self.forwarding_layer {
            Some(fl) => fl.route_by_chunk(chunk_id).await,
            None => Ok(RouteAction::Local),
        }
    }
}

#[tonic::async_trait]
impl StorageMetadataService for StorageMetadataImpl {
    #[tracing::instrument(skip(self, request), fields(object_name = %request.get_ref().name, checksum = request.get_ref().checksum, error))]
    async fn put_object(
        &self,
        request: Request<PutObjectRequest>,
    ) -> Result<Response<PutObjectResponse>, Status> {
        let req = request.get_ref();

        // Route: object-scoped
        if let RouteAction::Forward(addr) = self.route_by_object(&req.name).await {
            if let Some(ref fl) = self.forwarding_layer {
                return fl.forward_put_object(&addr, req.clone()).await;
            }
        }

        match self
            .metadata_store
            .lock()
            .await
            .put_object(req.checksum, req.name.clone())
        {
            Ok(_) => {
                self.metrics
                    .grpc_requests_total
                    .get_or_create(&metrics::RpcLabels {
                        method: "put_object".to_string(),
                        status: "ok".to_string(),
                    })
                    .inc();
                self.metrics.objects_total.inc();
                let resp = PutObjectResponse {};
                Ok(Response::new(resp))
            }
            Err(e) => {
                self.metrics
                    .grpc_requests_total
                    .get_or_create(&metrics::RpcLabels {
                        method: "put_object".to_string(),
                        status: "error".to_string(),
                    })
                    .inc();
                let span = tracing::Span::current();
                let return_status = match e.kind() {
                    std::io::ErrorKind::AlreadyExists => {
                        span.record("error", format!("object name {} already exists", req.name));
                        Status::new(
                            Code::AlreadyExists,
                            format!("object name {} already exists", req.name),
                        )
                    }
                    _ => {
                        span.record("error", "failed to store object");
                        Status::new(Code::Internal, "failed to store object")
                    }
                };
                Err(return_status)
            }
        }
    }

    #[tracing::instrument(skip(self, request), fields(object_name = %request.get_ref().object_name, checksum = request.get_ref().checksum, chunk_id, storage_node_address, error, chunk.created_for_object, storage_node.found, storage_node.id))]
    async fn put_chunk(
        &self,
        request: Request<PutChunkRequest>,
    ) -> Result<Response<PutChunkResponse>, Status> {
        let req = request.get_ref();
        let span = tracing::Span::current();

        // Route: object-scoped
        if let RouteAction::Forward(addr) = self.route_by_object(&req.object_name).await {
            if let Some(ref fl) = self.forwarding_layer {
                return fl.forward_put_chunk(&addr, req.clone()).await;
            }
        }

        let (chunk_id, storage_node_address) = match self
            .metadata_store
            .lock()
            .await
            .put_chunk(req.object_name.clone(), req.checksum)
        {
            Ok((chunk_id, storage_node_address)) => {
                span.record("chunk_id", chunk_id);
                span.record("storage_node_address", &storage_node_address);
                (chunk_id, storage_node_address)
            }
            Err(e) => {
                let return_status = match e.kind() {
                    std::io::ErrorKind::AlreadyExists => {
                        span.record("error", "chunk id already exists");
                        tonic::Status::new(tonic::Code::AlreadyExists, "chunk id already exists")
                    }
                    _ => {
                        span.record("error", "failed to store chunk");
                        tonic::Status::new(tonic::Code::Internal, "failed to store chunk")
                    }
                };
                return Err(return_status);
            }
        };

        // Update chunk index
        self.chunk_index
            .insert(chunk_id, req.object_name.clone())
            .await;

        self.metrics
            .grpc_requests_total
            .get_or_create(&metrics::RpcLabels {
                method: "put_chunk".to_string(),
                status: "ok".to_string(),
            })
            .inc();
        self.metrics.chunks_total.inc();
        let resp = PutChunkResponse {
            chunk_id,
            storage_node_address,
        };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, request), fields(object_name = %request.get_ref().object_name, object_checksum, object_chunks_count, error))]
    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>, Status> {
        let req = request.get_ref();
        let span = tracing::Span::current();

        // Route: object-scoped
        if let RouteAction::Forward(addr) = self.route_by_object(&req.object_name).await {
            if let Some(ref fl) = self.forwarding_layer {
                return fl.forward_get_object(&addr, req.clone()).await;
            }
        }

        let object = match self
            .metadata_store
            .lock()
            .await
            .get_object(req.object_name.clone())
        {
            Ok(object) => object.clone(),
            Err(e) => {
                let return_status = match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        span.record("error", "object not found");
                        Status::new(Code::NotFound, "object not found")
                    }
                    _ => {
                        span.record("error", "failed to get object");
                        Status::new(Code::Internal, "failed to get object")
                    }
                };
                return Err(return_status);
            }
        };

        // Hold a single lock to avoid TOCTOU races between object, chunks, and storage_nodes
        let store = self.metadata_store.lock().await;

        span.record("object_checksum", object.checksum);
        span.record("object_chunks_count", object.chunks.len());

        self.metrics
            .grpc_requests_total
            .get_or_create(&metrics::RpcLabels {
                method: "get_object".to_string(),
                status: "ok".to_string(),
            })
            .inc();

        let mut response_chunks = Vec::with_capacity(object.chunks.len());
        for chunk_id in &object.chunks {
            let chunk = match store.chunks.get(chunk_id) {
                Some(c) => c,
                None => {
                    return Err(Status::new(
                        Code::Internal,
                        format!("chunk {} referenced by object but not found", chunk_id),
                    ));
                }
            };
            let mut storage_nodes = Vec::with_capacity(chunk.storage_nodes.len());
            for node_id in &chunk.storage_nodes {
                let node = match store.storage_nodes.get(node_id) {
                    Some(n) => n,
                    None => {
                        return Err(Status::new(
                            Code::Internal,
                            format!("storage node {} referenced by chunk but not found", node_id),
                        ));
                    }
                };
                storage_nodes.push(storage_metadata::MetaStorageNode {
                    id: *node_id,
                    address: node.address.clone(),
                });
            }
            response_chunks.push(storage_metadata::MetaChunk {
                id: chunk.id,
                checksum: chunk.checksum,
                storage_nodes,
            });
        }

        let resp = GetObjectResponse {
            checksum: object.checksum,
            chunks: response_chunks,
        };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, request), fields(chunk_id = request.get_ref().chunk_id, object_name, chunk_checksum, chunk_storage_nodes_count, error))]
    async fn get_chunk(
        &self,
        request: Request<GetChunkRequest>,
    ) -> Result<Response<GetChunkResponse>, Status> {
        let req = request.get_ref();
        let span = tracing::Span::current();

        // Route: chunk-scoped (lookup chunk index → object_name → forward to owner)
        match self.route_by_chunk(req.chunk_id).await {
            Ok(RouteAction::Forward(addr)) => {
                if let Some(ref fl) = self.forwarding_layer {
                    return fl.forward_get_chunk(&addr, req.clone()).await;
                }
            }
            Ok(RouteAction::Local) => {}
            Err(_) => {
                // Chunk not in index — try local store as fallback
            }
        }

        let (object_name, chunk) = match self.metadata_store.lock().await.get_chunk(req.chunk_id) {
            Ok((object_name, chunk)) => {
                span.record("object_name", &object_name);
                span.record("chunk_checksum", chunk.checksum);
                span.record("chunk_storage_nodes_count", chunk.storage_nodes.len());
                (object_name, chunk.clone())
            }
            Err(e) => {
                let return_status = match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        span.record("error", "chunk not found");
                        Status::new(Code::NotFound, "chunk not found")
                    }
                    _ => {
                        span.record("error", "failed to get chunk");
                        Status::new(Code::Internal, "failed to get chunk")
                    }
                };
                return Err(return_status);
            }
        };

        let store = self.metadata_store.lock().await;

        let mut storage_nodes = Vec::with_capacity(chunk.storage_nodes.len());
        for node_id in &chunk.storage_nodes {
            let node = match store.storage_nodes.get(node_id) {
                Some(n) => n,
                None => {
                    return Err(Status::new(
                        Code::Internal,
                        format!("storage node {} referenced by chunk but not found", node_id),
                    ));
                }
            };
            storage_nodes.push(storage_metadata::MetaStorageNode {
                id: *node_id,
                address: node.address.clone(),
            });
        }

        let resp = GetChunkResponse {
            object_name,
            checksum: chunk.checksum,
            storage_nodes,
        };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, request), fields(object_name = %request.get_ref().object_name, error))]
    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let req = request.get_ref();
        let span = tracing::Span::current();

        // Route: object-scoped
        if let RouteAction::Forward(addr) = self.route_by_object(&req.object_name).await {
            if let Some(ref fl) = self.forwarding_layer {
                return fl.forward_delete_object(&addr, req.clone()).await;
            }
        }

        // Get chunk IDs for index cleanup
        let chunk_ids = {
            let store = self.metadata_store.lock().await;
            match store.get_object(req.object_name.clone()) {
                Ok(obj) => obj.chunks.clone(),
                Err(_) => Vec::new(),
            }
        };

        if let Err(e) = self
            .metadata_store
            .lock()
            .await
            .delete_object(req.object_name.clone())
        {
            let return_status = match e.kind() {
                std::io::ErrorKind::NotFound => {
                    span.record("error", "object not found");
                    Status::new(Code::NotFound, "object not found")
                }
                _ => {
                    span.record("error", "failed to delete object");
                    Status::new(Code::Internal, "failed to delete object")
                }
            };
            return Err(return_status);
        }

        // Clean up chunk index
        for chunk_id in chunk_ids {
            self.chunk_index.remove(chunk_id).await;
        }

        self.metrics
            .grpc_requests_total
            .get_or_create(&metrics::RpcLabels {
                method: "delete_object".to_string(),
                status: "ok".to_string(),
            })
            .inc();
        self.metrics.objects_total.dec();
        let resp = DeleteObjectResponse {};
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, request), fields(chunk_id = request.get_ref().chunk_id, error))]
    async fn delete_chunk(
        &self,
        request: Request<DeleteChunkRequest>,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        let req = request.get_ref();
        let span = tracing::Span::current();

        // Route: chunk-scoped
        match self.route_by_chunk(req.chunk_id).await {
            Ok(RouteAction::Forward(addr)) => {
                if let Some(ref fl) = self.forwarding_layer {
                    return fl.forward_delete_chunk(&addr, req.clone()).await;
                }
            }
            Ok(RouteAction::Local) => {}
            Err(_) => {} // fallback to local
        }

        if let Err(e) = self.metadata_store.lock().await.delete_chunk(req.chunk_id) {
            let return_status = match e.kind() {
                std::io::ErrorKind::NotFound => {
                    span.record("error", "chunk not found");
                    Status::new(Code::NotFound, "chunk not found")
                }
                _ => {
                    span.record("error", "failed to delete chunk");
                    Status::new(Code::Internal, "failed to delete chunk")
                }
            };
            return Err(return_status);
        }

        self.chunk_index.remove(req.chunk_id).await;

        let resp = DeleteChunkResponse {};
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, request), fields(address = %request.get_ref().address, storage_node_id, error))]
    async fn put_storage_node(
        &self,
        request: Request<PutStorageNodeRequest>,
    ) -> Result<Response<PutStorageNodeResponse>, Status> {
        let req = request.get_ref();
        let span = tracing::Span::current();

        // Global write: handle locally AND broadcast to all peers
        let storage_node_id = match self
            .metadata_store
            .lock()
            .await
            .construct_storage_node(req.address.clone())
        {
            Ok(storage_node_id) => {
                span.record("storage_node_id", storage_node_id);
                storage_node_id
            }
            Err(e) => {
                let status = match e.kind() {
                    std::io::ErrorKind::InvalidInput => {
                        span.record("error", "invalid address");
                        Status::new(Code::InvalidArgument, "invalid address")
                    }
                    _ => {
                        span.record("error", "failed to put storage node");
                        Status::new(Code::Internal, "failed to put storage node")
                    }
                };
                return Err(status);
            }
        };

        // Broadcast to peers (fire and forget errors on individual peers)
        if let Some(ref fl) = self.forwarding_layer {
            let results = fl.broadcast_put_storage_node(req).await;
            for result in &results {
                if let Err(e) = result {
                    tracing::warn!("Failed to broadcast put_storage_node to peer: {}", e);
                }
            }
        }

        self.metrics
            .grpc_requests_total
            .get_or_create(&metrics::RpcLabels {
                method: "put_storage_node".to_string(),
                status: "ok".to_string(),
            })
            .inc();
        self.metrics.storage_nodes_total.inc();
        let resp = PutStorageNodeResponse {
            id: storage_node_id,
        };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, _request), fields(objects_count))]
    async fn list_objects(
        &self,
        _request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let span = tracing::Span::current();

        // Aggregate: scatter-gather from all peers + local
        let store = self.metadata_store.lock().await;
        let local_objects = store.list_objects();

        let mut response_objects = Vec::new();

        // Build local objects response
        for obj in &local_objects {
            let mut response_chunks = Vec::with_capacity(obj.chunks.len());
            for chunk_id in &obj.chunks {
                let chunk = match store.chunks.get(chunk_id) {
                    Some(c) => c,
                    None => {
                        tracing::warn!(
                            "chunk {} referenced by object {} but not found, skipping",
                            chunk_id,
                            obj.name
                        );
                        continue;
                    }
                };
                let mut storage_nodes = Vec::with_capacity(chunk.storage_nodes.len());
                for node_id in &chunk.storage_nodes {
                    if let Some(node) = store.storage_nodes.get(node_id) {
                        storage_nodes.push(storage_metadata::MetaStorageNode {
                            id: *node_id,
                            address: node.address.clone(),
                        });
                    } else {
                        tracing::warn!(
                            "storage node {} referenced by chunk {} but not found, skipping",
                            node_id,
                            chunk_id
                        );
                    }
                }
                response_chunks.push(storage_metadata::MetaChunk {
                    id: chunk.id,
                    checksum: chunk.checksum,
                    storage_nodes,
                });
            }
            response_objects.push(storage_metadata::MetaObject {
                name: obj.name.clone(),
                checksum: obj.checksum,
                chunks: response_chunks,
                total_size: obj.total_size,
            });
        }
        // Release the lock before scatter-gather
        drop(store);

        // Gather from peers
        if let Some(ref fl) = self.forwarding_layer {
            match fl.scatter_gather_list_objects().await {
                Ok(peer_objects) => {
                    response_objects.extend(peer_objects);
                }
                Err(e) => {
                    tracing::warn!("Failed to scatter-gather list_objects: {}", e);
                }
            }
        }

        span.record("objects_count", response_objects.len());

        let resp = ListObjectsResponse {
            objects: response_objects,
        };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, request), fields(storage_node_id = request.get_ref().id, address, error))]
    async fn get_storage_node(
        &self,
        request: Request<GetStorageNodeRequest>,
    ) -> Result<Response<GetStorageNodeResponse>, Status> {
        // Global read: handle locally (all nodes have full storage node list)
        let req = request.get_ref();
        let span = tracing::Span::current();
        let address = match self.metadata_store.lock().await.get_storage_node(req.id) {
            Ok(address) => {
                span.record("address", &address);
                address
            }
            Err(e) => {
                let status = match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        span.record("error", format!("{}", e));
                        Status::new(Code::NotFound, format!("{}", e))
                    }
                    _ => {
                        span.record("error", "failed to get storage node");
                        Status::new(Code::Internal, "failed to get storage node")
                    }
                };
                return Err(status);
            }
        };
        let resp = GetStorageNodeResponse { address };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, _request), fields(storage_nodes_count))]
    async fn list_storage_nodes(
        &self,
        _request: Request<ListStorageNodesRequest>,
    ) -> Result<Response<ListStorageNodesResponse>, Status> {
        // Global read: handle locally
        let span = tracing::Span::current();
        let storage_nodes = self.metadata_store.lock().await.list_storage_nodes();
        span.record("storage_nodes_count", storage_nodes.len());
        let resp = ListStorageNodesResponse {
            storage_nodes: storage_nodes
                .iter()
                .map(|node| storage_metadata::MetaStorageNode {
                    id: node.id,
                    address: node.address.clone(),
                })
                .collect(),
        };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, request), fields(storage_node_id = request.get_ref().id, error))]
    async fn delete_storage_node(
        &self,
        request: Request<DeleteStorageNodeRequest>,
    ) -> Result<Response<DeleteStorageNodeResponse>, Status> {
        let req = request.get_ref();
        let storage_node_id = req.id;
        let span = tracing::Span::current();

        // Global write: handle locally AND broadcast
        match self
            .metadata_store
            .lock()
            .await
            .delete_storage_node(storage_node_id)
        {
            Ok(_) => {
                // Broadcast to peers
                if let Some(ref fl) = self.forwarding_layer {
                    let results = fl.broadcast_delete_storage_node(req).await;
                    for result in &results {
                        if let Err(e) = result {
                            tracing::warn!(
                                "Failed to broadcast delete_storage_node to peer: {}",
                                e
                            );
                        }
                    }
                }
                Ok(Response::new(DeleteStorageNodeResponse {}))
            }
            Err(_) => {
                span.record(
                    "error",
                    format!("could not delete storage node {}", storage_node_id),
                );
                Err(Status::new(
                    Code::Internal,
                    format!("could not delete storage node {}", storage_node_id),
                ))
            }
        }
    }

    // ===== Replication Management RPCs =====

    #[tracing::instrument(skip(self, request), fields(max_replicas = request.get_ref().max_replicas, chunks_count))]
    async fn list_under_replicated_chunks(
        &self,
        request: Request<ListUnderReplicatedChunksRequest>,
    ) -> Result<Response<ListUnderReplicatedChunksResponse>, Status> {
        let span = tracing::Span::current();
        let max_replicas = request.get_ref().max_replicas;
        let max_replicas = if max_replicas == 0 { 3 } else { max_replicas };

        // Local data
        let store = self.metadata_store.lock().await;
        let local_chunks = store.list_under_replicated_chunks(max_replicas);
        let mut all_chunks: Vec<UnderReplicatedChunk> = local_chunks
            .iter()
            .map(|chunk| UnderReplicatedChunk {
                chunk_id: chunk.id,
                checksum: chunk.checksum,
                current_storage_node_ids: chunk.storage_nodes.clone(),
            })
            .collect();
        drop(store);

        // Scatter-gather from peers
        if let Some(ref fl) = self.forwarding_layer {
            match fl
                .scatter_gather_under_replicated_chunks(max_replicas)
                .await
            {
                Ok(peer_chunks) => {
                    all_chunks.extend(peer_chunks);
                }
                Err(e) => {
                    tracing::warn!("Failed to scatter-gather under-replicated chunks: {}", e);
                }
            }
        }

        // Sort by replica count (fewest first)
        all_chunks.sort_by_key(|c| c.current_storage_node_ids.len());

        span.record("chunks_count", all_chunks.len());

        self.metrics
            .grpc_requests_total
            .get_or_create(&metrics::RpcLabels {
                method: "list_under_replicated_chunks".to_string(),
                status: "ok".to_string(),
            })
            .inc();

        let resp = ListUnderReplicatedChunksResponse { chunks: all_chunks };
        Ok(Response::new(resp))
    }

    #[tracing::instrument(skip(self, request), fields(chunk_id = request.get_ref().chunk_id, storage_node_id = request.get_ref().storage_node_id, error))]
    async fn add_chunk_replica(
        &self,
        request: Request<AddChunkReplicaRequest>,
    ) -> Result<Response<AddChunkReplicaResponse>, Status> {
        let req = request.get_ref();
        let span = tracing::Span::current();

        // Route: chunk-scoped
        match self.route_by_chunk(req.chunk_id).await {
            Ok(RouteAction::Forward(addr)) => {
                if let Some(ref fl) = self.forwarding_layer {
                    return fl.forward_add_chunk_replica(&addr, req.clone()).await;
                }
            }
            Ok(RouteAction::Local) => {}
            Err(_) => {} // fallback to local
        }

        match self
            .metadata_store
            .lock()
            .await
            .add_chunk_replica(req.chunk_id, req.storage_node_id)
        {
            Ok(_) => {
                self.metrics
                    .grpc_requests_total
                    .get_or_create(&metrics::RpcLabels {
                        method: "add_chunk_replica".to_string(),
                        status: "ok".to_string(),
                    })
                    .inc();
                Ok(Response::new(AddChunkReplicaResponse {}))
            }
            Err(e) => {
                self.metrics
                    .grpc_requests_total
                    .get_or_create(&metrics::RpcLabels {
                        method: "add_chunk_replica".to_string(),
                        status: "error".to_string(),
                    })
                    .inc();
                let status = match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        span.record("error", format!("{}", e));
                        Status::new(Code::NotFound, format!("{}", e))
                    }
                    std::io::ErrorKind::AlreadyExists => {
                        span.record("error", format!("{}", e));
                        Status::new(Code::AlreadyExists, format!("{}", e))
                    }
                    std::io::ErrorKind::InvalidInput => {
                        span.record("error", format!("{}", e));
                        Status::new(Code::FailedPrecondition, format!("{}", e))
                    }
                    _ => {
                        span.record("error", "failed to add chunk replica");
                        Status::new(Code::Internal, "failed to add chunk replica")
                    }
                };
                Err(status)
            }
        }
    }

    // ===== Chunk Allocation RPCs (Query -> Action -> Persist) =====

    #[tracing::instrument(
        skip(self, request),
        fields(chunk_id, storage_node_id, storage_node_address, error)
    )]
    async fn allocate_chunk(
        &self,
        request: Request<AllocateChunkRequest>,
    ) -> Result<Response<AllocateChunkResponse>, Status> {
        let req = request.get_ref();
        let span = tracing::Span::current();

        // Route: object-scoped (uses object_name from request)
        if !req.object_name.is_empty() {
            if let RouteAction::Forward(addr) = self.route_by_object(&req.object_name).await {
                if let Some(ref fl) = self.forwarding_layer {
                    return fl.forward_allocate_chunk(&addr, req.clone()).await;
                }
            }
        }

        match self
            .metadata_store
            .lock()
            .await
            .allocate_chunk(&req.object_name)
        {
            Ok((chunk_id, storage_node_id, storage_node_address)) => {
                span.record("chunk_id", chunk_id);
                span.record("storage_node_id", storage_node_id);
                span.record("storage_node_address", &storage_node_address);

                self.metrics
                    .grpc_requests_total
                    .get_or_create(&metrics::RpcLabels {
                        method: "allocate_chunk".to_string(),
                        status: "ok".to_string(),
                    })
                    .inc();

                Ok(Response::new(AllocateChunkResponse {
                    chunk_id,
                    storage_node_address,
                    storage_node_id,
                }))
            }
            Err(e) => {
                self.metrics
                    .grpc_requests_total
                    .get_or_create(&metrics::RpcLabels {
                        method: "allocate_chunk".to_string(),
                        status: "error".to_string(),
                    })
                    .inc();
                span.record("error", format!("{}", e));
                Err(Status::new(Code::Unavailable, format!("{}", e)))
            }
        }
    }

    #[tracing::instrument(skip(self, request), fields(chunk_id = request.get_ref().chunk_id, object_name = %request.get_ref().object_name, storage_node_id = request.get_ref().storage_node_id, error))]
    async fn confirm_chunk(
        &self,
        request: Request<ConfirmChunkRequest>,
    ) -> Result<Response<ConfirmChunkResponse>, Status> {
        let req = request.get_ref();
        let span = tracing::Span::current();

        // Route: object-scoped
        if let RouteAction::Forward(addr) = self.route_by_object(&req.object_name).await {
            if let Some(ref fl) = self.forwarding_layer {
                return fl.forward_confirm_chunk(&addr, req.clone()).await;
            }
        }

        match self.metadata_store.lock().await.confirm_chunk(
            req.chunk_id,
            req.object_name.clone(),
            req.checksum,
            req.storage_node_id,
        ) {
            Ok(_) => {
                // Update chunk index
                self.chunk_index
                    .insert(req.chunk_id, req.object_name.clone())
                    .await;

                self.metrics
                    .grpc_requests_total
                    .get_or_create(&metrics::RpcLabels {
                        method: "confirm_chunk".to_string(),
                        status: "ok".to_string(),
                    })
                    .inc();
                self.metrics.chunks_total.inc();
                Ok(Response::new(ConfirmChunkResponse {}))
            }
            Err(e) => {
                self.metrics
                    .grpc_requests_total
                    .get_or_create(&metrics::RpcLabels {
                        method: "confirm_chunk".to_string(),
                        status: "error".to_string(),
                    })
                    .inc();
                let status = match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        span.record("error", format!("{}", e));
                        Status::new(Code::NotFound, format!("{}", e))
                    }
                    std::io::ErrorKind::AlreadyExists => {
                        span.record("error", format!("{}", e));
                        Status::new(Code::AlreadyExists, format!("{}", e))
                    }
                    _ => {
                        span.record("error", "failed to confirm chunk");
                        Status::new(Code::Internal, "failed to confirm chunk")
                    }
                };
                Err(status)
            }
        }
    }

    #[tracing::instrument(skip(self, request), fields(object_name = %request.get_ref().object_name, checksum = request.get_ref().checksum, total_size = request.get_ref().total_size, error))]
    async fn update_object_checksum(
        &self,
        request: Request<storage_metadata::UpdateObjectChecksumRequest>,
    ) -> Result<Response<storage_metadata::UpdateObjectChecksumResponse>, Status> {
        let req = request.get_ref();
        let span = tracing::Span::current();

        // Route: object-scoped
        if let RouteAction::Forward(addr) = self.route_by_object(&req.object_name).await {
            if let Some(ref fl) = self.forwarding_layer {
                // Forward not yet implemented for this RPC; fall through to local
                let _ = (addr, fl);
            }
        }

        match self
            .metadata_store
            .lock()
            .await
            .update_object_checksum(&req.object_name, req.checksum, req.total_size)
        {
            Ok(_) => {
                self.metrics
                    .grpc_requests_total
                    .get_or_create(&metrics::RpcLabels {
                        method: "update_object_checksum".to_string(),
                        status: "ok".to_string(),
                    })
                    .inc();
                Ok(Response::new(storage_metadata::UpdateObjectChecksumResponse {}))
            }
            Err(e) => {
                span.record("error", format!("{}", e));
                let status = match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        Status::new(Code::NotFound, format!("{}", e))
                    }
                    _ => {
                        Status::new(Code::Internal, "failed to update object checksum")
                    }
                };
                Err(status)
            }
        }
    }
}

async fn health_check(storage_metadata: StorageMetadataImpl) {
    loop {
        // Add jitter (0-5 seconds) to the 20-second interval to avoid thundering herd
        let jitter = rand::random::<u64>() % 5000;
        time::sleep(Duration::from_millis(20_000 + jitter)).await;
        tracing::debug!("checking storage node health");

        let storage_nodes = {
            storage_metadata
                .metadata_store
                .lock()
                .await
                .storage_nodes
                .clone()
        };

        storage_metadata.metrics.health_checks_total.inc();

        // Run health checks in parallel to avoid blocking on slow/unresponsive nodes
        let mut health_check_tasks = Vec::new();
        for (idx, node) in storage_nodes {
            let task = tokio::spawn(async move {
                let result = node.check_health().await;
                (idx, result)
            });
            health_check_tasks.push(task);
        }

        // Collect results from all health checks
        let mut delete_ids: Vec<u64> = Vec::new();
        for task in health_check_tasks {
            match task.await {
                Ok((_idx, Ok(_))) => {
                    // Node is healthy
                }
                Ok((idx, Err(e))) => {
                    tracing::warn!("health check failed for node {}: {}", idx, e);
                    storage_metadata.metrics.health_checks_failed_total.inc();
                    delete_ids.push(idx);
                }
                Err(e) => {
                    tracing::error!("health check task panicked: {}", e);
                }
            }
        }

        if !delete_ids.is_empty() {
            tracing::info!("removing {} unhealthy storage node(s)", delete_ids.len());
            let mut metadata_store = storage_metadata.metadata_store.lock().await;
            for idx in &delete_ids {
                metadata_store.storage_nodes.remove(idx);
            }
            storage_metadata
                .metrics
                .storage_nodes_total
                .set(metadata_store.storage_nodes.len() as i64);
        }
    }
}

fn init_tracer() -> SdkTracerProvider {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let otlp_endpoint =
        std::env::var("OTLP_ENDPOINT").unwrap_or_else(|_| "http://localhost:4317".to_string());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&otlp_endpoint)
        .build()
        .expect("failed to create OTLP exporter");

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_attributes([
                    KeyValue::new("service.name", env!("CARGO_PKG_NAME")),
                    KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                ])
                .build(),
        )
        .build();

    global::set_tracer_provider(provider.clone());
    provider
}

fn init_logs() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_target(true)
        .with_current_span(true)
        .with_span_list(false)
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        );
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tracer_provider = init_tracer();
    init_logs();

    // Parse cluster configuration from environment
    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| {
        hostname::get()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string()
    });
    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3001);
    let gossip_port: u16 = std::env::var("GOSSIP_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3002);
    let seed_nodes: Vec<String> = std::env::var("SEED_NODES")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| ".".to_string());
    let metrics_port: u16 = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9091);

    tracing::info!(
        node_id = %node_id,
        grpc_port = grpc_port,
        gossip_port = gossip_port,
        seed_nodes = ?seed_nodes,
        data_dir = %data_dir,
        "Starting storage-metadata service"
    );

    // Initialize metrics
    let mut registry = Registry::default();
    let app_metrics = Arc::new(metrics::Metrics::new(&mut registry));
    let registry = Arc::new(Mutex::new(registry));

    // Start metrics HTTP server
    let metrics_registry = registry.clone();
    let metrics_bind = format!("0.0.0.0:{}", metrics_port);
    tokio::spawn(async move {
        let metrics_router = Router::new().route(
            "/metrics",
            get(move || {
                let reg = metrics_registry.clone();
                async move {
                    let registry = reg.lock().unwrap();
                    let body = metrics::encode_metrics(&registry);
                    (
                        [(
                            header::CONTENT_TYPE,
                            "text/plain; version=0.0.4; charset=utf-8",
                        )],
                        body,
                    )
                }
            }),
        );
        let listener = TcpListener::bind(&metrics_bind).await.unwrap();
        tracing::info!("metrics server listening on {}", metrics_bind);
        axum::serve(listener, metrics_router).await.unwrap();
    });

    // Create metadata store and service implementation
    let mut storage_metadata =
        StorageMetadataImpl::construct(app_metrics.clone(), &data_dir, &node_id)
            .expect("failed to create metadata store");

    // Initialize cluster
    let cluster = ClusterManager::new(node_id.clone(), grpc_port, gossip_port, seed_nodes)
        .await
        .expect("failed to initialize cluster");
    let cluster = Arc::new(cluster);

    // Start membership watcher
    cluster.spawn_membership_watcher();

    // Populate chunk index from existing data
    storage_metadata.populate_chunk_index().await;

    // Create forwarding layer
    let forwarding_layer = Arc::new(ForwardingLayer::new(
        cluster.clone(),
        storage_metadata.chunk_index.clone(),
    ));
    storage_metadata.set_forwarding_layer(forwarding_layer.clone());

    // Spawn migration task to pull data from peers on startup
    let migration_manager = Arc::new(MigrationManager::new(
        cluster.clone(),
        storage_metadata.metadata_store.clone(),
        storage_metadata.chunk_index.clone(),
        forwarding_layer,
    ));
    tokio::spawn({
        let mm = migration_manager.clone();
        async move {
            mm.migrate_on_join().await;
        }
    });

    // Start health check background task
    let background_storage_metadata = storage_metadata.clone();
    tokio::spawn(health_check(background_storage_metadata));

    // Create both gRPC services
    let storage_metadata_svc = StorageMetadataServiceServer::new(storage_metadata.clone());
    let cluster_svc = MetadataClusterServiceServer::new(MetadataClusterServiceImpl {
        metadata_store: storage_metadata.metadata_store.clone(),
        chunk_index: storage_metadata.chunk_index.clone(),
    });

    let addr = format!("0.0.0.0:{}", grpc_port).parse().unwrap();
    tracing::info!(message = format!("gRPC server listening on {}", addr));

    let server_result = Server::builder()
        .accept_http1(true)
        .add_service(tonic_web::enable(storage_metadata_svc))
        .add_service(cluster_svc)
        .serve(addr)
        .await;

    tracer_provider
        .shutdown()
        .expect("shutdown tracer provider failed");

    server_result?;
    Ok(())
}
