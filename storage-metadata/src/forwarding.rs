use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::chunk_index::ChunkIndex;
use crate::cluster::ClusterManager;
use storage_proto_lib::storage_metadata::{
    self, AddChunkReplicaRequest, AddChunkReplicaResponse, AllocateChunkRequest,
    AllocateChunkResponse, ChunkIndexEntry, ChunkIndexUpdate, ConfirmChunkRequest,
    ConfirmChunkResponse, DeleteChunkRequest, DeleteChunkResponse, DeleteObjectRequest,
    DeleteObjectResponse, DeleteStorageNodeRequest, DeleteStorageNodeResponse, GetChunkRequest,
    GetChunkResponse, GetObjectRequest, GetObjectResponse, ListObjectsRequest,
    ListUnderReplicatedChunksRequest, PutChunkRequest,
    PutChunkResponse, PutObjectRequest, PutObjectResponse, PutStorageNodeRequest,
    PutStorageNodeResponse,
};

/// Routing decisions for RPC requests.
pub enum RouteAction {
    /// Handle locally — this node owns the key.
    Local,
    /// Forward to the given peer gRPC address.
    Forward(String),
}

/// The forwarding layer routes requests based on the consistent hash ring.
pub struct ForwardingLayer {
    cluster: Arc<ClusterManager>,
    chunk_index: Arc<ChunkIndex>,
}

impl ForwardingLayer {
    pub fn new(cluster: Arc<ClusterManager>, chunk_index: Arc<ChunkIndex>) -> Self {
        Self {
            cluster,
            chunk_index,
        }
    }

    /// Determine routing for an object-scoped request.
    pub async fn route_by_object(&self, object_name: &str) -> RouteAction {
        match self.cluster.get_owner_addr(object_name).await {
            Some(addr) => RouteAction::Forward(addr),
            None => RouteAction::Local,
        }
    }

    /// Determine routing for a chunk-scoped request by looking up the chunk index.
    pub async fn route_by_chunk(&self, chunk_id: u64) -> Result<RouteAction, Status> {
        let object_name = self
            .chunk_index
            .get(chunk_id)
            .await
            .ok_or_else(|| Status::not_found(format!("chunk {} not found in index", chunk_id)))?;

        Ok(self.route_by_object(&object_name).await)
    }

    // ===== Object-scoped forwarding =====

    pub async fn forward_put_object(
        &self,
        addr: &str,
        request: PutObjectRequest,
    ) -> Result<Response<PutObjectResponse>, Status> {
        let mut client = self
            .cluster
            .get_peer_client(addr)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to peer: {}", e)))?;
        client
            .forward_put_object(Request::new(request))
            .await
    }

    pub async fn forward_get_object(
        &self,
        addr: &str,
        request: GetObjectRequest,
    ) -> Result<Response<GetObjectResponse>, Status> {
        let mut client = self
            .cluster
            .get_peer_client(addr)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to peer: {}", e)))?;
        client
            .forward_get_object(Request::new(request))
            .await
    }

    pub async fn forward_delete_object(
        &self,
        addr: &str,
        request: DeleteObjectRequest,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let mut client = self
            .cluster
            .get_peer_client(addr)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to peer: {}", e)))?;
        client
            .forward_delete_object(Request::new(request))
            .await
    }

    pub async fn forward_put_chunk(
        &self,
        addr: &str,
        request: PutChunkRequest,
    ) -> Result<Response<PutChunkResponse>, Status> {
        let mut client = self
            .cluster
            .get_peer_client(addr)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to peer: {}", e)))?;
        client
            .forward_put_chunk(Request::new(request))
            .await
    }

    pub async fn forward_allocate_chunk(
        &self,
        addr: &str,
        request: AllocateChunkRequest,
    ) -> Result<Response<AllocateChunkResponse>, Status> {
        let mut client = self
            .cluster
            .get_peer_client(addr)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to peer: {}", e)))?;
        client
            .forward_allocate_chunk(Request::new(request))
            .await
    }

    pub async fn forward_confirm_chunk(
        &self,
        addr: &str,
        request: ConfirmChunkRequest,
    ) -> Result<Response<ConfirmChunkResponse>, Status> {
        let mut client = self
            .cluster
            .get_peer_client(addr)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to peer: {}", e)))?;
        client
            .forward_confirm_chunk(Request::new(request))
            .await
    }

    pub async fn forward_get_chunk(
        &self,
        addr: &str,
        request: GetChunkRequest,
    ) -> Result<Response<GetChunkResponse>, Status> {
        let mut client = self
            .cluster
            .get_peer_client(addr)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to peer: {}", e)))?;
        client
            .forward_get_chunk(Request::new(request))
            .await
    }

    pub async fn forward_delete_chunk(
        &self,
        addr: &str,
        request: DeleteChunkRequest,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        let mut client = self
            .cluster
            .get_peer_client(addr)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to peer: {}", e)))?;
        client
            .forward_delete_chunk(Request::new(request))
            .await
    }

    pub async fn forward_add_chunk_replica(
        &self,
        addr: &str,
        request: AddChunkReplicaRequest,
    ) -> Result<Response<AddChunkReplicaResponse>, Status> {
        let mut client = self
            .cluster
            .get_peer_client(addr)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to peer: {}", e)))?;
        client
            .forward_add_chunk_replica(Request::new(request))
            .await
    }

    // ===== Global write broadcast =====

    /// Broadcast PutStorageNode to all peers (and handle locally).
    pub async fn broadcast_put_storage_node(
        &self,
        request: &PutStorageNodeRequest,
    ) -> Vec<Result<Response<PutStorageNodeResponse>, Status>> {
        let addrs = self.cluster.get_all_peer_addrs().await;
        let mut results = Vec::new();

        for addr in addrs {
            let result = async {
                let mut client = self
                    .cluster
                    .get_peer_client(&addr)
                    .await
                    .map_err(|e| {
                        Status::unavailable(format!("failed to connect to peer {}: {}", addr, e))
                    })?;
                client
                    .broadcast_put_storage_node(Request::new(request.clone()))
                    .await
            }
            .await;
            results.push(result);
        }

        results
    }

    /// Broadcast DeleteStorageNode to all peers (and handle locally).
    pub async fn broadcast_delete_storage_node(
        &self,
        request: &DeleteStorageNodeRequest,
    ) -> Vec<Result<Response<DeleteStorageNodeResponse>, Status>> {
        let addrs = self.cluster.get_all_peer_addrs().await;
        let mut results = Vec::new();

        for addr in addrs {
            let result = async {
                let mut client = self
                    .cluster
                    .get_peer_client(&addr)
                    .await
                    .map_err(|e| {
                        Status::unavailable(format!("failed to connect to peer {}: {}", addr, e))
                    })?;
                client
                    .broadcast_delete_storage_node(Request::new(request.clone()))
                    .await
            }
            .await;
            results.push(result);
        }

        results
    }

    // ===== Scatter-gather for aggregate queries =====

    /// Scatter ListObjects to all peers and gather results.
    pub async fn scatter_gather_list_objects(
        &self,
    ) -> Result<Vec<storage_metadata::MetaObject>, Status> {
        let addrs = self.cluster.get_all_peer_addrs().await;
        let mut all_objects = Vec::new();

        for addr in addrs {
            let mut client = self
                .cluster
                .get_peer_client(&addr)
                .await
                .map_err(|e| {
                    Status::unavailable(format!("failed to connect to peer {}: {}", addr, e))
                })?;
            match client
                .list_local_objects(Request::new(ListObjectsRequest {}))
                .await
            {
                Ok(resp) => {
                    all_objects.extend(resp.into_inner().objects);
                }
                Err(e) => {
                    tracing::warn!("Failed to list objects from peer {}: {}", addr, e);
                }
            }
        }

        Ok(all_objects)
    }

    /// Scatter ListUnderReplicatedChunks to all peers and gather results.
    pub async fn scatter_gather_under_replicated_chunks(
        &self,
        max_replicas: u32,
    ) -> Result<Vec<storage_metadata::UnderReplicatedChunk>, Status> {
        let addrs = self.cluster.get_all_peer_addrs().await;
        let mut all_chunks = Vec::new();

        for addr in addrs {
            let mut client = self
                .cluster
                .get_peer_client(&addr)
                .await
                .map_err(|e| {
                    Status::unavailable(format!("failed to connect to peer {}: {}", addr, e))
                })?;
            match client
                .list_local_under_replicated_chunks(Request::new(
                    ListUnderReplicatedChunksRequest { max_replicas },
                ))
                .await
            {
                Ok(resp) => {
                    all_chunks.extend(resp.into_inner().chunks);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to list under-replicated chunks from peer {}: {}",
                        addr,
                        e
                    );
                }
            }
        }

        Ok(all_chunks)
    }

    /// Broadcast chunk index entries to all peers.
    pub async fn broadcast_chunk_index_update(&self, entries: Vec<(u64, String)>) {
        let addrs = self.cluster.get_all_peer_addrs().await;
        let update = ChunkIndexUpdate {
            entries: entries
                .iter()
                .map(|(id, name)| ChunkIndexEntry {
                    chunk_id: *id,
                    object_name: name.clone(),
                })
                .collect(),
        };

        for addr in addrs {
            match self.cluster.get_peer_client(&addr).await {
                Ok(mut client) => {
                    if let Err(e) = client
                        .broadcast_chunk_index(Request::new(update.clone()))
                        .await
                    {
                        tracing::warn!("Failed to broadcast chunk index to {}: {}", addr, e);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to connect to peer {} for chunk index broadcast: {}",
                        addr,
                        e
                    );
                }
            }
        }
    }
}
