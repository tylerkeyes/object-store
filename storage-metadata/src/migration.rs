use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex as TokioMutex;
use tonic::Request;

use crate::chunk_index::ChunkIndex;
use crate::cluster::ClusterManager;
use crate::forwarding::ForwardingLayer;
use crate::metadata_store::{MetaChunk, MetaObject, MetadataStore};
use storage_proto_lib::storage_metadata::{
    self, HashRange, TransferPartitionRequest,
};

/// Handles data migration when nodes join or leave the cluster.
pub struct MigrationManager {
    cluster: Arc<ClusterManager>,
    metadata_store: Arc<TokioMutex<MetadataStore>>,
    chunk_index: Arc<ChunkIndex>,
    forwarding: Arc<ForwardingLayer>,
}

impl MigrationManager {
    pub fn new(
        cluster: Arc<ClusterManager>,
        metadata_store: Arc<TokioMutex<MetadataStore>>,
        chunk_index: Arc<ChunkIndex>,
        forwarding: Arc<ForwardingLayer>,
    ) -> Self {
        Self {
            cluster,
            metadata_store,
            chunk_index,
            forwarding,
        }
    }

    /// Run migration on startup: wait for peers to appear, then pull our
    /// owned hash ranges from all existing peers.
    ///
    /// Each peer's TransferPartition handler only returns objects that fall
    /// within the requested ranges, so it's safe to broadcast our ranges to
    /// everyone — each peer returns only what it has.
    pub async fn migrate_on_join(&self) {
        // Wait for cluster to discover at least one peer
        let mut attempts = 0;
        loop {
            if self.cluster.is_clustered().await {
                break;
            }
            attempts += 1;
            if attempts > 30 {
                tracing::info!("No peers discovered after 30s, skipping migration (single-node mode)");
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Give a brief moment for the ring to stabilize
        tokio::time::sleep(Duration::from_secs(2)).await;

        let owned_ranges = {
            let ring = self.cluster.ring().read().await;
            ring.get_owned_ranges(self.cluster.node_id())
        };

        if owned_ranges.is_empty() {
            tracing::info!("No owned ranges to migrate");
            return;
        }

        tracing::info!(
            node_id = %self.cluster.node_id(),
            range_count = owned_ranges.len(),
            "Starting migration: pulling owned ranges from peers"
        );

        let peer_addrs = self.cluster.get_all_peer_addrs().await;

        for addr in &peer_addrs {
            match self.pull_partition(addr, owned_ranges.clone()).await {
                Ok(()) => {
                    tracing::info!("Migration from {} completed successfully", addr);
                }
                Err(e) => {
                    tracing::warn!("Migration from {} failed: {}", addr, e);
                }
            }
        }

        // Broadcast our full chunk index to all peers so they know about our data
        let entries = self.chunk_index.entries().await;
        if !entries.is_empty() {
            self.forwarding
                .broadcast_chunk_index_update(entries)
                .await;
        }

        tracing::info!("Migration on join complete");
    }

    /// Request data transfer from a peer for the given hash ranges.
    /// Called when this node has joined the cluster and needs to take ownership
    /// of partitions from existing nodes.
    pub async fn pull_partition(
        &self,
        source_addr: &str,
        ranges: Vec<(u64, u64)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut client = self
            .cluster
            .get_peer_client(source_addr)
            .await
            .map_err(|e| format!("failed to connect to source {}: {}", source_addr, e))?;

        let request = TransferPartitionRequest {
            ranges: ranges
                .iter()
                .map(|(start, end)| HashRange {
                    start: *start,
                    end: *end,
                })
                .collect(),
        };

        let response = client.transfer_partition(Request::new(request)).await?;
        let mut stream = response.into_inner();

        let mut objects = Vec::new();
        let mut chunks = Vec::new();
        let mut chunk_index_entries = Vec::new();

        while let Some(entry) = stream.message().await? {
            match entry.entry {
                Some(storage_metadata::transfer_partition_entry::Entry::Object(obj)) => {
                    objects.push(MetaObject {
                        name: obj.name,
                        checksum: obj.checksum,
                        chunks: obj.chunk_ids,
                        total_size: 0,
                    });
                }
                Some(storage_metadata::transfer_partition_entry::Entry::Chunk(chunk)) => {
                    let meta_chunk = MetaChunk {
                        id: chunk.id,
                        object_name: chunk.object_name.clone(),
                        checksum: chunk.checksum,
                        storage_nodes: chunk.storage_node_ids,
                        status: crate::metadata_store::ChunkStatus::Confirmed,
                    };
                    chunk_index_entries.push((chunk.id, chunk.object_name));
                    chunks.push(meta_chunk);
                }
                None => {}
            }
        }

        if objects.is_empty() && chunks.is_empty() {
            tracing::info!("No data to migrate from {}", source_addr);
            return Ok(());
        }

        tracing::info!(
            "Received {} objects and {} chunks from {}",
            objects.len(),
            chunks.len(),
            source_addr
        );

        // Ingest into local store
        self.metadata_store
            .lock()
            .await
            .ingest_partition(objects, chunks)?;

        // Update chunk index locally
        self.chunk_index.bulk_insert(chunk_index_entries.clone()).await;

        // Broadcast chunk index updates to all peers
        self.forwarding
            .broadcast_chunk_index_update(chunk_index_entries)
            .await;

        Ok(())
    }
}
