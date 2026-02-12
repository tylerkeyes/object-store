use std::collections::HashMap;
use tokio::sync::RwLock;

/// Global chunk_id → object_name index.
///
/// Every node in the cluster maintains this index so that chunk-scoped RPCs
/// (GetChunk, DeleteChunk, AddChunkReplica) can be routed to the correct
/// object owner without a network hop to discover the mapping.
pub struct ChunkIndex {
    index: RwLock<HashMap<u64, String>>,
}

impl ChunkIndex {
    pub fn new() -> Self {
        Self {
            index: RwLock::new(HashMap::new()),
        }
    }

    /// Insert a chunk_id → object_name mapping.
    pub async fn insert(&self, chunk_id: u64, object_name: String) {
        self.index.write().await.insert(chunk_id, object_name);
    }

    /// Look up the object_name for a chunk_id.
    pub async fn get(&self, chunk_id: u64) -> Option<String> {
        self.index.read().await.get(&chunk_id).cloned()
    }

    /// Remove a chunk_id from the index.
    pub async fn remove(&self, chunk_id: u64) -> Option<String> {
        self.index.write().await.remove(&chunk_id)
    }

    /// Insert multiple mappings at once.
    pub async fn bulk_insert(&self, entries: Vec<(u64, String)>) {
        let mut idx = self.index.write().await;
        for (chunk_id, object_name) in entries {
            idx.insert(chunk_id, object_name);
        }
    }

    /// Remove all entries whose object_name is in the given set.
    pub async fn remove_by_objects(&self, object_names: &[String]) {
        let mut idx = self.index.write().await;
        idx.retain(|_, obj_name| !object_names.contains(obj_name));
    }

    /// Populate from a MetadataStore's chunks map.
    pub async fn populate_from_chunks(&self, chunks: &HashMap<u64, crate::metadata_store::MetaChunk>) {
        let mut idx = self.index.write().await;
        for (chunk_id, chunk) in chunks {
            idx.insert(*chunk_id, chunk.object_name.clone());
        }
    }

    /// Get all entries as a vec (for broadcasting to peers).
    pub async fn entries(&self) -> Vec<(u64, String)> {
        self.index
            .read()
            .await
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Number of entries in the index.
    pub async fn len(&self) -> usize {
        self.index.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_insert_and_get() {
        let idx = ChunkIndex::new();
        idx.insert(1, "obj-a".to_string()).await;
        idx.insert(2, "obj-b".to_string()).await;

        assert_eq!(idx.get(1).await, Some("obj-a".to_string()));
        assert_eq!(idx.get(2).await, Some("obj-b".to_string()));
        assert_eq!(idx.get(3).await, None);
    }

    #[tokio::test]
    async fn test_remove() {
        let idx = ChunkIndex::new();
        idx.insert(1, "obj-a".to_string()).await;

        assert_eq!(idx.remove(1).await, Some("obj-a".to_string()));
        assert_eq!(idx.get(1).await, None);
        assert_eq!(idx.remove(1).await, None);
    }

    #[tokio::test]
    async fn test_bulk_insert() {
        let idx = ChunkIndex::new();
        idx.bulk_insert(vec![
            (1, "obj-a".to_string()),
            (2, "obj-b".to_string()),
            (3, "obj-a".to_string()),
        ])
        .await;

        assert_eq!(idx.len().await, 3);
        assert_eq!(idx.get(1).await, Some("obj-a".to_string()));
        assert_eq!(idx.get(2).await, Some("obj-b".to_string()));
    }

    #[tokio::test]
    async fn test_remove_by_objects() {
        let idx = ChunkIndex::new();
        idx.bulk_insert(vec![
            (1, "obj-a".to_string()),
            (2, "obj-b".to_string()),
            (3, "obj-a".to_string()),
            (4, "obj-c".to_string()),
        ])
        .await;

        idx.remove_by_objects(&["obj-a".to_string()]).await;

        assert_eq!(idx.len().await, 2);
        assert_eq!(idx.get(1).await, None);
        assert_eq!(idx.get(2).await, Some("obj-b".to_string()));
        assert_eq!(idx.get(3).await, None);
        assert_eq!(idx.get(4).await, Some("obj-c".to_string()));
    }

    #[tokio::test]
    async fn test_entries() {
        let idx = ChunkIndex::new();
        idx.insert(1, "obj-a".to_string()).await;
        idx.insert(2, "obj-b".to_string()).await;

        let mut entries = idx.entries().await;
        entries.sort_by_key(|(k, _)| *k);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (1, "obj-a".to_string()));
        assert_eq!(entries[1], (2, "obj-b".to_string()));
    }

    /// Simulate chunk index synchronization across 3 nodes.
    /// Each node has its own ChunkIndex; when one node adds entries,
    /// it "broadcasts" them to the others via bulk_insert.
    #[tokio::test]
    async fn test_multi_node_chunk_index_sync() {
        let node_1 = ChunkIndex::new();
        let node_2 = ChunkIndex::new();
        let node_3 = ChunkIndex::new();

        // Node 1 owns some chunks
        node_1.insert(100, "obj-a".to_string()).await;
        node_1.insert(101, "obj-a".to_string()).await;

        // Node 2 owns different chunks
        node_2.insert(200, "obj-b".to_string()).await;

        // Simulate broadcast: node 1 sends its entries to nodes 2 and 3
        let entries_1 = node_1.entries().await;
        node_2.bulk_insert(entries_1.clone()).await;
        node_3.bulk_insert(entries_1).await;

        // Simulate broadcast: node 2 sends its entries to nodes 1 and 3
        let entries_2 = node_2.entries().await;
        node_1.bulk_insert(entries_2.clone()).await;
        node_3.bulk_insert(entries_2).await;

        // All nodes should now have the same global view
        assert_eq!(node_1.len().await, 3);
        assert_eq!(node_2.len().await, 3);
        assert_eq!(node_3.len().await, 3);

        // Verify all can look up any chunk
        for node in [&node_1, &node_2, &node_3] {
            assert_eq!(node.get(100).await, Some("obj-a".to_string()));
            assert_eq!(node.get(101).await, Some("obj-a".to_string()));
            assert_eq!(node.get(200).await, Some("obj-b".to_string()));
        }
    }

    /// Simulate the full migration flow: Node 1 has data, Node 2 joins and
    /// gets chunk index entries synced.
    #[tokio::test]
    async fn test_migration_chunk_index_update() {
        let existing_node = ChunkIndex::new();
        let new_node = ChunkIndex::new();

        // Existing node has 5 chunks across 2 objects
        existing_node.bulk_insert(vec![
            (1, "obj-x".to_string()),
            (2, "obj-x".to_string()),
            (3, "obj-x".to_string()),
            (4, "obj-y".to_string()),
            (5, "obj-y".to_string()),
        ]).await;

        // New node joins and pulls chunks 4,5 (obj-y migrated to it)
        // After migration, new node broadcasts its chunk index
        new_node.insert(4, "obj-y".to_string()).await;
        new_node.insert(5, "obj-y".to_string()).await;

        // Broadcast new node's entries to existing node
        let new_entries = new_node.entries().await;
        existing_node.bulk_insert(new_entries).await;

        // Both nodes should be able to resolve all chunks
        assert_eq!(existing_node.len().await, 5);
        assert_eq!(new_node.len().await, 2); // new node only has its own data + broadcast

        // Broadcast existing node's entries to new node for full global view
        let all_entries = existing_node.entries().await;
        new_node.bulk_insert(all_entries).await;
        assert_eq!(new_node.len().await, 5);
    }
}
